package shard

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/registry"
)

// ErrTxDone is returned by a statement, Commit or Rollback on a transaction that
// has already ended. It is database/sql's ErrTxDone, so a deferred Rollback
// after Commit can be ignored as usual.
var ErrTxDone = sql.ErrTxDone

// TxTarget says which shard a transaction runs on. Build one with ForKey,
// ForTable or ForShard.
type TxTarget struct {
	kind  txTargetKind
	key   any
	table string
	shard ShardID
}

type txTargetKind int

const (
	txNone txTargetKind = iota
	txByKey
	txByTable
	txByShard
)

// ForKey runs the transaction on the shard that owns key. The key is hashed as
// given, so pass it in the same type the table's key column has: the number 42,
// not the text "42". Use ForTable to have the library convert it.
func ForKey(key any) TxTarget { return TxTarget{kind: txByKey, key: key} }

// ForTable runs the transaction on the shard that owns key in a table. The key
// is converted to the type declared for that table's shard key, so the number
// 42 and the text "42" pick the same shard. For a colocated table the key is
// its parent's.
func ForTable(table string, key any) TxTarget {
	return TxTarget{kind: txByTable, table: table, key: key}
}

// ForShard runs the transaction on a named shard.
func ForShard(id ShardID) TxTarget { return TxTarget{kind: txByShard, shard: id} }

// TxOption adjusts a transaction.
type TxOption func(*sql.TxOptions)

// ReadOnly makes the transaction read-only: PostgreSQL refuses writes in it.
func ReadOnly() TxOption { return func(o *sql.TxOptions) { o.ReadOnly = true } }

// Isolation sets the transaction's isolation level, for example
// sql.LevelSerializable. The default is PostgreSQL's (read committed).
func Isolation(level sql.IsolationLevel) TxOption {
	return func(o *sql.TxOptions) { o.Isolation = level }
}

// Tx is a transaction on one shard. It is a Querier, so code written against
// Querier runs unchanged inside a transaction.
//
// Every statement is routed as usual and then checked: it must belong to the
// transaction's shard, otherwise it fails with ErrCrossShardTx before anything
// is sent. A statement that only reads global tables runs on the transaction's
// shard, since every shard holds a copy. A write to a global table would go to
// every shard and is refused.
//
// A Tx holds one database connection, so close the Rows of one query before
// running the next statement. Cancelling the context given to Begin rolls the
// transaction back. Idempotency keys are not used inside a transaction (a
// statement with one is refused): the transaction is already atomic on its shard.
type Tx interface {
	Querier
	// Commit commits the transaction.
	Commit() error
	// Rollback abandons it. After Commit it returns ErrTxDone, which a deferred
	// Rollback can ignore.
	Rollback() error
	// Shard is the shard the transaction runs on.
	Shard() ShardID
	// Unchecked returns a Querier that runs statements on this transaction's
	// shard without routing them or checking where they belong. Use it for SQL
	// the router cannot place (WITH, a scan by a non-key column) when you know
	// the statement belongs on this shard. Nothing stops it from reading or
	// changing rows that live elsewhere.
	Unchecked() Querier
}

// TxBeginner starts transactions. *DB implements it. It is separate from
// Querier so that a Tx, which is a Querier, cannot start another.
type TxBeginner interface {
	Begin(ctx context.Context, target TxTarget, opts ...TxOption) (Tx, error)
}

var _ TxBeginner = (*DB)(nil)

// Begin starts a transaction on one shard.
//
//	tx, err := db.Begin(ctx, shard.ForTable("profiles", id))
//	if err != nil { ... }
//	defer tx.Rollback() // harmless after Commit
//	_, err = tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, $2)", id, name)
//	_, err = tx.Exec(ctx, "INSERT INTO addresses (id, profile_id, city) VALUES ($1, $2, $3)", aid, id, city)
//	err = tx.Commit()
//
// Tables colocated with each other share a shard, so a profile and its
// addresses can be written in one transaction.
func (db *DB) Begin(ctx context.Context, target TxTarget, opts ...TxOption) (Tx, error) {
	id, err := db.resolveTarget(target)
	if err != nil {
		return nil, err
	}
	var o sql.TxOptions
	for _, opt := range opts {
		opt(&o)
	}
	t, err := db.exec.Begin(ctx, id, &o)
	if err != nil {
		return nil, err
	}
	return &tx{db: db, id: id, t: t}, nil
}

// InTx runs fn in a transaction. It commits if fn returns nil and rolls back if
// fn returns an error or panics (the panic continues), so a transaction cannot
// be left open.
func (db *DB) InTx(ctx context.Context, target TxTarget, fn func(Tx) error, opts ...TxOption) error {
	t, err := db.Begin(ctx, target, opts...)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = t.Rollback()
			panic(p)
		}
	}()

	if err := fn(t); err != nil {
		if rbErr := t.Rollback(); rbErr != nil && !errors.Is(rbErr, ErrTxDone) {
			return errors.Join(err, rbErr)
		}
		return err
	}
	return t.Commit()
}

// resolveTarget finds the shard a transaction will run on.
func (db *DB) resolveTarget(t TxTarget) (ShardID, error) {
	switch t.kind {
	case txByKey:
		id, err := db.router.ShardFor(t.key)
		if err != nil {
			return "", fmt.Errorf("transaction key %v: %w", t.key, err)
		}
		return id, nil

	case txByTable:
		tbl, ok := db.registry.Table(t.table)
		switch {
		case !ok:
			return "", fmt.Errorf("%w: %q", ErrUnknownTable, t.table)
		case tbl.Kind == registry.KindGlobal:
			return "", fmt.Errorf("%w: %q is a global table, which has no shard key; use shard.ForShard", ErrUnsupportedQuery, t.table)
		default:
			// a sharded or colocated table
		}
		key, err := plan.CoerceKey(t.key, tbl.KeyType)
		if err != nil {
			return "", fmt.Errorf("transaction key for %s.%s: %w", tbl.Name, tbl.KeyCol, err)
		}
		id, err := db.router.ShardFor(key)
		if err != nil {
			return "", fmt.Errorf("transaction key for %s.%s: %w", tbl.Name, tbl.KeyCol, err)
		}
		return id, nil

	case txByShard:
		if _, ok := db.pool.Conn(t.shard); !ok {
			return "", fmt.Errorf("%w: %q (configured shards: %v)", ErrUnknownShard, t.shard, db.pool.IDs())
		}
		return t.shard, nil

	default:
		return "", errors.New("shard: Begin needs a target: use shard.ForKey, shard.ForTable or shard.ForShard")
	}
}

// tx is the Tx implementation.
type tx struct {
	db *DB
	id ShardID
	t  *exec.Tx
}

var _ Tx = (*tx)(nil)

func (t *tx) Shard() ShardID     { return t.id }
func (t *tx) Commit() error      { return t.t.Commit() }
func (t *tx) Rollback() error    { return t.t.Rollback() }
func (t *tx) Unchecked() Querier { return uncheckedTx{t} }

func (t *tx) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return t.query(ctx, (&scoped{db: t.db}).sqlRequest(sql, args))
}

func (t *tx) QueryStatement(ctx context.Context, st Statement) (Rows, error) {
	return t.query(ctx, statementRequest(st))
}

func (t *tx) Exec(ctx context.Context, sql string, args ...any) (WriteResult, error) {
	return t.exec(ctx, (&scoped{db: t.db}).sqlRequest(sql, args))
}

func (t *tx) ExecStatement(ctx context.Context, st Statement) (WriteResult, error) {
	return t.exec(ctx, statementRequest(st))
}

func (t *tx) Explain(ctx context.Context, sql string, args ...any) (Explain, error) {
	return t.explain(ctx, explainConfig{}, (&scoped{db: t.db}).sqlRequest(sql, args))
}

func (t *tx) ExplainStatement(ctx context.Context, st Statement) (Explain, error) {
	return t.explain(ctx, explainConfig{}, statementRequest(st))
}

func (t *tx) Explainer(opts ...ExplainOption) Explainer {
	return configured{
		cfg: newExplainConfig(opts),
		sql: func(ctx context.Context, cfg explainConfig, sql string, args []any) (Explain, error) {
			return t.explain(ctx, cfg, (&scoped{db: t.db}).sqlRequest(sql, args))
		},
		stmt: func(ctx context.Context, cfg explainConfig, st Statement) (Explain, error) {
			return t.explain(ctx, cfg, statementRequest(st))
		},
	}
}

// explain checks the statement against the transaction like running it would.
// Shard plans are read inside the transaction; the Analyze option is refused, since
// it would run the statement there and could not be undone separately.
func (t *tx) explain(ctx context.Context, cfg explainConfig, r request) (Explain, error) {
	p, err := t.plan(r)
	if err != nil {
		return Explain{}, err
	}
	e, final, err := describePlan(r, p)
	if err != nil {
		return Explain{}, err
	}
	return t.withShardPlans(ctx, cfg, e, final)
}

func (t *tx) withShardPlans(ctx context.Context, cfg explainConfig, e Explain, p plan.Plan) (Explain, error) {
	switch {
	case cfg.analyze:
		return Explain{}, fmt.Errorf("%w: the Analyze option runs the statement, and inside a transaction that cannot be undone separately; "+
			"use the ShardPlans option, or explain outside the transaction", ErrUnsupportedQuery)
	case !cfg.shardPlans:
		return e, nil
	default:
		text, err := t.t.Explain(ctx, p.SQL, p.Args)
		if err != nil {
			return Explain{}, err
		}
		e.ShardPlans = map[ShardID]string{t.id: text}
		return e, nil
	}
}

func (t *tx) query(ctx context.Context, r request) (Rows, error) {
	p, err := t.plan(r)
	if err != nil {
		return nil, err
	}
	return t.t.Query(ctx, p.SQL, p.Args)
}

func (t *tx) exec(ctx context.Context, r request) (WriteResult, error) {
	if err := rejectKey(ctx); err != nil {
		return WriteResult{}, err
	}
	p, err := t.plan(r)
	if err != nil {
		return WriteResult{}, err
	}
	return t.run(ctx, p)
}

func (t *tx) run(ctx context.Context, p plan.Plan) (WriteResult, error) {
	n, err := t.t.Exec(ctx, p.SQL, p.Args)
	out := ShardOutcome{RowsAffected: n, Rows: p.Rows[t.id], Err: err}
	return WriteResult{PerShard: map[ShardID]ShardOutcome{t.id: out}}, err
}

// plan routes a statement and requires it to belong to the transaction's shard.
func (t *tx) plan(r request) (plan.Plan, error) {
	an, err := r.analysis()
	if err != nil {
		return plan.Plan{}, t.hint(err)
	}
	// A statement every shard can answer runs on this one.
	p, err := plan.Route(an, t.db.registry, t.db.router, plan.Options{AnyShard: func() ShardID { return t.id }})
	if err != nil {
		return plan.Plan{}, t.hint(err)
	}
	if len(p.Targets) != 1 || p.Targets[0] != t.id {
		return plan.Plan{}, t.crossShard(p)
	}
	p.SQL, p.Args = r.sql, r.args
	return p, nil
}

// hint adds how to proceed to a routing refusal.
func (t *tx) hint(err error) error {
	if errors.Is(err, ErrShardKeyRequired) || errors.Is(err, ErrUnsupportedQuery) {
		return fmt.Errorf("%w (inside this transaction on %s, tx.Unchecked() runs a statement on the transaction's shard without routing it)", err, t.id)
	}
	return err
}

func (t *tx) crossShard(p plan.Plan) error {
	extra := ""
	if p.GlobalWrite {
		extra = "; a write to a global table goes to every shard, so do it outside the transaction"
	}
	return fmt.Errorf("%w: the transaction is on %s but this statement belongs to %v (%s)%s; "+
		"a transaction covers one shard, so keep rows that change together on one shard with colocation, or run this statement outside the transaction",
		ErrCrossShardTx, t.id, p.Targets, p.Reason, extra)
}

// rejectKey refuses a statement carrying an idempotency key.
func rejectKey(ctx context.Context) error {
	if _, ok := IdempotencyKey(ctx); ok {
		return fmt.Errorf("%w: idempotency keys are not used inside a transaction, which is already atomic on its shard; "+
			"run the statement without a key, or put the key on a statement outside the transaction", ErrUnsupportedQuery)
	}
	return nil
}

// uncheckedTx runs statements on the transaction's shard without routing them.
type uncheckedTx struct{ t *tx }

var _ Querier = uncheckedTx{}

func (u uncheckedTx) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return u.t.t.Query(ctx, sql, args)
}

func (u uncheckedTx) QueryStatement(ctx context.Context, st Statement) (Rows, error) {
	return u.t.t.Query(ctx, st.SQL(), st.Args())
}

func (u uncheckedTx) Exec(ctx context.Context, sql string, args ...any) (WriteResult, error) {
	if err := rejectKey(ctx); err != nil {
		return WriteResult{}, err
	}
	return u.t.run(ctx, plan.Plan{SQL: sql, Args: args})
}

func (u uncheckedTx) ExecStatement(ctx context.Context, st Statement) (WriteResult, error) {
	return u.Exec(ctx, st.SQL(), st.Args()...)
}

func (u uncheckedTx) Explain(ctx context.Context, sql string, args ...any) (Explain, error) {
	return u.explain(ctx, explainConfig{}, plan.Plan{SQL: sql, Args: args})
}

func (u uncheckedTx) ExplainStatement(ctx context.Context, st Statement) (Explain, error) {
	return u.explain(ctx, explainConfig{}, plan.Plan{SQL: st.SQL(), Args: st.Args()})
}

func (u uncheckedTx) Explainer(opts ...ExplainOption) Explainer {
	return configured{
		cfg: newExplainConfig(opts),
		sql: func(ctx context.Context, cfg explainConfig, sql string, args []any) (Explain, error) {
			return u.explain(ctx, cfg, plan.Plan{SQL: sql, Args: args})
		},
		stmt: func(ctx context.Context, cfg explainConfig, st Statement) (Explain, error) {
			return u.explain(ctx, cfg, plan.Plan{SQL: st.SQL(), Args: st.Args()})
		},
	}
}

func (u uncheckedTx) explain(ctx context.Context, cfg explainConfig, p plan.Plan) (Explain, error) {
	p.Targets, p.Strategy = []ShardID{u.t.id}, plan.Single
	p.Reason = fmt.Sprintf("tx.Unchecked(): the transaction's shard %s, not routed", u.t.id)
	e, final, err := describePlan(request{}, p)
	if err != nil {
		return Explain{}, err
	}
	return u.t.withShardPlans(ctx, cfg, e, final)
}
