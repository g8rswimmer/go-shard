package exec

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/router"
)

// Tx is a transaction on one shard. Every statement runs on the same
// connection until Commit or Rollback.
//
// The context given to Begin controls the transaction's life: cancelling it
// rolls the transaction back. The per-shard timeout applies to each statement,
// not to the transaction as a whole.
type Tx struct {
	shard   router.ShardID
	tx      *sql.Tx
	timeout time.Duration
	hooks   shardHooks
}

// Begin starts a transaction on one shard.
func (e *Executor) Begin(ctx context.Context, id router.ShardID, opts *sql.TxOptions) (*Tx, error) {
	conn, ok := e.pool.Conn(id)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownShard, id)
	}
	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, &ShardError{Shard: id, Err: err}
	}
	return &Tx{shard: id, tx: tx, timeout: e.timeout, hooks: e.hooks}, nil
}

// Shard returns the shard the transaction runs on.
func (t *Tx) Shard() router.ShardID { return t.shard }

func (t *Tx) statementContext(parent context.Context) (context.Context, context.CancelFunc) {
	if t.timeout > 0 {
		return context.WithTimeout(parent, t.timeout)
	}
	return context.WithCancel(parent)
}

// Query runs a statement that returns rows. Close the rows before running the
// next statement: a transaction has one connection, which is busy until then.
func (t *Tx) Query(ctx context.Context, query string, args []any) (*Rows, error) {
	sctx, cancel := t.statementContext(ctx)
	hctx, began := t.hooks.start(sctx, observe.Query, t.shard)
	rows, err := t.tx.QueryContext(hctx, query, args...)
	t.hooks.done(hctx, observe.Query, t.shard, began, 0, false, err)
	if err != nil {
		cancel()
		return nil, &ShardError{Shard: t.shard, Err: err}
	}
	return &Rows{Rows: rows, cancel: cancel, pause: func() {}, restart: func() {}}, nil
}

// Exec runs a statement and returns the rows it affected.
func (t *Tx) Exec(ctx context.Context, query string, args []any) (int64, error) {
	sctx, cancel := t.statementContext(ctx)
	defer cancel()
	hctx, began := t.hooks.start(sctx, observe.Exec, t.shard)
	res, err := t.tx.ExecContext(hctx, query, args...)
	var n int64
	if err == nil {
		n, _ = res.RowsAffected()
	}
	t.hooks.done(hctx, observe.Exec, t.shard, began, n, false, err)
	if err != nil {
		return 0, &ShardError{Shard: t.shard, Err: err}
	}
	return n, nil
}

// Commit commits the transaction. After it, further statements fail with
// sql.ErrTxDone.
func (t *Tx) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return &ShardError{Shard: t.shard, Err: err}
	}
	return nil
}

// Rollback abandons the transaction. It returns sql.ErrTxDone, wrapped, if the
// transaction has already ended, so a deferred Rollback after Commit is harmless.
func (t *Tx) Rollback() error {
	if err := t.tx.Rollback(); err != nil {
		return &ShardError{Shard: t.shard, Err: err}
	}
	return nil
}
