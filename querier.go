package shard

import (
	"context"
	"errors"
	"fmt"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/plan"
)

// Rows is a result set. *sql.Rows satisfies it, and so will merged results from
// several shards. Always Close it.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	Err() error
	Close() error
}

// ShardOutcome is what a write did on one shard.
type ShardOutcome struct {
	RowsAffected int64
	// Err is nil when the write succeeded on this shard.
	Err error
}

// WriteResult reports a write shard by shard. A write that reached several
// shards is not atomic: some can succeed while others fail.
type WriteResult struct {
	PerShard map[ShardID]ShardOutcome
}

// RowsAffected totals the rows affected on the shards where the write
// succeeded.
func (r WriteResult) RowsAffected() int64 {
	var n int64
	for _, o := range r.PerShard {
		if o.Err == nil {
			n += o.RowsAffected
		}
	}
	return n
}

// Statement is a statement that already knows how it routes. Build one with
// package query.
type Statement interface {
	SQL() string
	Args() []any
	Analysis() analyze.Analysis
}

// Querier runs statements. *DB and the values returned by WithShardKey,
// WithShard and WithAllShards implement it. Application code should depend on
// this interface so tests can substitute a fake.
//
// Without WithShardKey, WithShard or WithAllShards, the library finds the
// shard from the statement itself (see the package documentation).
type Querier interface {
	// Query runs a read on the target shard and returns its rows. The caller
	// must close them.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	// Exec runs a write on the target shard(s). It returns an error if the
	// write failed on any shard; the WriteResult still reports every shard.
	Exec(ctx context.Context, sql string, args ...any) (WriteResult, error)
	// QueryStatement is Query for a built Statement; it needs no SQL parsing.
	QueryStatement(ctx context.Context, st Statement) (Rows, error)
	// ExecStatement is Exec for a built Statement; it needs no SQL parsing.
	ExecStatement(ctx context.Context, st Statement) (WriteResult, error)
}

type routeKind int

const (
	routeNone routeKind = iota
	routeByKey
	routeByShard
	routeAll
)

// route is a caller-supplied routing decision.
type route struct {
	kind  routeKind
	key   any
	shard ShardID
}

// scoped is a Querier with a fixed routing decision.
type scoped struct {
	db    *DB
	route route
}

// WithShardKey routes statements to the shard that owns key.
func (db *DB) WithShardKey(key any) Querier { return &scoped{db, route{kind: routeByKey, key: key}} }

// WithShard routes statements to one named shard.
func (db *DB) WithShard(id ShardID) Querier { return &scoped{db, route{kind: routeByShard, shard: id}} }

// WithAllShards runs statements on every shard. Exec works today; Query needs
// result merging and returns ErrUnsupportedQuery until that is added.
func (db *DB) WithAllShards() Querier { return &scoped{db, route{kind: routeAll}} }

// Query implements Querier.
func (db *DB) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return (&scoped{db: db}).Query(ctx, sql, args...)
}

// Exec implements Querier.
func (db *DB) Exec(ctx context.Context, sql string, args ...any) (WriteResult, error) {
	return (&scoped{db: db}).Exec(ctx, sql, args...)
}

// QueryStatement implements Querier.
func (db *DB) QueryStatement(ctx context.Context, st Statement) (Rows, error) {
	return (&scoped{db: db}).QueryStatement(ctx, st)
}

// ExecStatement implements Querier.
func (db *DB) ExecStatement(ctx context.Context, st Statement) (WriteResult, error) {
	return (&scoped{db: db}).ExecStatement(ctx, st)
}

func (s *scoped) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return s.query(ctx, sql, args, s.analyzeSQL(sql, args))
}

func (s *scoped) QueryStatement(ctx context.Context, st Statement) (Rows, error) {
	return s.query(ctx, st.SQL(), st.Args(), func() (analyze.Analysis, error) { return st.Analysis(), nil })
}

func (s *scoped) query(ctx context.Context, sql string, args []any, a analysis) (Rows, error) {
	p, err := s.plan(sql, args, a)
	if err != nil {
		return nil, err
	}
	if len(p.Targets) > 1 {
		return nil, fmt.Errorf("%w: reading from %d shards needs result merging, which is not available yet; "+
			"narrow the query to one shard key, or use WithShardKey / WithShard", ErrUnsupportedQuery, len(p.Targets))
	}
	res, err := s.db.exec.Query(ctx, p)
	if err != nil {
		return nil, err
	}
	return res[0].Rows, nil
}

func (s *scoped) Exec(ctx context.Context, sql string, args ...any) (WriteResult, error) {
	return s.exec(ctx, sql, args, s.analyzeSQL(sql, args))
}

func (s *scoped) ExecStatement(ctx context.Context, st Statement) (WriteResult, error) {
	return s.exec(ctx, st.SQL(), st.Args(), func() (analyze.Analysis, error) { return st.Analysis(), nil })
}

func (s *scoped) exec(ctx context.Context, sql string, args []any, a analysis) (WriteResult, error) {
	p, err := s.plan(sql, args, a)
	if err != nil {
		return WriteResult{}, err
	}
	outcomes, err := s.db.exec.Exec(ctx, p)
	if err != nil {
		return WriteResult{}, err
	}

	res := WriteResult{PerShard: make(map[ShardID]ShardOutcome, len(outcomes))}
	var failed []error
	for _, o := range outcomes {
		switch {
		case o.Err != nil:
			res.PerShard[o.Shard] = ShardOutcome{Err: &ShardError{Shard: o.Shard, Err: o.Err}}
			failed = append(failed, res.PerShard[o.Shard].Err)
		default:
			n, _ := o.Result.RowsAffected()
			res.PerShard[o.Shard] = ShardOutcome{RowsAffected: n}
		}
	}
	switch len(failed) {
	case 0:
		return res, nil
	case 1:
		return res, failed[0]
	default:
		return res, fmt.Errorf("write failed on %d of %d shards: %w", len(failed), len(outcomes), errors.Join(failed...))
	}
}

// analysis produces the Analysis of a statement. It is a function so that
// nothing is parsed when the caller has already said where the statement goes.
type analysis func() (analyze.Analysis, error)

// analyzeSQL returns an analysis that parses sql, or reports that parsing is
// not available.
func (s *scoped) analyzeSQL(sql string, args []any) analysis {
	return func() (analyze.Analysis, error) {
		if s.db.analyzer == nil {
			return analyze.Analysis{}, fmt.Errorf("%w: raw SQL cannot be routed automatically because no SQL analyzer is available "+
				"(the library was built without cgo); use db.WithShardKey / WithShard / WithAllShards, build the statement with package query, or set Config.Analyzer",
				ErrShardKeyRequired)
		}
		return s.db.analyzer.FromSQL(sql, args)
	}
}

// plan turns the routing decision into a Plan. A caller-supplied route wins;
// otherwise the statement is analyzed and routed from its own conditions.
func (s *scoped) plan(sql string, args []any, a analysis) (plan.Plan, error) {
	p := plan.Plan{SQL: sql, Args: args}
	switch s.route.kind {
	case routeByKey:
		bucket, err := s.db.router.Bucket(s.route.key)
		if err != nil {
			return plan.Plan{}, fmt.Errorf("shard key %v: %w", s.route.key, err)
		}
		id, err := s.db.router.ShardFor(s.route.key)
		if err != nil {
			return plan.Plan{}, fmt.Errorf("shard key %v: %w", s.route.key, err)
		}
		p.Targets = []ShardID{id}
		p.Strategy = plan.Single
		p.Reason = fmt.Sprintf("WithShardKey(%v): bucket %d belongs to %s", s.route.key, bucket, id)
	case routeByShard:
		if _, ok := s.db.pool.Conn(s.route.shard); !ok {
			return plan.Plan{}, fmt.Errorf("%w: %q (configured shards: %v)", ErrUnknownShard, s.route.shard, s.db.pool.IDs())
		}
		p.Targets = []ShardID{s.route.shard}
		p.Strategy = plan.Single
		p.Reason = fmt.Sprintf("WithShard(%s)", s.route.shard)
	case routeAll:
		p.Targets = s.db.pool.IDs()
		p.Strategy = plan.All
		p.Reason = "WithAllShards()"
	default:
		an, err := a()
		if err != nil {
			return plan.Plan{}, err
		}
		routed, err := plan.Route(an, s.db.registry, s.db.router, plan.Options{AnyShard: s.db.anyShard})
		if err != nil {
			return plan.Plan{}, err
		}
		p.Targets, p.Strategy, p.Reason = routed.Targets, routed.Strategy, routed.Reason
	}
	return p, nil
}
