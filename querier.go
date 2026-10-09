package shard

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/merge"
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
	// Rows lists which VALUES rows of an INSERT this shard received, as indexes
	// into the statement's rows. It is nil for other statements. After a
	// partial failure it tells you which rows to retry.
	Rows []int
	// Replayed is true when an idempotency key showed that the write had
	// already been applied on this shard, so it was not repeated.
	// RowsAffected is then what the first attempt reported.
	Replayed bool
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

// Failed returns the error from each shard where the write failed.
func (r WriteResult) Failed() map[ShardID]error {
	var out map[ShardID]error
	for id, o := range r.PerShard {
		if o.Err != nil {
			if out == nil {
				out = map[ShardID]error{}
			}
			out[id] = o.Err
		}
	}
	return out
}

// FailedRows returns, sorted, the INSERT rows that went to shards where the
// write failed: the rows to send again. Rows that were applied are not listed.
func (r WriteResult) FailedRows() []int {
	var rows []int
	for _, o := range r.PerShard {
		if o.Err != nil {
			rows = append(rows, o.Rows...)
		}
	}
	sort.Ints(rows)
	return rows
}

// Statement is a statement that already knows how it routes. Build one with
// package query.
type Statement interface {
	SQL() string
	Args() []any
	Analysis() analyze.Analysis
}

// statementMerger is implemented by statements that can rewrite themselves for
// several shards (package query's SELECT does).
type statementMerger interface {
	MergePlan() (merge.Plan, error)
}

// rowSplitter is implemented by statements that can restrict an INSERT to some
// of its rows (package query's do).
type rowSplitter interface {
	SplitRows(rows []int) (string, []any, error)
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
	// An INSERT with several rows is split so each shard receives only its own
	// rows. See WithIdempotencyKey to make a retry safe.
	Exec(ctx context.Context, sql string, args ...any) (WriteResult, error)
	// QueryStatement is Query for a built Statement; it needs no SQL parsing.
	QueryStatement(ctx context.Context, st Statement) (Rows, error)
	// ExecStatement is Exec for a built Statement; it needs no SQL parsing.
	ExecStatement(ctx context.Context, st Statement) (WriteResult, error)
	// Explain says where a statement would run and why, without running it.
	// It routes exactly as Query and Exec do, so it returns the same errors.
	Explain(ctx context.Context, sql string, args ...any) (Explain, error)
	// ExplainStatement is Explain for a built Statement.
	ExplainStatement(ctx context.Context, st Statement) (Explain, error)
	// Explainer returns an Explainer that explains with the options given:
	// ShardPlans adds each shard's own PostgreSQL plan, Analyze measures it.
	// Explainer() with none is Explain and ExplainStatement.
	Explainer(opts ...ExplainOption) Explainer
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

// WithAllShards runs statements on every shard. A query's rows are merged:
// ORDER BY, LIMIT, OFFSET, DISTINCT, aggregates and GROUP BY give the answer
// one database would; see the package documentation for what is supported.
func (db *DB) WithAllShards() Querier { return &scoped{db, route{kind: routeAll}} }

// request is one statement to run.
type request struct {
	sql      string
	args     []any
	analysis analysis
	// split restricts an INSERT to some of its VALUES rows. It is nil when the
	// statement cannot be split.
	split func(rows []int) (string, []any, error)
	// merge rewrites a SELECT for several shards. It is nil when the statement
	// cannot be.
	merge func() (merge.Plan, error)
}

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
	return s.query(ctx, s.sqlRequest(sql, args))
}

func (s *scoped) QueryStatement(ctx context.Context, st Statement) (Rows, error) {
	return s.query(ctx, statementRequest(st))
}

func (s *scoped) Exec(ctx context.Context, sql string, args ...any) (WriteResult, error) {
	return s.exec(ctx, s.sqlRequest(sql, args))
}

func (s *scoped) ExecStatement(ctx context.Context, st Statement) (WriteResult, error) {
	return s.exec(ctx, statementRequest(st))
}

// sqlRequest wraps raw SQL: it is analyzed, and split, by the DB's analyzer.
func (s *scoped) sqlRequest(sql string, args []any) request {
	r := request{sql: sql, args: args, analysis: s.analyzeSQL(sql, args)}
	if sp, ok := s.db.analyzer.(analyze.RowSplitter); ok {
		r.split = func(rows []int) (string, []any, error) { return sp.SplitRows(sql, args, rows) }
	}
	if mp, ok := s.db.analyzer.(merge.Planner); ok {
		r.merge = func() (merge.Plan, error) { return mp.PlanMerge(sql, args) }
	}
	return r
}

// statementRequest wraps a built statement, which brings its own analysis.
func statementRequest(st Statement) request {
	r := request{
		sql:      st.SQL(),
		args:     st.Args(),
		analysis: func() (analyze.Analysis, error) { return st.Analysis(), nil },
	}
	if sp, ok := st.(rowSplitter); ok {
		r.split = sp.SplitRows
	}
	if mp, ok := st.(statementMerger); ok {
		r.merge = mp.MergePlan
	}
	return r
}

func (s *scoped) query(ctx context.Context, r request) (Rows, error) {
	p, err := s.plan(r)
	if err != nil {
		return nil, err
	}
	if len(p.Targets) > 1 {
		return s.fanOut(ctx, r, p)
	}
	res, err := s.db.exec.Query(ctx, p)
	if err != nil {
		return nil, err
	}
	return res[0].Rows, nil
}

func (s *scoped) exec(ctx context.Context, r request) (WriteResult, error) {
	p, err := s.plan(r)
	if err != nil {
		return WriteResult{}, err
	}

	var outcomes []exec.ShardExec
	key, hasKey := IdempotencyKey(ctx)
	switch {
	case hasKey && key == "":
		return WriteResult{}, errors.New("shard: the idempotency key is empty")
	case hasKey:
		outcomes, err = s.db.exec.ExecIdempotent(ctx, p, exec.Idempotency{Key: key, Table: s.db.idemTable})
	default:
		outcomes, err = s.db.exec.Exec(ctx, p)
	}
	if err != nil {
		return WriteResult{}, err
	}

	res := WriteResult{PerShard: make(map[ShardID]ShardOutcome, len(outcomes))}
	var failed []error
	for _, o := range outcomes {
		out := ShardOutcome{RowsAffected: o.RowsAffected, Rows: o.Rows, Replayed: o.Replayed}
		if o.Err != nil {
			out = ShardOutcome{Rows: o.Rows, Err: &ShardError{Shard: o.Shard, Err: o.Err}}
			failed = append(failed, out.Err)
		}
		res.PerShard[o.Shard] = out
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
func (s *scoped) plan(r request) (plan.Plan, error) {
	p := plan.Plan{SQL: r.sql, Args: r.args}
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
		an, err := r.analysis()
		if err != nil {
			return plan.Plan{}, err
		}
		routed, err := plan.Route(an, s.db.registry, s.db.router, plan.Options{AnyShard: s.db.anyShard})
		if err != nil {
			return plan.Plan{}, err
		}
		p.Targets, p.Strategy, p.Reason, p.Rows = routed.Targets, routed.Strategy, routed.Reason, routed.Rows
		if err := splitInsert(&p, r); err != nil {
			return plan.Plan{}, err
		}
	}
	return p, nil
}

// splitInsert gives each shard of a multi-shard INSERT a statement holding only
// its own rows.
func splitInsert(p *plan.Plan, r request) error {
	if len(p.Targets) < 2 || len(p.Rows) < 2 {
		return nil
	}
	if r.split == nil {
		return fmt.Errorf("%w: the rows of this INSERT belong to %d different shards and this statement cannot be split; "+
			"insert the rows in separate calls, or use package query, whose INSERT can be split", ErrUnsupportedQuery, len(p.Targets))
	}
	p.PerShard = make(map[ShardID]plan.ShardStatement, len(p.Targets))
	for _, id := range p.Targets {
		sql, args, err := r.split(p.Rows[id])
		if err != nil {
			return fmt.Errorf("splitting the INSERT for %s: %w", id, err)
		}
		p.PerShard[id] = plan.ShardStatement{SQL: sql, Args: args}
	}
	return nil
}
