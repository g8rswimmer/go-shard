package shard

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/merge"
	"github.com/g8rswimmer/go-shard/plan"
)

// Strategy says how many shards a statement runs on.
type Strategy = plan.Strategy

const (
	// Single: exactly one shard.
	Single = plan.Single
	// Multi: some, but not all, shards.
	Multi = plan.Multi
	// All: every shard.
	All = plan.All
)

// Explain says where a statement would run, and why, without running it. Get
// one from Querier.Explain.
type Explain struct {
	// Op is the kind of statement: SELECT, INSERT, UPDATE, DELETE, or the
	// parser's name for anything else. It is empty when the statement could not
	// be read (an override such as WithAllShards was given and no analyzer is
	// available).
	Op       string
	Strategy Strategy
	// Targets are the shards the statement runs on, in the order they are used.
	Targets []ShardID
	// Reason says which rule or override chose them.
	Reason string
	// ShardSQL is the SQL every target runs. For a query on several shards it
	// is the rewritten SELECT (hidden order columns, widened LIMIT), not the
	// statement as written. It is empty when shards run different statements;
	// see PerShard.
	ShardSQL string
	// PerShard is set instead of ShardSQL when shards run different statements:
	// a multi-row INSERT whose rows belong to different shards.
	PerShard map[ShardID]string
	// Rows is set for an INSERT ... VALUES: the indexes of the VALUES rows each
	// shard receives.
	Rows map[ShardID][]int
	// Merge lists, in order, what happens to the rows the shards return. It is
	// empty for a statement on one shard, which needs none, and for writes.
	Merge []string
	// Notes are other things worth knowing about the plan.
	Notes []string
	// ShardPlans is PostgreSQL's own EXPLAIN output for each target. It is only
	// filled when asked for with the ShardPlans or Analyze option of Querier.Explainer.
	ShardPlans map[ShardID]string
}

// String renders the plan as text for people. The layout is stable enough to
// diff, but is not meant to be parsed; use the fields for that.
func (e Explain) String() string {
	var b strings.Builder
	line := func(label, value string) { fmt.Fprintf(&b, "%-11s%s\n", label, value) }
	if e.Op != "" {
		line("op:", e.Op)
	}
	line("strategy:", e.Strategy.String())
	ids := make([]string, len(e.Targets))
	for i, id := range e.Targets {
		ids[i] = string(id)
	}
	line("targets:", strings.Join(ids, ", "))
	line("reason:", e.Reason)

	switch {
	case len(e.PerShard) > 0:
		b.WriteString("shard sql:\n")
		for _, id := range sortedShards(e.PerShard) {
			fmt.Fprintf(&b, "  %s: %s\n", id, e.PerShard[id])
		}
	default:
		line("shard sql:", e.ShardSQL)
	}
	if len(e.Rows) > 0 {
		b.WriteString("rows:\n")
		for _, id := range sortedShards(e.Rows) {
			nums := make([]string, len(e.Rows[id]))
			for i, n := range e.Rows[id] {
				nums[i] = fmt.Sprint(n)
			}
			fmt.Fprintf(&b, "  %s: %s\n", id, strings.Join(nums, ", "))
		}
	}
	if len(e.Merge) > 0 {
		b.WriteString("merge:\n")
		for i, step := range e.Merge {
			fmt.Fprintf(&b, "  %d. %s\n", i+1, step)
		}
	}
	for _, n := range e.Notes {
		fmt.Fprintf(&b, "note: %s\n", n)
	}
	if len(e.ShardPlans) > 0 {
		b.WriteString("shard plans:\n")
		for _, id := range sortedShards(e.ShardPlans) {
			fmt.Fprintf(&b, "  %s:\n", id)
			for _, line := range strings.Split(e.ShardPlans[id], "\n") {
				fmt.Fprintf(&b, "    %s\n", line)
			}
		}
	}
	return b.String()
}

func sortedShards[V any](m map[ShardID]V) []ShardID {
	ids := make([]ShardID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// ExplainOption configures an Explainer; see Querier.Explainer.
type ExplainOption func(*explainConfig)

type explainConfig struct {
	shardPlans bool
	analyze    bool
}

func newExplainConfig(opts []ExplainOption) explainConfig {
	var c explainConfig
	for _, o := range opts {
		o(&c)
	}
	// ANALYZE is a kind of shard plan
	c.shardPlans = c.shardPlans || c.analyze
	return c
}

// ShardPlans makes the Explainer also ask each target shard for PostgreSQL's
// own plan of the statement it would run (EXPLAIN), filling
// Explain.ShardPlans. The statement is not run.
func ShardPlans() ExplainOption {
	return func(c *explainConfig) { c.shardPlans = true }
}

// Analyze is ShardPlans using EXPLAIN ANALYZE, which makes PostgreSQL run the
// statement to measure it. A write is run inside a transaction that is rolled
// back, so it leaves no rows behind; side effects a rollback does not undo (a
// sequence advancing, a trigger that calls out) still happen. A SELECT is run
// for real, so expect its cost. Not available inside a transaction.
func Analyze() ExplainOption {
	return func(c *explainConfig) { c.analyze = true }
}

// Explainer is the planning half of Querier: it says where statements would
// run, without running them. Get a configured one from Querier.Explainer.
type Explainer interface {
	Explain(ctx context.Context, sql string, args ...any) (Explain, error)
	ExplainStatement(ctx context.Context, st Statement) (Explain, error)
}

// PlannerScope is what Planner.WithShardKey, WithShard and WithAllShards
// return: an Explainer with a fixed routing decision that can also be
// configured.
type PlannerScope interface {
	Explainer
	Explainer(opts ...ExplainOption) Explainer
}

// configured is an Explainer whose options were fixed when it was made. The
// two funcs are the owner's explain, already bound to its routing.
type configured struct {
	cfg  explainConfig
	sql  func(ctx context.Context, cfg explainConfig, sql string, args []any) (Explain, error)
	stmt func(ctx context.Context, cfg explainConfig, st Statement) (Explain, error)
}

func (c configured) Explain(ctx context.Context, sql string, args ...any) (Explain, error) {
	return c.sql(ctx, c.cfg, sql, args)
}

func (c configured) ExplainStatement(ctx context.Context, st Statement) (Explain, error) {
	return c.stmt(ctx, c.cfg, st)
}

// Explain implements Querier.
func (db *DB) Explain(ctx context.Context, sql string, args ...any) (Explain, error) {
	return (&scoped{db: db}).Explain(ctx, sql, args...)
}

// ExplainStatement implements Querier.
func (db *DB) ExplainStatement(ctx context.Context, st Statement) (Explain, error) {
	return (&scoped{db: db}).ExplainStatement(ctx, st)
}

// Explainer implements Querier.
func (db *DB) Explainer(opts ...ExplainOption) Explainer {
	return (&scoped{db: db}).Explainer(opts...)
}

func (s *scoped) Explain(ctx context.Context, sql string, args ...any) (Explain, error) {
	return s.explain(ctx, explainConfig{}, s.sqlRequest(sql, args))
}

func (s *scoped) ExplainStatement(ctx context.Context, st Statement) (Explain, error) {
	return s.explain(ctx, explainConfig{}, statementRequest(st))
}

func (s *scoped) Explainer(opts ...ExplainOption) Explainer {
	return configured{
		cfg: newExplainConfig(opts),
		sql: func(ctx context.Context, cfg explainConfig, sql string, args []any) (Explain, error) {
			return s.explain(ctx, cfg, s.sqlRequest(sql, args))
		},
		stmt: func(ctx context.Context, cfg explainConfig, st Statement) (Explain, error) {
			return s.explain(ctx, cfg, statementRequest(st))
		},
	}
}

// explain routes the statement exactly as running it would, so it returns the
// errors running it would return.
func (s *scoped) explain(ctx context.Context, cfg explainConfig, r request) (Explain, error) {
	p, err := s.plan(r)
	if err != nil {
		return Explain{}, err
	}
	e, final, err := describePlan(r, p)
	if err != nil {
		return Explain{}, err
	}
	if !cfg.shardPlans {
		return e, nil
	}
	if s.db.exec == nil {
		return Explain{}, errors.New("shard: this Planner is not connected to any shard, so it cannot ask them for their plans")
	}
	e.ShardPlans, err = s.db.exec.ExplainShards(ctx, final, cfg.analyze)
	if err != nil {
		return Explain{}, err
	}
	return e, nil
}

// describePlan builds the Explain for a routed statement. It also returns the
// plan with the SQL the shards would really run.
func describePlan(r request, p plan.Plan) (Explain, plan.Plan, error) {
	e := Explain{
		Strategy: p.Strategy,
		Targets:  p.Targets,
		Reason:   p.Reason,
		Rows:     p.Rows,
	}
	op, known := opOf(r)
	if known {
		e.Op = op.String()
		if op == analyze.OpOther {
			e.Op = otherName(r)
		}
	}

	switch {
	case len(p.Targets) > 1 && (!known || op == analyze.OpSelect):
		mp, err := mergePlanFor(r, p, known)
		switch {
		case err != nil && known:
			return Explain{}, p, err
		case err != nil:
			e.Notes = append(e.Notes, fmt.Sprintf("could not tell how rows would be merged: %v", err))
		default:
			p.SQL, p.Args, p.PerShard = mp.SQL, mp.Args, nil
			e.Merge = mp.Spec.Steps()
		}
	default:
		// one shard, or a write: nothing to merge
	}

	switch {
	case len(p.PerShard) > 0:
		e.PerShard = make(map[ShardID]string, len(p.PerShard))
		for id, st := range p.PerShard {
			e.PerShard[id] = st.SQL
		}
	default:
		e.ShardSQL = p.SQL
	}
	return e, p, nil
}

// mergePlanFor rewrites a SELECT for several shards, as fanOut does. The
// error is the one running the query would return.
func mergePlanFor(r request, p plan.Plan, known bool) (merge.Plan, error) {
	if r.merge == nil {
		if !known {
			return merge.Plan{}, errors.New("no SQL analyzer is available")
		}
		return merge.Plan{}, fmt.Errorf("%w: running a query on %d shards needs its results merged, and merging needs the SQL rewritten, "+
			"which needs an analyzer that supports it (the library was built without cgo); build the statement with package query, "+
			"narrow it to one shard key, use WithShardKey / WithShard, or set Config.Analyzer to one that also implements merge.Planner",
			ErrUnsupportedQuery, len(p.Targets))
	}
	return r.merge()
}

// opOf says what kind of statement a request is, if that can be told. It does
// not fail: an override route may carry SQL the analyzer cannot read.
func opOf(r request) (analyze.OpKind, bool) {
	if r.analysis == nil {
		return 0, false
	}
	an, err := r.analysis()
	if err != nil {
		return 0, false
	}
	return an.Op, true
}

func otherName(r request) string {
	if an, err := r.analysis(); err == nil && an.Kind != "" {
		return an.Kind
	}
	return analyze.OpOther.String()
}

// ---- Planner ----------------------------------------------------------------

// Planner routes and explains statements without connecting to any shard. It
// is built from the same Config as Open (shard DSNs may be left empty) and
// makes the same decisions, so it can answer "where would this go?" in a unit
// test or a tool. Package shardtest builds its fake on it.
type Planner struct{ db *DB }

var _ Explainer = (*Planner)(nil)

// NewPlanner validates cfg like Open does, except that no shard is contacted
// and DSNs are not required.
func NewPlanner(cfg Config) (*Planner, error) {
	r, err := cfg.validateShape(false)
	if err != nil {
		return nil, err
	}
	conns := make(map[ShardID]exec.Conn, len(cfg.Shards))
	for _, sc := range cfg.Shards {
		conns[sc.ID] = nil // never used: a Planner only plans
	}
	return &Planner{db: newDB(cfg, r, exec.NewPool(conns), nil)}, nil
}

// Explainer returns an Explainer with the options set. A Planner has no
// shards to ask, so ShardPlans and Analyze make its Explain fail.
func (p *Planner) Explainer(opts ...ExplainOption) Explainer { return p.db.Explainer(opts...) }

// Explain says where a statement would run, finding the shard from the
// statement.
func (p *Planner) Explain(ctx context.Context, sql string, args ...any) (Explain, error) {
	return p.db.Explain(ctx, sql, args...)
}

// ExplainStatement is Explain for a built Statement.
func (p *Planner) ExplainStatement(ctx context.Context, st Statement) (Explain, error) {
	return p.db.ExplainStatement(ctx, st)
}

// WithShardKey explains statements as if routed with DB.WithShardKey.
func (p *Planner) WithShardKey(key any) PlannerScope {
	return &scoped{p.db, route{kind: routeByKey, key: key}}
}

// WithShard explains statements as if routed with DB.WithShard.
func (p *Planner) WithShard(id ShardID) PlannerScope {
	return &scoped{p.db, route{kind: routeByShard, shard: id}}
}

// WithAllShards explains statements as if routed with DB.WithAllShards.
func (p *Planner) WithAllShards() PlannerScope { return &scoped{p.db, route{kind: routeAll}} }

// Shards returns every shard ID, sorted.
func (p *Planner) Shards() []ShardID { return p.db.Shards() }
