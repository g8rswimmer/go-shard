package shardtest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/merge"
	"github.com/g8rswimmer/go-shard/registry"
)

// CallKind says whether a recorded call was a read or a write.
type CallKind int

const (
	// QueryCall is a Query or QueryStatement.
	QueryCall CallKind = iota + 1
	// ExecCall is an Exec or ExecStatement.
	ExecCall
)

// String returns "query" or "exec".
func (k CallKind) String() string {
	switch k {
	case QueryCall:
		return "query"
	case ExecCall:
		return "exec"
	default:
		return fmt.Sprintf("CallKind(%d)", int(k))
	}
}

// Call is one statement the code under test sent to a Fake.
type Call struct {
	Kind CallKind
	SQL  string
	Args []any
	// Plan is where the statement would have run, decided by the same registry,
	// router and analyzer a real shard.DB uses. It is the zero value when
	// routing failed.
	Plan shard.Explain
	// Err is the error routing returned to the code under test, such as
	// ErrShardKeyRequired. Nil if the statement was routed.
	Err error
}

// Fake is a stand-in for a *shard.DB that needs no database. It routes every
// statement with the real rules, records where it would have gone, and answers
// with the results you give it. Code under test depends on shard.Querier; hand
// it a Fake in a unit test and a *shard.DB in production.
//
//	fake := shardtest.NewFake(t, reg, "shard-01", "shard-02", "shard-03")
//	fake.StubRows("FROM profiles", []string{"id", "name"}, []any{42, "Ada"})
//
//	name, err := repo.NameOf(ctx, fake, 42) // code under test
//
//	fake.AssertSingleShard("FROM profiles")
//
// A Fake does not run SQL, merge shard results or keep data: a query returns
// the rows you stubbed (the same rows whatever the shards), and a write reports
// the rows you stubbed as affected on each target shard. What it checks is
// routing, which is the part that breaks when a query changes. Transactions are
// not faked; test them against a Cluster.
//
// Routing raw SQL needs the SQL parser (cgo). Without cgo, build statements
// with package query, or route with WithShardKey / WithShard / WithAllShards.
type Fake struct {
	t       testing.TB
	planner *shard.Planner

	mu    sync.Mutex
	calls []Call
	stubs []stub
}

var _ shard.Querier = (*Fake)(nil)

type stub struct {
	match    string
	columns  []string
	rows     [][]any
	affected int64
	err      error
}

// NewFake returns a Fake for a cluster with the given shards. Buckets are split
// evenly in the order the shards are listed, as Open does by default, so a key
// goes to the same shard here as it would in a DB configured the same way.
func NewFake(t testing.TB, reg *registry.Registry, shards ...shard.ShardID) *Fake {
	t.Helper()
	cfg := shard.Config{Registry: reg}
	for _, id := range shards {
		cfg.Shards = append(cfg.Shards, shard.ShardConfig{ID: id})
	}
	p, err := shard.NewPlanner(cfg)
	if err != nil {
		t.Fatalf("shardtest: NewFake: %v", err)
	}
	return &Fake{t: t, planner: p}
}

// StubRows makes queries whose SQL contains match return these columns and rows.
// An empty match matches every query. When several stubs match, the last one
// added wins.
func (f *Fake) StubRows(match string, columns []string, rows ...[]any) {
	f.add(stub{match: match, columns: columns, rows: rows})
}

// StubAffected makes writes whose SQL contains match report n rows affected on
// each shard they run on.
func (f *Fake) StubAffected(match string, n int64) { f.add(stub{match: match, affected: n}) }

// StubError makes statements whose SQL contains match fail with err, after they
// have been routed and recorded.
func (f *Fake) StubError(match string, err error) { f.add(stub{match: match, err: err}) }

func (f *Fake) add(s stub) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stubs = append(f.stubs, s)
}

// Calls returns every statement received so far, oldest first.
func (f *Fake) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// Reset forgets the recorded calls. Stubs are kept.
func (f *Fake) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

// LastPlan returns where the most recent statement would have run. It fails the
// test if there was none, or if routing it failed.
func (f *Fake) LastPlan() shard.Explain {
	f.t.Helper()
	calls := f.Calls()
	if len(calls) == 0 {
		f.t.Fatalf("shardtest: LastPlan: no statement was sent to the fake")
	}
	last := calls[len(calls)-1]
	if last.Err != nil {
		f.t.Fatalf("shardtest: LastPlan: the last statement was refused: %v\n  %s", last.Err, last.SQL)
	}
	return last.Plan
}

// AssertRoutes checks that the most recent statement containing match would
// have run on exactly these shards (in any order).
func (f *Fake) AssertRoutes(match string, shards ...shard.ShardID) {
	f.t.Helper()
	c, ok := f.find(match)
	if !ok {
		return
	}
	got := slices.Clone(c.Plan.Targets)
	want := slices.Clone(shards)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		f.t.Errorf("shardtest: %q would run on %v, want %v\n  reason: %s", match, c.Plan.Targets, shards, c.Plan.Reason)
	}
}

// AssertSingleShard checks that the most recent statement containing match
// would have run on exactly one shard, and returns it. It returns "" if the
// check failed.
func (f *Fake) AssertSingleShard(match string) shard.ShardID {
	f.t.Helper()
	c, ok := f.find(match)
	if !ok {
		return ""
	}
	if len(c.Plan.Targets) != 1 {
		f.t.Errorf("shardtest: %q would run on %d shards %v, want one\n  reason: %s", match, len(c.Plan.Targets), c.Plan.Targets, c.Plan.Reason)
		return ""
	}
	return c.Plan.Targets[0]
}

// AssertFanout checks that the most recent statement containing match would
// have run on several shards. If shards are given, they must be exactly the
// targets (in any order).
func (f *Fake) AssertFanout(match string, shards ...shard.ShardID) {
	f.t.Helper()
	c, ok := f.find(match)
	if !ok {
		return
	}
	if len(c.Plan.Targets) < 2 {
		f.t.Errorf("shardtest: %q would run on %d shard(s) %v, want a fan-out\n  reason: %s", match, len(c.Plan.Targets), c.Plan.Targets, c.Plan.Reason)
		return
	}
	if len(shards) > 0 {
		f.AssertRoutes(match, shards...)
	}
}

// find returns the most recent routed call whose SQL contains match, and
// reports a test failure if there is none.
func (f *Fake) find(match string) (Call, bool) {
	f.t.Helper()
	calls := f.Calls()
	for i := len(calls) - 1; i >= 0; i-- {
		switch c := calls[i]; {
		case !strings.Contains(c.SQL, match):
			// not this one
		case c.Err != nil:
			f.t.Errorf("shardtest: %q was refused, not routed: %v", match, c.Err)
			return Call{}, false
		default:
			return c, true
		}
	}
	seen := make([]string, len(calls))
	for i, c := range calls {
		seen[i] = c.SQL
	}
	f.t.Errorf("shardtest: no statement contains %q; received %q", match, seen)
	return Call{}, false
}

// record routes a statement, remembers it, and returns the stub that answers it.
func (f *Fake) record(kind CallKind, sql string, args []any, e shard.Explain, err error) (stub, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, Call{Kind: kind, SQL: sql, Args: args, Plan: e, Err: err})
	if err != nil {
		return stub{}, err
	}
	for i := len(f.stubs) - 1; i >= 0; i-- {
		if strings.Contains(sql, f.stubs[i].match) {
			return f.stubs[i], f.stubs[i].err
		}
	}
	return stub{}, nil
}

// ---- shard.Querier ----------------------------------------------------------

// Query implements shard.Querier.
func (f *Fake) Query(ctx context.Context, sql string, args ...any) (shard.Rows, error) {
	return f.query(ctx, f.planner, sql, args)
}

// Exec implements shard.Querier.
func (f *Fake) Exec(ctx context.Context, sql string, args ...any) (shard.WriteResult, error) {
	return f.exec(ctx, f.planner, sql, args)
}

// QueryStatement implements shard.Querier.
func (f *Fake) QueryStatement(ctx context.Context, st shard.Statement) (shard.Rows, error) {
	return f.queryStatement(ctx, f.planner, st)
}

// ExecStatement implements shard.Querier.
func (f *Fake) ExecStatement(ctx context.Context, st shard.Statement) (shard.WriteResult, error) {
	return f.execStatement(ctx, f.planner, st)
}

// Explain implements shard.Querier. It does not count as a call.
func (f *Fake) Explain(ctx context.Context, sql string, args ...any) (shard.Explain, error) {
	return f.planner.Explain(ctx, sql, args...)
}

// ExplainStatement implements shard.Querier. It does not count as a call.
func (f *Fake) ExplainStatement(ctx context.Context, st shard.Statement) (shard.Explain, error) {
	return f.planner.ExplainStatement(ctx, st)
}

// Explainer implements shard.Querier. A Fake has no shards, so the ShardPlans
// and Analyze options make its Explain fail.
func (f *Fake) Explainer(opts ...shard.ExplainOption) shard.Explainer {
	return f.planner.Explainer(opts...)
}

// WithShardKey routes statements to the shard that owns key, like DB.WithShardKey.
func (f *Fake) WithShardKey(key any) shard.Querier {
	return &fakeScope{f, f.planner.WithShardKey(key)}
}

// WithShard routes statements to one named shard, like DB.WithShard.
func (f *Fake) WithShard(id shard.ShardID) shard.Querier {
	return &fakeScope{f, f.planner.WithShard(id)}
}

// WithAllShards runs statements on every shard, like DB.WithAllShards.
func (f *Fake) WithAllShards() shard.Querier {
	return &fakeScope{f, f.planner.WithAllShards()}
}

// fakeScope is a Fake with a fixed routing decision.
type fakeScope struct {
	f  *Fake
	ex shard.PlannerScope
}

func (s *fakeScope) Explainer(opts ...shard.ExplainOption) shard.Explainer {
	return s.ex.Explainer(opts...)
}

func (s *fakeScope) Query(ctx context.Context, sql string, args ...any) (shard.Rows, error) {
	return s.f.query(ctx, s.ex, sql, args)
}

func (s *fakeScope) Exec(ctx context.Context, sql string, args ...any) (shard.WriteResult, error) {
	return s.f.exec(ctx, s.ex, sql, args)
}

func (s *fakeScope) QueryStatement(ctx context.Context, st shard.Statement) (shard.Rows, error) {
	return s.f.queryStatement(ctx, s.ex, st)
}

func (s *fakeScope) ExecStatement(ctx context.Context, st shard.Statement) (shard.WriteResult, error) {
	return s.f.execStatement(ctx, s.ex, st)
}

func (s *fakeScope) Explain(ctx context.Context, sql string, args ...any) (shard.Explain, error) {
	return s.ex.Explain(ctx, sql, args...)
}

func (s *fakeScope) ExplainStatement(ctx context.Context, st shard.Statement) (shard.Explain, error) {
	return s.ex.ExplainStatement(ctx, st)
}

func (f *Fake) query(ctx context.Context, ex shard.Explainer, sql string, args []any) (shard.Rows, error) {
	e, err := ex.Explain(ctx, sql, args...)
	return f.answerQuery(sql, args, e, err)
}

func (f *Fake) queryStatement(ctx context.Context, ex shard.Explainer, st shard.Statement) (shard.Rows, error) {
	e, err := ex.ExplainStatement(ctx, st)
	return f.answerQuery(st.SQL(), st.Args(), e, err)
}

func (f *Fake) exec(ctx context.Context, ex shard.Explainer, sql string, args []any) (shard.WriteResult, error) {
	e, err := ex.Explain(ctx, sql, args...)
	return f.answerExec(sql, args, e, err)
}

func (f *Fake) execStatement(ctx context.Context, ex shard.Explainer, st shard.Statement) (shard.WriteResult, error) {
	e, err := ex.ExplainStatement(ctx, st)
	return f.answerExec(st.SQL(), st.Args(), e, err)
}

func (f *Fake) answerQuery(sql string, args []any, e shard.Explain, err error) (shard.Rows, error) {
	s, err := f.record(QueryCall, sql, args, e, err)
	if err != nil {
		return nil, err
	}
	// merge.Merge with no Spec just hands out one source's rows, and gives them
	// the same Scan as a real result.
	rows, err := merge.Merge(merge.Spec{}, []merge.Source{&sliceRows{cols: s.columns, rows: s.rows, at: -1}}, merge.Options{})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (f *Fake) answerExec(sql string, args []any, e shard.Explain, err error) (shard.WriteResult, error) {
	s, err := f.record(ExecCall, sql, args, e, err)
	if err != nil {
		return shard.WriteResult{}, err
	}
	res := shard.WriteResult{PerShard: make(map[shard.ShardID]shard.ShardOutcome, len(e.Targets))}
	for _, id := range e.Targets {
		res.PerShard[id] = shard.ShardOutcome{RowsAffected: s.affected, Rows: e.Rows[id]}
	}
	return res, nil
}

// sliceRows is a merge.Source over rows held in memory.
type sliceRows struct {
	cols []string
	rows [][]any
	at   int
}

func (r *sliceRows) Next() bool {
	r.at++
	return r.at < len(r.rows)
}

func (r *sliceRows) Scan(dest ...any) error {
	if len(dest) != len(r.rows[r.at]) {
		return fmt.Errorf("shardtest: a stubbed row has %d values for %d columns", len(r.rows[r.at]), len(dest))
	}
	for i, d := range dest {
		p, ok := d.(*any)
		if !ok {
			return fmt.Errorf("shardtest: cannot scan into %T", d)
		}
		*p = r.rows[r.at][i]
	}
	return nil
}

func (r *sliceRows) Columns() ([]string, error) { return r.cols, nil }
func (r *sliceRows) Err() error                 { return nil }
func (r *sliceRows) Close() error               { return nil }
