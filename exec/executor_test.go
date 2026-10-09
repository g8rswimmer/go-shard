package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/router"
)

// newPool builds a pool of fake shards named "s0", "s1", ...
func newPool(t *testing.T, fakes ...*fakeShard) *Pool {
	t.Helper()
	conns := map[router.ShardID]Conn{}
	for i, f := range fakes {
		conns[router.ShardID(fmt.Sprintf("s%d", i))] = f.db()
	}
	p := NewPool(conns)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func planFor(ids ...router.ShardID) plan.Plan {
	return plan.Plan{SQL: "SELECT 1", Targets: ids, Strategy: plan.Multi}
}

func blockUntilDone(ctx context.Context) (int, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestQueryReturnsRowsFromEveryTarget(t *testing.T) {
	pool := newPool(t,
		&fakeShard{query: func(context.Context) (int, error) { return 2, nil }},
		&fakeShard{query: func(context.Context) (int, error) { return 3, nil }},
	)
	got, err := NewExecutor(pool, Options{}).Query(context.Background(), planFor("s0", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []int{2, 3} {
		if got[i].Shard != router.ShardID(fmt.Sprintf("s%d", i)) {
			t.Errorf("result %d is from shard %s", i, got[i].Shard)
		}
		n := 0
		for got[i].Rows.Next() {
			n++
		}
		if err := got[i].Rows.Err(); err != nil || n != want {
			t.Errorf("shard %s: read %d rows, err %v; want %d rows", got[i].Shard, n, err, want)
		}
		_ = got[i].Rows.Close()
	}
}

// Rows must stay readable after Query returns: the executor's internal
// fail-fast context is cancelled on return and must not reach the rows.
func TestRowsOutliveQuery(t *testing.T) {
	pool := newPool(t, &fakeShard{query: func(context.Context) (int, error) { return 5, nil }})
	got, err := NewExecutor(pool, Options{}).Query(context.Background(), planFor("s0"))
	if err != nil {
		t.Fatal(err)
	}
	defer got[0].Rows.Close()
	time.Sleep(20 * time.Millisecond) // let any stray cancellation take effect
	n := 0
	for got[0].Rows.Next() {
		n++
	}
	if err := got[0].Rows.Err(); err != nil || n != 5 {
		t.Fatalf("read %d rows, err %v; want 5 rows, no error", n, err)
	}
}

func TestQueryBoundsConcurrency(t *testing.T) {
	shared := &fakeShard{}
	// All shards share one fake so in-flight is counted across them.
	shared.query = func(context.Context) (int, error) { time.Sleep(30 * time.Millisecond); return 1, nil }
	conns := map[router.ShardID]Conn{}
	var ids []router.ShardID
	for i := 0; i < 6; i++ {
		id := router.ShardID(fmt.Sprintf("s%d", i))
		conns[id] = shared.db()
		ids = append(ids, id)
	}
	pool := NewPool(conns)
	defer pool.Close()

	got, err := NewExecutor(pool, Options{MaxFanout: 2}).Query(context.Background(), planFor(ids...))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range got {
		_ = r.Rows.Close()
	}
	if m := shared.maxInFlight.Load(); m != 2 {
		t.Errorf("max concurrent shard queries = %d, want exactly 2 (the MaxFanout)", m)
	}
}

func TestQueryDefaultFanout(t *testing.T) {
	if e := NewExecutor(NewPool(nil), Options{}); e.maxFanout != DefaultMaxFanout {
		t.Errorf("maxFanout = %d, want %d", e.maxFanout, DefaultMaxFanout)
	}
}

func TestQueryFailsFastAndCancelsOthers(t *testing.T) {
	// The failure waits until the slow shard is running its query: if the other
	// shard failed first, the slow query could be cancelled before it began and
	// the driver would never see it.
	slowRunning := make(chan struct{})
	slow := &fakeShard{query: func(ctx context.Context) (int, error) {
		close(slowRunning)
		return blockUntilDone(ctx)
	}}
	bad := &fakeShard{query: func(context.Context) (int, error) {
		<-slowRunning
		return 0, errors.New("boom")
	}}
	ok := &fakeShard{query: func(context.Context) (int, error) { return 1, nil }}
	pool := newPool(t, slow, bad, ok)

	start := time.Now()
	got, err := NewExecutor(pool, Options{}).Query(context.Background(), planFor("s0", "s1", "s2"))
	if got != nil {
		t.Error("no results should be returned on failure")
	}
	var se *ShardError
	if !errors.As(err, &se) || se.Shard != "s1" || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error = %v, want a *ShardError for s1 (the shard that failed first)", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("took %v: the blocked shard was not cancelled", d)
	}
	if slow.cancelledQueries.Load() != 1 {
		t.Error("the blocked shard's query should have been cancelled")
	}
	if n := ok.openRows.Load(); n != 0 {
		t.Errorf("%d result sets left open after a failure", n)
	}
}

func TestQueryShardTimeout(t *testing.T) {
	pool := newPool(t, &fakeShard{query: blockUntilDone})
	_, err := NewExecutor(pool, Options{ShardTimeout: 20 * time.Millisecond}).Query(context.Background(), planFor("s0"))
	var se *ShardError
	if !errors.As(err, &se) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want a ShardError wrapping DeadlineExceeded", err)
	}
}

func TestQueryCallerCancellation(t *testing.T) {
	pool := newPool(t, &fakeShard{query: blockUntilDone})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	_, err := NewExecutor(pool, Options{}).Query(ctx, planFor("s0"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestRowsCloseReleasesContext(t *testing.T) {
	pool := newPool(t, &fakeShard{query: func(context.Context) (int, error) { return 1, nil }})
	got, err := NewExecutor(pool, Options{ShardTimeout: time.Minute}).Query(context.Background(), planFor("s0"))
	if err != nil {
		t.Fatal(err)
	}
	var released atomic.Bool
	prev := got[0].Rows.cancel
	got[0].Rows.cancel = func() { released.Store(true); prev() }
	if err := got[0].Rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !released.Load() {
		t.Error("Close did not release the per-shard context")
	}
}

func TestExecDoesNotFailFast(t *testing.T) {
	pool := newPool(t,
		&fakeShard{exec: func(context.Context) (int64, error) { return 0, errors.New("disk full") }},
		&fakeShard{exec: func(context.Context) (int64, error) { return 7, nil }},
	)
	out, err := NewExecutor(pool, Options{}).Exec(context.Background(), planFor("s0", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Shard != "s0" || out[0].Err == nil || out[0].RowsAffected != 0 {
		t.Errorf("s0 outcome = %+v, want the failure", out[0])
	}
	if out[1].Shard != "s1" || out[1].Err != nil || out[1].RowsAffected != 7 {
		t.Errorf("s1 outcome = %+v, want 7 rows affected: a failure elsewhere must not cancel it", out[1])
	}
}

func TestExecShardTimeout(t *testing.T) {
	pool := newPool(t, &fakeShard{exec: func(ctx context.Context) (int64, error) { <-ctx.Done(); return 0, ctx.Err() }})
	out, _ := NewExecutor(pool, Options{ShardTimeout: 20 * time.Millisecond}).Exec(context.Background(), planFor("s0"))
	if !errors.Is(out[0].Err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want DeadlineExceeded", out[0].Err)
	}
}

func TestPlanValidation(t *testing.T) {
	pool := newPool(t, &fakeShard{}, &fakeShard{})
	e := NewExecutor(pool, Options{})
	tests := []struct {
		name    string
		targets []router.ShardID
		want    string
		is      error
	}{
		{"no targets", nil, "no target shards", nil},
		{"unknown shard", []router.ShardID{"s0", "nope"}, `"nope"`, ErrUnknownShard},
		{"repeated shard", []router.ShardID{"s0", "s0"}, "more than once", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, qerr := e.Query(context.Background(), planFor(tc.targets...))
			_, eerr := e.Exec(context.Background(), planFor(tc.targets...))
			for _, err := range []error{qerr, eerr} {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Errorf("error = %v, want it to mention %q", err, tc.want)
				}
				if tc.is != nil && !errors.Is(err, tc.is) {
					t.Errorf("error = %v, want %v", err, tc.is)
				}
			}
		})
	}
}

func TestHealth(t *testing.T) {
	pool := newPool(t,
		&fakeShard{},
		&fakeShard{ping: func(context.Context) error { return errors.New("connection refused") }},
	)
	h := pool.Health(context.Background())
	if len(h) != 2 || h[0].ID != "s0" || h[1].ID != "s1" {
		t.Fatalf("health = %+v, want s0 then s1", h)
	}
	if !h[0].Healthy() || h[1].Healthy() || h[1].Err == nil {
		t.Errorf("s0 healthy=%v, s1 healthy=%v err=%v", h[0].Healthy(), h[1].Healthy(), h[1].Err)
	}
}

func TestPoolIDsSortedCopy(t *testing.T) {
	pool := newPool(t, &fakeShard{}, &fakeShard{}, &fakeShard{})
	ids := pool.IDs()
	if fmt.Sprint(ids) != "[s0 s1 s2]" {
		t.Errorf("IDs() = %v", ids)
	}
	ids[0] = "mutated"
	if pool.IDs()[0] != "s0" {
		t.Error("IDs() must return a copy")
	}
}

func TestOpenErrorsNameTheShardNotTheDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		// Port 1 refuses connections immediately; no database needed.
		{"unreachable", "postgres://shard:hunter2@127.0.0.1:1/shard?sslmode=disable&connect_timeout=2"},
		{"unparseable", "postgres://shard:hunter2@127.0.0.1:notaport/shard"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Open(context.Background(), []Spec{{ID: "shard-07", DSN: tc.dsn}})
			var se *ShardError
			if !errors.As(err, &se) || se.Shard != "shard-07" {
				t.Fatalf("error = %v, want a *ShardError for shard-07", err)
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the password: %v", err)
			}
		})
	}
}

func TestExecSendsEachShardItsOwnStatement(t *testing.T) {
	a, b, c := &fakeShard{}, &fakeShard{}, &fakeShard{}
	pool := newPool(t, a, b, c)
	p := plan.Plan{
		SQL:     "INSERT everything",
		Targets: []router.ShardID{"s0", "s1", "s2"},
		PerShard: map[router.ShardID]plan.ShardStatement{
			"s0": {SQL: "INSERT rows 0 and 2"},
			"s1": {SQL: "INSERT row 1"},
		},
		Rows: map[router.ShardID][]int{"s0": {0, 2}, "s1": {1}},
	}
	out, err := NewExecutor(pool, Options{}).Exec(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}

	for fake, want := range map[*fakeShard]string{a: "INSERT rows 0 and 2", b: "INSERT row 1", c: "INSERT everything"} {
		if got := fake.queries(); len(got) != 1 || got[0] != want {
			t.Errorf("a shard ran %v, want [%q]", got, want)
		}
	}
	if fmt.Sprint(out[0].Rows) != "[0 2]" || fmt.Sprint(out[1].Rows) != "[1]" || out[2].Rows != nil {
		t.Errorf("outcomes must report the rows each shard received: %+v", out)
	}
}

func TestQueryUsesThePerShardStatement(t *testing.T) {
	f := &fakeShard{query: func(context.Context) (int, error) { return 1, nil }}
	pool := newPool(t, f)
	p := plan.Plan{SQL: "generic", Targets: []router.ShardID{"s0"},
		PerShard: map[router.ShardID]plan.ShardStatement{"s0": {SQL: "specific"}}}
	got, err := NewExecutor(pool, Options{}).Query(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	_ = got[0].Rows.Close()
	if q := f.queries(); len(q) != 1 || q[0] != "specific" {
		t.Errorf("ran %v, want the per-shard statement", q)
	}
}

func TestQueryPartialKeepsTheShardsThatAnswer(t *testing.T) {
	boom := func(msg string) *fakeShard {
		return &fakeShard{query: func(context.Context) (int, error) { return 0, errors.New(msg) }}
	}
	slow := &fakeShard{query: blockUntilDone}
	ok := &fakeShard{query: func(context.Context) (int, error) { return 2, nil }}
	pool := newPool(t, boom("first"), ok, boom("third"), ok)

	rows, failed, err := NewExecutor(pool, Options{}).QueryPartial(context.Background(), planFor("s0", "s1", "s2", "s3"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Shard != "s1" || rows[1].Shard != "s3" {
		t.Errorf("rows from %v, want s1 and s3", rows)
	}
	if len(failed) != 2 || failed[0].Shard != "s0" || failed[1].Shard != "s2" {
		t.Errorf("failed = %v, want s0 and s2 in plan order", failed)
	}
	for _, r := range rows {
		n := 0
		for r.Rows.Next() {
			n++
		}
		if n != 2 || r.Rows.Err() != nil {
			t.Errorf("shard %s: %d rows, err %v", r.Shard, n, r.Rows.Err())
		}
		_ = r.Rows.Close()
	}

	// A failing shard must not cancel the others (unlike Query): the slow
	// shard here finishes only when its own timeout ends it.
	pool = newPool(t, boom("x"), slow)
	start := time.Now()
	_, failed, err = NewExecutor(pool, Options{ShardTimeout: 150 * time.Millisecond}).
		QueryPartial(context.Background(), planFor("s0", "s1"))
	if err == nil && len(failed) != 2 {
		t.Errorf("failed = %v, want both shards (one failed, one timed out)", failed)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Error("the slow shard was cancelled by the other shard's failure")
	}
}

func TestQueryPartialFailsWhenNobodyAnswers(t *testing.T) {
	boom := &fakeShard{query: func(context.Context) (int, error) { return 0, errors.New("down") }}
	pool := newPool(t, boom, boom)
	rows, failed, err := NewExecutor(pool, Options{}).QueryPartial(context.Background(), planFor("s0", "s1"))
	var se *ShardError
	if !errors.As(err, &se) || rows != nil || failed != nil {
		t.Errorf("rows=%v failed=%v err=%v, want only a *ShardError", rows, failed, err)
	}
}

func TestQueryPartialCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newPool(t, &fakeShard{query: blockUntilDone}, &fakeShard{query: blockUntilDone})
	time.AfterFunc(50*time.Millisecond, cancel)
	_, _, err := NewExecutor(pool, Options{}).QueryPartial(ctx, planFor("s0", "s1"))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// hookLog records the shard events it hears.
type hookLog struct {
	observe.Nop
	mu     sync.Mutex
	starts []observe.ShardStartEvent
	dones  []observe.ShardDoneEvent
}

type hookCtxKey struct{}

func (h *hookLog) OnShardStart(ctx context.Context, e observe.ShardStartEvent) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.starts = append(h.starts, e)
	return context.WithValue(ctx, hookCtxKey{}, e.Shard)
}

func (h *hookLog) OnShardDone(ctx context.Context, e observe.ShardDoneEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ctx.Value(hookCtxKey{}) != e.Shard {
		panic("OnShardDone did not get the context OnShardStart returned")
	}
	h.dones = append(h.dones, e)
}

func TestShardHooksForQueryAndExec(t *testing.T) {
	boom := errors.New("boom")
	pool := newPool(t,
		&fakeShard{exec: func(context.Context) (int64, error) { return 4, nil }},
		&fakeShard{exec: func(context.Context) (int64, error) { return 0, boom }},
	)
	h := &hookLog{}
	ex := NewExecutor(pool, Options{Hooks: h})

	got, err := ex.Query(context.Background(), planFor("s0", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range got {
		_ = r.Rows.Close()
	}
	if len(h.starts) != 2 || len(h.dones) != 2 || h.dones[0].Kind != observe.Query || h.dones[0].Err != nil {
		t.Fatalf("query events: %+v %+v", h.starts, h.dones)
	}

	h.starts, h.dones = nil, nil
	if _, err := ex.Exec(context.Background(), planFor("s0", "s1")); err != nil {
		t.Fatal(err)
	}
	byShard := map[router.ShardID]observe.ShardDoneEvent{}
	for _, d := range h.dones {
		byShard[d.Shard] = d
	}
	switch {
	case len(h.starts) != 2:
		t.Errorf("starts = %+v", h.starts)
	case byShard["s0"].Kind != observe.Exec || byShard["s0"].RowsAffected != 4 || byShard["s0"].Err != nil:
		t.Errorf("s0 = %+v", byShard["s0"])
	case !errors.Is(byShard["s1"].Err, boom) || byShard["s1"].RowsAffected != 0:
		t.Errorf("s1 = %+v", byShard["s1"])
	default:
		// as expected
	}
}

func TestShardHooksForATransaction(t *testing.T) {
	pool := newPool(t, &fakeShard{exec: func(context.Context) (int64, error) { return 2, nil }})
	h := &hookLog{}
	tx, err := NewExecutor(pool, Options{Hooks: h}).Begin(context.Background(), "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.Query(context.Background(), "SELECT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if n, err := tx.Exec(context.Background(), "UPDATE t SET a = 1", nil); err != nil || n != 2 {
		t.Fatalf("Exec = %d, %v", n, err)
	}
	if len(h.dones) != 2 || h.dones[0].Kind != observe.Query || h.dones[1].Kind != observe.Exec || h.dones[1].RowsAffected != 2 {
		t.Errorf("events = %+v", h.dones)
	}
}

// The context a hook returns is the one the database call gets, so anything
// instrumenting the driver (or a trace) sees the shard's span.
func TestShardWorkRunsWithTheHookContext(t *testing.T) {
	var queryGot, execGot atomic.Value
	seen := func(into *atomic.Value) func(context.Context) {
		return func(ctx context.Context) { into.Store(ctx.Value(hookCtxKey{}) == router.ShardID("s0")) }
	}
	q, e := seen(&queryGot), seen(&execGot)
	pool := newPool(t, &fakeShard{
		query: func(ctx context.Context) (int, error) { q(ctx); return 1, nil },
		exec:  func(ctx context.Context) (int64, error) { e(ctx); return 1, nil },
	})
	ex := NewExecutor(pool, Options{Hooks: &hookLog{}})

	rows, err := ex.Query(context.Background(), planFor("s0"))
	if err != nil {
		t.Fatal(err)
	}
	_ = rows[0].Rows.Close()
	if _, err := ex.Exec(context.Background(), planFor("s0")); err != nil {
		t.Fatal(err)
	}

	for name, v := range map[string]*atomic.Value{"Query": &queryGot, "Exec": &execGot} {
		if got, _ := v.Load().(bool); !got {
			t.Errorf("%s: the database call did not get the context OnShardStart returned", name)
		}
	}
}
