package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	slow := &fakeShard{query: blockUntilDone}
	bad := &fakeShard{query: func(context.Context) (int, error) { return 0, errors.New("boom") }}
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
	if out[0].Shard != "s0" || out[0].Err == nil || out[0].Result != nil {
		t.Errorf("s0 outcome = %+v, want the failure", out[0])
	}
	if n, _ := out[1].Result.RowsAffected(); out[1].Shard != "s1" || out[1].Err != nil || n != 7 {
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
