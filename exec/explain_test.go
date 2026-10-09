package exec

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/router"
)

func TestExplainShardsAsksEveryTarget(t *testing.T) {
	a := &fakeShard{query: func(context.Context) (int, error) { return 2, nil }}
	b := &fakeShard{query: func(context.Context) (int, error) { return 1, nil }}
	pool := newPool(t, a, b)

	p := plan.Plan{
		SQL: "SELECT 1", Targets: []router.ShardID{"s0", "s1"}, Strategy: plan.Multi,
		PerShard: map[router.ShardID]plan.ShardStatement{"s1": {SQL: "SELECT 2"}},
	}
	got, err := NewExecutor(pool, Options{}).ExplainShards(context.Background(), p, false)
	if err != nil {
		t.Fatal(err)
	}
	if got["s0"] != "0\n1" || got["s1"] != "0" {
		t.Errorf("plans = %q", got)
	}
	// Each shard is asked about its own statement, and nothing is run.
	if q := a.queries(); !slices.Equal(q, []string{"EXPLAIN SELECT 1"}) {
		t.Errorf("s0 saw %q", q)
	}
	if q := b.queries(); !slices.Equal(q, []string{"EXPLAIN SELECT 2"}) {
		t.Errorf("s1 saw %q", q)
	}
}

// ANALYZE runs the statement, so it must be undone.
func TestExplainShardsAnalyzeRollsBack(t *testing.T) {
	a := &fakeShard{}
	pool := newPool(t, a)
	p := plan.Plan{SQL: "DELETE FROM t", Targets: []router.ShardID{"s0"}, Strategy: plan.Single}

	if _, err := NewExecutor(pool, Options{}).ExplainShards(context.Background(), p, true); err != nil {
		t.Fatal(err)
	}
	if q := a.queries(); !slices.Equal(q, []string{"BEGIN", "EXPLAIN (ANALYZE) DELETE FROM t", "ROLLBACK"}) {
		t.Errorf("s0 saw %q, want the statement run inside a transaction that is rolled back", q)
	}
}

func TestExplainShardsFailure(t *testing.T) {
	boom := errors.New("boom")
	pool := newPool(t, &fakeShard{}, &fakeShard{query: func(context.Context) (int, error) { return 0, boom }})
	_, err := NewExecutor(pool, Options{}).ExplainShards(context.Background(), planFor("s0", "s1"), false)
	var se *ShardError
	if !errors.As(err, &se) || se.Shard != "s1" || !errors.Is(err, boom) {
		t.Errorf("error = %v, want a ShardError for s1 wrapping boom", err)
	}

	pool = newPool(t, &fakeShard{begin: boom})
	_, err = NewExecutor(pool, Options{}).ExplainShards(context.Background(), planFor("s0"), true)
	if !errors.As(err, &se) || !strings.Contains(err.Error(), "boom") {
		t.Errorf("analyze with a failing Begin: error = %v", err)
	}

	if _, err := NewExecutor(pool, Options{}).ExplainShards(context.Background(), planFor("nope"), false); !errors.Is(err, ErrUnknownShard) {
		t.Errorf("unknown shard: error = %v", err)
	}
}

func TestTxExplainStaysInTheTransaction(t *testing.T) {
	a := &fakeShard{}
	pool := newPool(t, a)
	e := NewExecutor(pool, Options{})
	tx, err := e.Begin(context.Background(), "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Explain(context.Background(), "SELECT 1", nil); err != nil {
		t.Fatal(err)
	}
	if q := a.queries(); !slices.Equal(q, []string{"BEGIN", "EXPLAIN SELECT 1"}) {
		t.Errorf("saw %q", q)
	}
}
