package shard

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/router"
)

// routingDB is a DB with a real router but no live shards, enough to test
// routing decisions: they are made before anything is executed.
func routingDB(t *testing.T, ids ...ShardID) *DB {
	t.Helper()
	r, err := router.New(router.Even(ids...)...)
	if err != nil {
		t.Fatal(err)
	}
	conns := map[ShardID]exec.Conn{}
	for _, id := range ids {
		conns[id] = nil // never used: plan() only checks membership
	}
	return &DB{registry: testRegistry(t), router: r, pool: exec.NewPool(conns)}
}

func TestPlanWithShardKey(t *testing.T) {
	db := routingDB(t, "a", "b", "c")
	for _, key := range []any{1, 42, "profile-42", "123e4567-e89b-12d3-a456-426614174000"} {
		want, _ := db.router.ShardFor(key)
		p, err := db.WithShardKey(key).(*scoped).plan("SELECT 1", nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Targets) != 1 || p.Targets[0] != want || p.Strategy != plan.Single {
			t.Errorf("key %v: plan = %+v, want single target %s", key, p, want)
		}
		if !strings.Contains(p.Reason, "WithShardKey") || !strings.Contains(p.Reason, string(want)) {
			t.Errorf("key %v: reason %q should name the override and the shard", key, p.Reason)
		}
	}
}

func TestPlanWithShardKeyRejectsUnroutableKeys(t *testing.T) {
	db := routingDB(t, "a", "b")
	for _, tc := range []struct {
		key  any
		want error
	}{{nil, router.ErrNilKey}, {1.5, router.ErrUnsupportedKey}} {
		_, err := db.WithShardKey(tc.key).(*scoped).plan("SELECT 1", nil)
		if !errors.Is(err, tc.want) {
			t.Errorf("key %v: error = %v, want %v", tc.key, err, tc.want)
		}
	}
}

func TestPlanWithShard(t *testing.T) {
	db := routingDB(t, "a", "b")
	p, err := db.WithShard("b").(*scoped).plan("SELECT 1", nil)
	if err != nil || len(p.Targets) != 1 || p.Targets[0] != "b" || p.Strategy != plan.Single {
		t.Fatalf("plan = %+v, %v", p, err)
	}
	_, err = db.WithShard("zzz").(*scoped).plan("SELECT 1", nil)
	if !errors.Is(err, ErrUnknownShard) || !strings.Contains(err.Error(), "[a b]") {
		t.Errorf("error = %v, want ErrUnknownShard listing the configured shards", err)
	}
}

func TestPlanWithAllShards(t *testing.T) {
	db := routingDB(t, "c", "a", "b")
	p, err := db.WithAllShards().(*scoped).plan("SELECT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Targets) != 3 || p.Targets[0] != "a" || p.Targets[2] != "c" || p.Strategy != plan.All {
		t.Errorf("plan = %+v, want all three shards, sorted", p)
	}
}

func TestPlanKeepsSQLAndArgs(t *testing.T) {
	db := routingDB(t, "a")
	p, _ := db.WithShard("a").(*scoped).plan("SELECT $1", []any{7})
	if p.SQL != "SELECT $1" || len(p.Args) != 1 || p.Args[0] != 7 {
		t.Errorf("plan = %+v", p)
	}
}

func TestUnroutedStatementsAreRefused(t *testing.T) {
	db := routingDB(t, "a", "b")
	_, qerr := db.Query(context.Background(), "SELECT 1")
	_, eerr := db.Exec(context.Background(), "DELETE FROM profiles")
	for _, err := range []error{qerr, eerr} {
		if !errors.Is(err, ErrShardKeyRequired) {
			t.Errorf("error = %v, want ErrShardKeyRequired", err)
		}
		if err != nil && !strings.Contains(err.Error(), "WithShardKey") {
			t.Errorf("error should say how to fix it: %v", err)
		}
	}
}

func TestQueryOnSeveralShardsIsNotSupportedYet(t *testing.T) {
	db := routingDB(t, "a", "b")
	_, err := db.WithAllShards().Query(context.Background(), "SELECT 1")
	if !errors.Is(err, ErrUnsupportedQuery) {
		t.Errorf("error = %v, want ErrUnsupportedQuery", err)
	}
}

func TestWriteResultRowsAffected(t *testing.T) {
	r := WriteResult{PerShard: map[ShardID]ShardOutcome{
		"a": {RowsAffected: 3},
		"b": {RowsAffected: 4},
		"c": {RowsAffected: 99, Err: errors.New("failed")}, // not counted
	}}
	if got := r.RowsAffected(); got != 7 {
		t.Errorf("RowsAffected() = %d, want 7 (failed shards are not counted)", got)
	}
}
