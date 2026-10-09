//go:build integration

package shard_test

import (
	"context"
	"sync"
	"testing"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/shardtest"
)

type countingHooks struct {
	observe.Nop
	mu     sync.Mutex
	plans  []observe.PlanEvent
	shards []observe.ShardDoneEvent
	merges []observe.MergeEvent
	done   []observe.DoneEvent
}

func (c *countingHooks) OnPlan(ctx context.Context, e observe.PlanEvent) context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.plans = append(c.plans, e)
	return ctx
}

func (c *countingHooks) OnShardDone(_ context.Context, e observe.ShardDoneEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shards = append(c.shards, e)
}

func (c *countingHooks) OnMerge(_ context.Context, e observe.MergeEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.merges = append(c.merges, e)
}

func (c *countingHooks) OnDone(_ context.Context, e observe.DoneEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.done = append(c.done, e)
}

func TestHooksAndHealthAgainstShards(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, "CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL)")
	cluster.ExecAll(t, "CREATE TABLE addresses (id bigint PRIMARY KEY, profile_id bigint NOT NULL, city text)")
	cluster.ExecAll(t, "CREATE TABLE countries (code text PRIMARY KEY)")

	hooks := &countingHooks{}
	cfg := cluster.Config(txRegistry(t))
	cfg.Hooks = hooks
	db, err := shard.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for id := 1; id <= 6; id++ {
		if _, err := db.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, $2)", id, "n"); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.WithAllShards().Query(ctx, "SELECT name FROM profiles ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	_ = rows.Close()
	if n != 6 {
		t.Fatalf("read %d rows", n)
	}

	// 6 single-shard inserts, then one query on all three shards
	if len(hooks.plans) != 7 || len(hooks.done) != 7 || len(hooks.shards) != 6+3 || len(hooks.merges) != 1 {
		t.Fatalf("%d plans, %d dones, %d shard events, %d merges", len(hooks.plans), len(hooks.done), len(hooks.shards), len(hooks.merges))
	}
	if p := hooks.plans[6]; p.Strategy != "all" || len(p.Targets) != 3 || p.Kind != observe.Query {
		t.Errorf("fan-out plan = %+v", p)
	}
	for _, s := range hooks.shards {
		if s.Err != nil || s.Duration <= 0 {
			t.Errorf("shard event = %+v", s)
		}
	}

	// Health pings; PoolStats only reads. Both see the pools.
	for _, h := range db.Health(ctx) {
		if !h.Healthy() || h.Latency <= 0 {
			t.Errorf("health = %+v", h)
		}
	}
	stats := db.PoolStats()
	if len(stats) != 3 {
		t.Fatalf("stats for %d shards", len(stats))
	}
	for id, s := range stats {
		if s.OpenConnections < 1 || s.MaxOpenConnections < 1 {
			t.Errorf("%s: %+v", id, s)
		}
	}
}
