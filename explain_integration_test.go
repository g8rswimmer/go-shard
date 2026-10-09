//go:build integration

package shard_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/shardtest"
)

func TestExplainAgainstShards(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, "CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL)")
	cluster.ExecAll(t, "CREATE TABLE addresses (id bigint PRIMARY KEY, profile_id bigint NOT NULL, city text)")
	cluster.ExecAll(t, "CREATE TABLE countries (code text PRIMARY KEY)")
	db := cluster.Open(t, txRegistry(t))

	count := func(table string) int64 {
		var n int64
		for _, id := range cluster.IDs() {
			n += cluster.Count(t, id, "SELECT count(*) FROM "+table)
		}
		return n
	}

	t.Run("plans come from the shards that would run the query", func(t *testing.T) {
		e, err := db.Explainer(shard.ShardPlans()).Explain(ctx, "SELECT name FROM profiles WHERE id = $1", 42)
		if err != nil {
			t.Fatal(err)
		}
		if len(e.Targets) != 1 || len(e.ShardPlans) != 1 {
			t.Fatalf("targets %v, plans %v: want one of each", e.Targets, e.ShardPlans)
		}
		plan := e.ShardPlans[e.Targets[0]]
		if !strings.Contains(plan, "profiles") || strings.Contains(plan, "actual time") {
			t.Errorf("plan = %q, want a plan of profiles without timings", plan)
		}
	})

	t.Run("a fan-out is explained on every shard with the rewritten SQL", func(t *testing.T) {
		e, err := db.WithAllShards().Explainer(shard.ShardPlans()).Explain(ctx,
			"SELECT name FROM profiles ORDER BY created_at DESC LIMIT 5")
		if err == nil {
			t.Fatalf("created_at does not exist, so the shards should refuse: %+v", e)
		}
		var se *shard.ShardError
		if !errors.As(err, &se) {
			t.Errorf("error = %v, want a ShardError", err)
		}

		e, err = db.WithAllShards().Explainer(shard.ShardPlans()).Explain(ctx, "SELECT name FROM profiles ORDER BY name DESC LIMIT 5")
		if err != nil {
			t.Fatal(err)
		}
		if len(e.ShardPlans) != 3 || !strings.Contains(e.ShardSQL, "LIMIT 5") {
			t.Errorf("plans %v, shard SQL %q", e.ShardPlans, e.ShardSQL)
		}
		if !strings.Contains(e.String(), "shard plans:") {
			t.Errorf("String() should include the plans:\n%s", e)
		}
	})

	t.Run("explain does not run the statement", func(t *testing.T) {
		before := count("profiles")
		if _, err := db.WithAllShards().Explainer(shard.ShardPlans()).Explain(ctx, "INSERT INTO profiles (id, name) VALUES (1, 'x')"); err != nil {
			t.Fatal(err)
		}
		if after := count("profiles"); after != before {
			t.Errorf("rows %d -> %d: EXPLAIN must not write", before, after)
		}
	})

	t.Run("analyze runs a write and rolls it back", func(t *testing.T) {
		e, err := db.WithAllShards().Explainer(shard.Analyze()).Explain(ctx, "INSERT INTO profiles (id, name) VALUES (1, 'x')")
		if err != nil {
			t.Fatal(err)
		}
		for id, plan := range e.ShardPlans {
			if !strings.Contains(plan, "actual time") {
				t.Errorf("%s: plan %q should have timings", id, plan)
			}
		}
		if n := count("profiles"); n != 0 {
			t.Errorf("%d rows left behind by EXPLAIN ANALYZE", n)
		}
	})

	t.Run("a built statement with parameters", func(t *testing.T) {
		st, err := query.From("profiles").Where(query.In("id", 1, 2, 3, 4, 5, 6)).OrderBy("name", query.Asc).Limit(3).Build()
		if err != nil {
			t.Fatal(err)
		}
		e, err := db.Explainer(shard.ShardPlans()).ExplainStatement(ctx, st)
		if err != nil {
			t.Fatal(err)
		}
		if len(e.ShardPlans) != len(e.Targets) {
			t.Errorf("plans for %d of %d targets", len(e.ShardPlans), len(e.Targets))
		}
	})

	t.Run("routing errors are the ones running would give", func(t *testing.T) {
		if _, err := db.Explain(ctx, "SELECT * FROM profiles WHERE name = 'x'"); !errors.Is(err, shard.ErrShardKeyRequired) {
			t.Errorf("error = %v, want ErrShardKeyRequired", err)
		}
	})

	t.Run("inside a transaction", func(t *testing.T) {
		tx, err := db.Begin(ctx, shard.ForTable("profiles", 42))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		e, err := tx.Explainer(shard.ShardPlans()).Explain(ctx, "SELECT name FROM profiles WHERE id = $1", 42)
		if err != nil {
			t.Fatal(err)
		}
		if len(e.ShardPlans) != 1 || e.ShardPlans[tx.Shard()] == "" {
			t.Errorf("plans = %v, want the transaction's shard", e.ShardPlans)
		}
		if _, err := tx.Explainer(shard.Analyze()).Explain(ctx, "SELECT 1 FROM profiles WHERE id = $1", 42); !errors.Is(err, shard.ErrUnsupportedQuery) {
			t.Errorf("analyze in a transaction: error = %v", err)
		}
	})
}
