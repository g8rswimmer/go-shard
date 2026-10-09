//go:build integration

package shard_test

import (
	"context"
	"testing"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/shardtest"
)

// Against real PostgreSQL the library's own cost is small next to the round
// trip. Run with: make bench-integration (see docs/BENCHMARKS.md).

func benchCluster(b *testing.B) (*shardtest.Cluster, *shard.DB) {
	b.Helper()
	cluster := shardtest.NewCluster(b, 3)
	cluster.ExecAll(b, "CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL)")
	cluster.ExecAll(b, "CREATE TABLE addresses (id bigint PRIMARY KEY, profile_id bigint NOT NULL, city text)")
	cluster.ExecAll(b, "CREATE TABLE countries (code text PRIMARY KEY)")
	db := cluster.Open(b, txRegistry(b))
	rows := make([]map[string]any, 3000)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "name": "profile"}
	}
	cluster.Seed(b, db, "profiles", rows...)
	return cluster, db
}

func readAll(b *testing.B, rows shard.Rows, err error) int {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		b.Fatal(err)
	}
	_ = rows.Close()
	return n
}

// The same lookup made straight on the owning shard, bypassing the library.
func BenchmarkRealDirectToTheShard(b *testing.B) {
	cluster, db := benchCluster(b)
	owner := shardOf(b, db, 42)
	direct := cluster.Direct(b, owner)
	b.ResetTimer()
	for b.Loop() {
		rows, err := direct.Query("SELECT name FROM profiles WHERE id = $1", 42)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		_ = rows.Close()
	}
}

func BenchmarkRealSingleShardBuilt(b *testing.B) {
	_, db := benchCluster(b)
	st, err := query.From("profiles").Columns("name").Where(query.Eq("id", 42)).Build()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.QueryStatement(ctx, st)
		readAll(b, rows, err)
	}
}

func BenchmarkRealSingleShardRawSQL(b *testing.B) {
	_, db := benchCluster(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.Query(ctx, "SELECT name FROM profiles WHERE id = $1", 42)
		readAll(b, rows, err)
	}
}

func BenchmarkRealFanOutOrderedLimit(b *testing.B) {
	_, db := benchCluster(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.WithAllShards().Query(ctx, "SELECT id, name FROM profiles ORDER BY id LIMIT 20")
		if n := readAll(b, rows, err); n != 20 {
			b.Fatalf("read %d rows", n)
		}
	}
}

func BenchmarkRealFanOutAggregate(b *testing.B) {
	_, db := benchCluster(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.WithAllShards().Query(ctx, "SELECT name, count(*) FROM profiles GROUP BY name")
		readAll(b, rows, err)
	}
}

func shardOf(b *testing.B, db *shard.DB, key any) shard.ShardID {
	b.Helper()
	e, err := db.WithShardKey(key).Explain(context.Background(), "SELECT 1")
	if err != nil || len(e.Targets) != 1 {
		b.Fatalf("explain: %+v, %v", e, err)
	}
	return e.Targets[0]
}
