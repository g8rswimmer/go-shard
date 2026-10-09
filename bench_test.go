package shard

import (
	"context"
	"testing"

	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/query"
)

// These benchmarks measure what the library adds to a call, with no network or
// database in the way: the shards are in-process fakes that answer instantly,
// so the numbers are the routing, planning and merging themselves. The
// baseline is the same call made straight on database/sql. See
// docs/BENCHMARKS.md, and bench_integration_test.go for real PostgreSQL.

func benchDB(b *testing.B, rows int, hooks observe.Hooks) *DB {
	b.Helper()
	// fakeDB wants a *testing.T only for Fatal and Cleanup, which *testing.B has too
	cfg := Config{Registry: explainRegistry(b), Hooks: hooks}
	return fakeDBFor(b, cfg, &fakeShard{rows: rows}, &fakeShard{rows: rows}, &fakeShard{rows: rows})
}

func BenchmarkDirectDatabaseSQL(b *testing.B) {
	db := (&fakeShard{rows: 1}).db()
	defer db.Close()
	ctx := context.Background()
	for b.Loop() {
		rows, err := db.QueryContext(ctx, "SELECT name FROM profiles WHERE id = $1", 42)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		_ = rows.Close()
	}
}

// A built statement is routed from the key it carries: hashing the key, finding
// the shard, planning and running.
func BenchmarkSingleShardBuilt(b *testing.B) {
	db := benchDB(b, 1, nil)
	st, err := query.From("profiles").Where(query.Eq("id", 42)).Build()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.QueryStatement(ctx, st)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		_ = rows.Close()
	}
}

// Building the statement is part of using the builder.
func BenchmarkSingleShardBuildAndRun(b *testing.B) {
	db := benchDB(b, 1, nil)
	ctx := context.Background()
	for b.Loop() {
		st, err := query.From("profiles").Where(query.Eq("id", 42)).Build()
		if err != nil {
			b.Fatal(err)
		}
		rows, err := db.QueryStatement(ctx, st)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		_ = rows.Close()
	}
}

// The shard is named by the caller, so there is no key to find: what remains is
// the library's own fixed cost.
func BenchmarkSingleShardWithShardKey(b *testing.B) {
	db := benchDB(b, 1, nil)
	ctx := context.Background()
	for b.Loop() {
		rows, err := db.WithShardKey(42).Query(ctx, "SELECT name FROM profiles WHERE id = $1", 42)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		_ = rows.Close()
	}
}

func BenchmarkSingleShardWithHooks(b *testing.B) {
	db := benchDB(b, 1, observe.Nop{})
	ctx := context.Background()
	for b.Loop() {
		rows, err := db.WithShardKey(42).Query(ctx, "SELECT name FROM profiles WHERE id = $1", 42)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		_ = rows.Close()
	}
}

// Routing alone, without running anything.
func BenchmarkPlanOnly(b *testing.B) {
	db := benchDB(b, 1, nil)
	st, err := query.From("profiles").Where(query.Eq("id", 42)).Build()
	if err != nil {
		b.Fatal(err)
	}
	s := &scoped{db: db}
	r := statementRequest(st)
	for b.Loop() {
		if _, err := s.plan(r); err != nil {
			b.Fatal(err)
		}
	}
}

// A query on all three shards: three calls in parallel plus the merge.
func BenchmarkFanOutOrdered100RowsPerShard(b *testing.B) {
	db := benchDB(b, 100, nil)
	st, err := query.From("profiles").OrderBy("id", query.Asc).Build()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		rows, err := db.WithAllShards().QueryStatement(ctx, st)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		_ = rows.Close()
		if n != 300 {
			b.Fatalf("read %d rows", n)
		}
	}
}

func BenchmarkWriteSingleShard(b *testing.B) {
	db := benchDB(b, 1, nil)
	st, err := query.Update("profiles").Set("name", "x").Where(query.Eq("id", 42)).Build()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		if _, err := db.ExecStatement(ctx, st); err != nil {
			b.Fatal(err)
		}
	}
}
