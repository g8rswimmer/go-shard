//go:build integration

package shard_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
	"github.com/g8rswimmer/go-shard/shardtest"
)

func demoRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id")),
		registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
		registry.Global("countries"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// The tests share one 3-shard cluster, because starting containers dominates
// the run time. Each subtest uses its own tables.
func TestIntegration(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	db := cluster.Open(t, demoRegistry(t))

	// An independent router with the same layout as the one Open builds, so
	// the expected shard does not come from the code under test.
	expected, err := router.New(router.Even(cluster.IDs()...)...)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("a row lands on the shard its key routes to", func(t *testing.T) {
		cluster.ExecAll(t, "CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL)")

		const n = 200
		perShard := map[shard.ShardID]int{}
		for id := int64(1); id <= n; id++ {
			res, err := db.WithShardKey(id).Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, $2)", id, fmt.Sprintf("profile-%d", id))
			if err != nil {
				t.Fatalf("insert %d: %v", id, err)
			}
			want, _ := expected.ShardFor(id)
			if len(res.PerShard) != 1 || res.PerShard[want].RowsAffected != 1 {
				t.Fatalf("insert %d: result %+v, want one row on %s", id, res.PerShard, want)
			}
			perShard[want]++
		}

		// Physically: each shard holds exactly the rows routed to it.
		total := int64(0)
		for _, sid := range cluster.IDs() {
			got := cluster.Count(t, sid, "SELECT count(*) FROM profiles")
			if got != int64(perShard[sid]) {
				t.Errorf("%s holds %d rows, want %d", sid, got, perShard[sid])
			}
			if got == 0 {
				t.Errorf("%s holds no rows: keys are not spreading across shards", sid)
			}
			total += got
		}
		if total != n {
			t.Errorf("shards hold %d rows in total, want %d (a row is missing or duplicated)", total, n)
		}

		// And each row can be read back through the same routing.
		for _, id := range []int64{1, 2, 3, 77, 200} {
			rows, err := db.WithShardKey(id).Query(ctx, "SELECT name FROM profiles WHERE id = $1", id)
			if err != nil {
				t.Fatal(err)
			}
			var name string
			if !rows.Next() {
				t.Fatalf("id %d: not found on its shard (err %v)", id, rows.Err())
			}
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if want := fmt.Sprintf("profile-%d", id); name != want {
				t.Errorf("id %d: name %q, want %q", id, name, want)
			}
		}
	})

	t.Run("reading on the wrong shard finds nothing", func(t *testing.T) {
		cluster.ExecAll(t, "CREATE TABLE wrong_shard (id bigint PRIMARY KEY)")
		const id = int64(5)
		if _, err := db.WithShardKey(id).Exec(ctx, "INSERT INTO wrong_shard (id) VALUES ($1)", id); err != nil {
			t.Fatal(err)
		}
		owner, _ := expected.ShardFor(id)
		for _, sid := range cluster.IDs() {
			want := int64(0)
			if sid == owner {
				want = 1
			}
			if got := cluster.Count(t, sid, "SELECT count(*) FROM wrong_shard"); got != want {
				t.Errorf("%s holds %d rows, want %d", sid, got, want)
			}
		}
	})

	t.Run("seed places sharded, colocated and global rows", func(t *testing.T) {
		cluster.ExecAll(t, `CREATE TABLE addresses (id bigint PRIMARY KEY, profile_id bigint NOT NULL, city text)`)
		cluster.ExecAll(t, `CREATE TABLE countries (code text PRIMARY KEY)`)

		const profiles = 30
		for id := int64(1000); id < 1000+profiles; id++ {
			cluster.Seed(t, db, "profiles", map[string]any{"id": id, "name": "seeded"})
			cluster.Seed(t, db, "addresses",
				map[string]any{"id": id * 10, "profile_id": id, "city": "Austin"},
				map[string]any{"id": id*10 + 1, "profile_id": id, "city": "Boston"})
		}
		cluster.Seed(t, db, "countries", map[string]any{"code": "US"}, map[string]any{"code": "CA"})

		for _, sid := range cluster.IDs() {
			// Colocation: every address on this shard belongs to a profile on this shard.
			orphans := cluster.Count(t, sid, `SELECT count(*) FROM addresses a
				WHERE NOT EXISTS (SELECT 1 FROM profiles p WHERE p.id = a.profile_id)`)
			if orphans != 0 {
				t.Errorf("%s: %d addresses are on a different shard from their profile", sid, orphans)
			}
			if got := cluster.Count(t, sid, "SELECT count(*) FROM countries"); got != 2 {
				t.Errorf("%s: %d countries, want 2 (global tables live on every shard)", sid, got)
			}
		}
		var addresses int64
		for _, sid := range cluster.IDs() {
			addresses += cluster.Count(t, sid, "SELECT count(*) FROM addresses")
		}
		if addresses != 2*profiles {
			t.Errorf("%d addresses in total, want %d", addresses, 2*profiles)
		}
	})

	t.Run("a write to all shards reports each shard", func(t *testing.T) {
		// The table exists on shard-01 only, so the write succeeds there and
		// fails elsewhere.
		first := cluster.IDs()[0]
		if _, err := cluster.Direct(t, first).ExecContext(ctx, "CREATE TABLE partial (id int)"); err != nil {
			t.Fatal(err)
		}
		res, err := db.WithAllShards().Exec(ctx, "INSERT INTO partial (id) VALUES (1)")
		if err == nil {
			t.Fatal("expected an error: the table is missing on two shards")
		}
		var se *shard.ShardError
		if !errors.As(err, &se) {
			t.Errorf("error = %v, want it to carry a *ShardError", err)
		}
		if len(res.PerShard) != 3 {
			t.Fatalf("result covers %d shards, want 3", len(res.PerShard))
		}
		for sid, o := range res.PerShard {
			switch {
			case sid == first && (o.Err != nil || o.RowsAffected != 1):
				t.Errorf("%s: outcome %+v, want success with 1 row", sid, o)
			case sid != first && o.Err == nil:
				t.Errorf("%s: want a failure, got success", sid)
			default:
				// expected outcome
			}
		}
		if got := res.RowsAffected(); got != 1 {
			t.Errorf("RowsAffected = %d, want 1 (failed shards are not counted)", got)
		}
		if got := cluster.Count(t, first, "SELECT count(*) FROM partial"); got != 1 {
			t.Errorf("the successful shard rolled back: %d rows", got)
		}
	})

	t.Run("querying without routing is refused", func(t *testing.T) {
		if _, err := db.Query(ctx, "SELECT 1"); !errors.Is(err, shard.ErrShardKeyRequired) {
			t.Errorf("error = %v, want ErrShardKeyRequired", err)
		}
		if _, err := db.WithAllShards().Query(ctx, "SELECT 1"); !errors.Is(err, shard.ErrUnsupportedQuery) {
			t.Errorf("error = %v, want ErrUnsupportedQuery (merging arrives in a later milestone)", err)
		}
		if _, err := db.WithShard("nope").Query(ctx, "SELECT 1"); !errors.Is(err, shard.ErrUnknownShard) {
			t.Errorf("error = %v, want ErrUnknownShard", err)
		}
	})

	t.Run("WithShard targets a named shard", func(t *testing.T) {
		rows, err := db.WithShard(cluster.IDs()[1]).Query(ctx, "SELECT current_setting('server_version_num')::int")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var v int
		if !rows.Next() || rows.Scan(&v) != nil || v < 140000 {
			t.Errorf("server_version_num = %d (err %v), want a PostgreSQL 14+ answer", v, rows.Err())
		}
	})

	t.Run("health reports every shard", func(t *testing.T) {
		h := db.Health(ctx)
		if len(h) != 3 {
			t.Fatalf("health has %d entries, want 3", len(h))
		}
		for i, sh := range h {
			if sh.ID != cluster.IDs()[i] || !sh.Healthy() || sh.Latency <= 0 {
				t.Errorf("health[%d] = %+v, want healthy %s with a latency", i, sh, cluster.IDs()[i])
			}
		}
	})

	t.Run("a slow shard hits the shard timeout", func(t *testing.T) {
		cfg := cluster.Config(demoRegistry(t))
		cfg.ShardTimeout = 200 * time.Millisecond
		slow, err := shard.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer slow.Close()

		start := time.Now()
		_, err = slow.WithShard(cluster.IDs()[0]).Query(ctx, "SELECT pg_sleep(10)")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want DeadlineExceeded", err)
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Errorf("took %v: the query was not cut off", d)
		}
	})
}

func TestOpenFailsWhenOneShardIsDown(t *testing.T) {
	cluster := shardtest.NewCluster(t, 2)
	cfg := cluster.Config(demoRegistry(t))
	cfg.Shards[1].DSN = "postgres://shard:shard@127.0.0.1:1/shard?sslmode=disable&connect_timeout=2"

	_, err := shard.Open(context.Background(), cfg)
	var se *shard.ShardError
	if !errors.As(err, &se) || se.Shard != cfg.Shards[1].ID {
		t.Fatalf("error = %v, want a ShardError naming %s", err, cfg.Shards[1].ID)
	}
}

func TestHealthAfterClose(t *testing.T) {
	cluster := shardtest.NewCluster(t, 2)
	db := cluster.Open(t, demoRegistry(t))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, h := range db.Health(context.Background()) {
		if h.Healthy() {
			t.Errorf("%s reports healthy after Close", h.ID)
		}
	}
}
