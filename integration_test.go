//go:build integration

package shard_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
	"github.com/g8rswimmer/go-shard/shardtest"
)

func demoRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
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
		if _, err := db.Query(ctx, "SELECT * FROM profiles"); !errors.Is(err, shard.ErrShardKeyRequired) {
			t.Errorf("error = %v, want ErrShardKeyRequired", err)
		}
		if _, err := db.WithAllShards().Query(ctx, "UPDATE profiles SET name = 'x' RETURNING id"); !errors.Is(err, shard.ErrUnsupportedQuery) {
			t.Errorf("error = %v, want ErrUnsupportedQuery (only a SELECT is merged across shards)", err)
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

	// ---- routing from the SQL itself -------------------------------------
	// These use data created above: profiles 1..200 and 1000..1029, two
	// addresses for each of 1000..1029, and countries US and CA.

	t.Run("auto: a read finds the row a keyed write put on a shard", func(t *testing.T) {
		// Write with an explicit key, read back with plain SQL and no hint. If the
		// SQL routing disagreed with the key routing anywhere, a row would be
		// missing.
		for id := int64(5000); id < 5100; id++ {
			if _, err := db.WithShardKey(id).Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, $2)", id, "auto"); err != nil {
				t.Fatal(err)
			}
			rows, err := db.Query(ctx, "SELECT name FROM profiles WHERE id = $1", id)
			if err != nil {
				t.Fatalf("id %d: %v", id, err)
			}
			found := rows.Next()
			_ = rows.Close()
			if !found {
				t.Fatalf("id %d: written by key but not found by an auto-routed read", id)
			}
		}
	})

	t.Run("auto: keys of different shapes route the same", func(t *testing.T) {
		const id = int64(5042)
		for name, q := range map[string]struct {
			sql  string
			args []any
		}{
			"param":          {"SELECT name FROM profiles WHERE id = $1", []any{id}},
			"literal":        {"SELECT name FROM profiles WHERE id = 5042", nil},
			"text param":     {"SELECT name FROM profiles WHERE id = $1", []any{"5042"}},
			"cast":           {"SELECT name FROM profiles WHERE id = $1::bigint", []any{id}},
			"with other AND": {"SELECT name FROM profiles WHERE name = 'auto' AND id = $1", []any{id}},
			"qualified":      {"SELECT p.name FROM profiles p WHERE p.id = $1", []any{id}},
			"IN one":         {"SELECT name FROM profiles WHERE id IN ($1)", []any{id}},
		} {
			rows, err := db.Query(ctx, q.sql, q.args...)
			if err != nil {
				t.Errorf("%s: %v", name, err)
				continue
			}
			if !rows.Next() {
				t.Errorf("%s: row not found (rows error: %v)", name, rows.Err())
			}
			_ = rows.Close()
		}
	})

	t.Run("auto: update and delete by key", func(t *testing.T) {
		const id = int64(5043)
		res, err := db.Exec(ctx, "UPDATE profiles SET name = $2 WHERE id = $1", id, "renamed")
		if err != nil || res.RowsAffected() != 1 || len(res.PerShard) != 1 {
			t.Fatalf("update: %+v, %v", res, err)
		}
		owner, _ := expected.ShardFor(id)
		if got := cluster.Count(t, owner, "SELECT count(*) FROM profiles WHERE id = $1 AND name = 'renamed'", id); got != 1 {
			t.Errorf("the update did not reach %s", owner)
		}
		res, err = db.Exec(ctx, "DELETE FROM profiles WHERE id = $1", id)
		if err != nil || res.RowsAffected() != 1 {
			t.Fatalf("delete: %+v, %v", res, err)
		}
		if got := cluster.Count(t, owner, "SELECT count(*) FROM profiles WHERE id = $1", id); got != 0 {
			t.Errorf("the row is still on %s", owner)
		}
	})

	t.Run("auto: an UPDATE with a key list reaches only the shards that own the keys", func(t *testing.T) {
		// Pick keys on exactly two different shards.
		var ids []any
		seen := map[shard.ShardID]bool{}
		for id := int64(5100); len(seen) < 2; id++ {
			if o, _ := expected.ShardFor(id); !seen[o] {
				seen[o] = true
				ids = append(ids, id)
				if _, err := db.WithShardKey(id).Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'x')", id); err != nil {
					t.Fatal(err)
				}
			}
		}
		res, err := db.Exec(ctx, "UPDATE profiles SET name = 'multi' WHERE id = ANY($1)", []int64{ids[0].(int64), ids[1].(int64)})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.PerShard) != 2 || res.RowsAffected() != 2 {
			t.Errorf("result %+v: want exactly two shards and two rows", res.PerShard)
		}
		for sid := range res.PerShard {
			if !seen[sid] {
				t.Errorf("shard %s should not have been touched", sid)
			}
		}
	})

	t.Run("auto: colocated tables join on one shard", func(t *testing.T) {
		const id = int64(1007)
		rows, err := db.Query(ctx, `SELECT a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id
			WHERE p.id = $1 ORDER BY a.city`, id)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var cities []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				t.Fatal(err)
			}
			cities = append(cities, c)
		}
		if fmt.Sprint(cities) != "[Austin Boston]" {
			t.Errorf("cities = %v, want both of the profile's addresses", cities)
		}
	})

	t.Run("auto: unsafe statements are refused and change nothing", func(t *testing.T) {
		before := int64(0)
		for _, sid := range cluster.IDs() {
			before += cluster.Count(t, sid, "SELECT count(*) FROM profiles")
		}
		for name, tc := range map[string]struct {
			sql  string
			args []any
			want error
		}{
			"delete without a key": {"DELETE FROM profiles", nil, shard.ErrShardKeyRequired},
			"update with an OR":    {"UPDATE profiles SET name = 'x' WHERE id = $1 OR id = $2", []any{1, 2}, shard.ErrShardKeyRequired},
			"join not on the key":  {"SELECT 1 FROM profiles p JOIN addresses a ON a.city = p.name WHERE p.id = $1", []any{1}, shard.ErrCrossShardJoin},
			"unregistered table":   {"SELECT * FROM mystery WHERE id = 1", nil, shard.ErrUnknownTable},
			"ddl":                  {"DROP TABLE profiles", nil, shard.ErrUnsupportedQuery},
			"missing argument":     {"SELECT * FROM profiles WHERE id = $2", []any{1}, shard.ErrMissingArgument},
		} {
			_, qerr := db.Query(ctx, tc.sql, tc.args...)
			_, eerr := db.Exec(ctx, tc.sql, tc.args...)
			if !errors.Is(qerr, tc.want) || !errors.Is(eerr, tc.want) {
				t.Errorf("%s: Query error %v, Exec error %v; want %v", name, qerr, eerr, tc.want)
			}
		}
		// A read of several shards is merged; one whose results cannot be merged is refused.
		if _, err := db.Query(ctx, "SELECT id, row_number() OVER (ORDER BY id) FROM profiles WHERE id IN (1, 2, 3, 4, 5, 6)"); !errors.Is(err, shard.ErrUnsupportedQuery) {
			t.Errorf("multi-shard window function: error = %v, want ErrUnsupportedQuery", err)
		}
		after := int64(0)
		for _, sid := range cluster.IDs() {
			after += cluster.Count(t, sid, "SELECT count(*) FROM profiles")
		}
		if before != after {
			t.Errorf("refused statements changed the data: %d rows before, %d after", before, after)
		}
	})

	t.Run("auto: global tables", func(t *testing.T) {
		// Reads can go to any shard; run enough to visit them all.
		for i := 0; i < 6; i++ {
			rows, err := db.Query(ctx, "SELECT code FROM countries ORDER BY code")
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for rows.Next() {
				n++
			}
			_ = rows.Close()
			if n != 2 {
				t.Errorf("read %d: %d countries, want 2 (every shard holds a copy)", i, n)
			}
		}
		// A write goes to every shard.
		res, err := db.Exec(ctx, "INSERT INTO countries (code) VALUES ('MX')")
		if err != nil || len(res.PerShard) != 3 || res.RowsAffected() != 3 {
			t.Fatalf("insert: %+v, %v", res, err)
		}
		for _, sid := range cluster.IDs() {
			if got := cluster.Count(t, sid, "SELECT count(*) FROM countries WHERE code = 'MX'"); got != 1 {
				t.Errorf("%s: MX present %d times, want 1", sid, got)
			}
		}
		if _, err := db.Exec(ctx, "DELETE FROM countries WHERE code = 'MX'"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("auto: SELECT without tables", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT 1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var n int
		if !rows.Next() || rows.Scan(&n) != nil || n != 1 {
			t.Errorf("SELECT 1 returned %d (err %v)", n, rows.Err())
		}
	})

	t.Run("built statements run without parsing", func(t *testing.T) {
		const id = int64(6044)
		if _, err := db.WithShardKey(id).Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'built')", id); err != nil {
			t.Fatal(err)
		}

		sel, err := query.From("profiles").Columns("name").Where(query.Eq("id", id)).Build()
		if err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryStatement(ctx, sel)
		if err != nil {
			t.Fatal(err)
		}
		var name string
		if !rows.Next() || rows.Scan(&name) != nil || name != "built" {
			t.Errorf("name = %q (err %v), want built", name, rows.Err())
		}
		_ = rows.Close()

		upd, _ := query.Update("profiles").Set("name", "built2").Where(query.Eq("id", id)).Build()
		if res, err := db.ExecStatement(ctx, upd); err != nil || res.RowsAffected() != 1 {
			t.Errorf("update: %+v, %v", res, err)
		}
		del, _ := query.DeleteFrom("profiles").Where(query.Eq("id", id)).Build()
		if res, err := db.ExecStatement(ctx, del); err != nil || res.RowsAffected() != 1 {
			t.Errorf("delete: %+v, %v", res, err)
		}

		// A built statement with no key is refused like raw SQL.
		all, _ := query.From("profiles").Build()
		if _, err := db.QueryStatement(ctx, all); !errors.Is(err, shard.ErrShardKeyRequired) {
			t.Errorf("error = %v, want ErrShardKeyRequired", err)
		}
	})

	t.Run("a custom analyzer can replace the parser", func(t *testing.T) {
		cfg := cluster.Config(demoRegistry(t))
		cfg.Analyzer = fixedAnalyzer{}
		custom, err := shard.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer custom.Close()
		// The analyzer says every statement is "profiles WHERE id = 5042".
		rows, err := custom.Query(ctx, "SELECT name FROM profiles WHERE id = 5042 /* any text */")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Error("the custom analyzer's route did not find the row")
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

// fixedAnalyzer reports every statement as a read of profiles where id = 5042.
type fixedAnalyzer struct{}

func (fixedAnalyzer) FromSQL(string, []any) (analyze.Analysis, error) {
	return analyze.Analysis{
		Op:       analyze.OpSelect,
		Scopes:   []int{-1},
		Tables:   []analyze.TableRef{{ID: 0, Name: "profiles"}},
		Bindings: []analyze.Binding{{Column: analyze.ColumnRef{Name: "id"}, Values: []any{int64(5042)}}},
	}, nil
}
