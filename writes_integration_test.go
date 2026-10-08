//go:build integration

package shard_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
	"github.com/g8rswimmer/go-shard/shardtest"
)

func writesRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg, err := registry.New(
		registry.Sharded("items", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Colocated("item_notes", registry.With("items"), registry.Key("item_id")),
		registry.Global("countries"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

const itemsDDL = "CREATE TABLE items (id bigint PRIMARY KEY, label text NOT NULL)"

// batchSQL builds INSERT INTO items VALUES ($1,$2),($3,$4),... for ids from..to.
func batchSQL(from, to int64) (string, []any) {
	var groups []string
	var args []any
	for id := from; id <= to; id++ {
		args = append(args, id, fmt.Sprintf("item-%d", id))
		groups = append(groups, fmt.Sprintf("($%d, $%d)", len(args)-1, len(args)))
	}
	return "INSERT INTO items (id, label) VALUES " + strings.Join(groups, ", "), args
}

func countAll(t *testing.T, c *shardtest.Cluster, q string) (total int64, per map[shard.ShardID]int64) {
	t.Helper()
	per = map[shard.ShardID]int64{}
	for _, id := range c.IDs() {
		per[id] = c.Count(t, id, q)
		total += per[id]
	}
	return total, per
}

func TestWrites(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, itemsDDL)
	cluster.ExecAll(t, "CREATE TABLE item_notes (id bigint PRIMARY KEY, item_id bigint NOT NULL, note text)")
	cluster.ExecAll(t, "CREATE TABLE countries (code text PRIMARY KEY)")
	db := cluster.Open(t, writesRegistry(t))
	if err := db.EnsureIdempotencyTable(ctx); err != nil {
		t.Fatal(err)
	}
	// Calling it again is harmless.
	if err := db.EnsureIdempotencyTable(ctx); err != nil {
		t.Fatalf("EnsureIdempotencyTable is not repeatable: %v", err)
	}

	// An independent router with the same layout as the one Open builds.
	expected, err := router.New(router.Even(cluster.IDs()...)...)
	if err != nil {
		t.Fatal(err)
	}
	owner := func(id int64) shard.ShardID { s, _ := expected.ShardFor(id); return s }

	t.Run("a multi-row INSERT is split so each shard gets its own rows", func(t *testing.T) {
		sql, args := batchSQL(1, 30)
		res, err := db.Exec(ctx, sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.PerShard) != 3 || res.RowsAffected() != 30 {
			t.Fatalf("result %+v: want 30 rows across the three shards", res.PerShard)
		}

		// Each shard holds exactly the rows whose keys it owns, and the outcome
		// says which rows (by position in the VALUES list) it received.
		for sid, o := range res.PerShard {
			var want []int
			for i := int64(1); i <= 30; i++ {
				if owner(i) == sid {
					want = append(want, int(i-1))
				}
			}
			if fmt.Sprint(o.Rows) != fmt.Sprint(want) || o.RowsAffected != int64(len(want)) {
				t.Errorf("%s: outcome %+v, want rows %v", sid, o, want)
			}
			if got := cluster.Count(t, sid, "SELECT count(*) FROM items"); got != int64(len(want)) {
				t.Errorf("%s holds %d rows, want %d", sid, got, len(want))
			}
			for _, r := range want {
				if cluster.Count(t, sid, "SELECT count(*) FROM items WHERE id = $1 AND label = $2", int64(r+1), fmt.Sprintf("item-%d", r+1)) != 1 {
					t.Errorf("%s: row for id %d is missing or has the wrong values", sid, r+1)
				}
			}
		}
	})

	t.Run("a built multi-row INSERT is split the same way", func(t *testing.T) {
		b := query.InsertInto("items").Columns("id", "label")
		for id := int64(101); id <= 130; id++ {
			b.Row(id, fmt.Sprintf("built-%d", id))
		}
		st, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		res, err := db.ExecStatement(ctx, st)
		if err != nil || res.RowsAffected() != 30 || len(res.PerShard) != 3 {
			t.Fatalf("result %+v, err %v", res.PerShard, err)
		}
		for sid, o := range res.PerShard {
			for _, r := range o.Rows {
				if owner(int64(101+r)) != sid {
					t.Errorf("row %d was sent to %s but belongs to %s", r, sid, owner(int64(101+r)))
				}
			}
		}
	})

	t.Run("colocated rows follow their parent's key", func(t *testing.T) {
		const id = int64(7)
		if _, err := db.Exec(ctx, "INSERT INTO item_notes (id, item_id, note) VALUES (1, $1, 'a'), (2, $1, 'b')", id); err != nil {
			t.Fatal(err)
		}
		if got := cluster.Count(t, owner(id), "SELECT count(*) FROM item_notes WHERE item_id = $1", id); got != 2 {
			t.Errorf("notes for item %d are not with the item's shard: %d", id, got)
		}
	})

	t.Run("INSERT ... RETURNING on one shard", func(t *testing.T) {
		rows, err := db.Query(ctx, "INSERT INTO items (id, label) VALUES ($1, 'ret') RETURNING id, label", int64(500))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var id int64
		var label string
		if !rows.Next() || rows.Scan(&id, &label) != nil || id != 500 || label != "ret" {
			t.Errorf("returned %d %q (err %v)", id, label, rows.Err())
		}
	})

	t.Run("ON CONFLICT DO UPDATE changes the row where it lives", func(t *testing.T) {
		st, err := query.InsertInto("items").Columns("id", "label").Row(int64(1), "first").Row(int64(2), "second").
			OnConflictUpdate([]string{"id"}, "label").Build()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecStatement(ctx, st); err != nil {
			t.Fatal(err)
		}
		for id, want := range map[int64]string{1: "first", 2: "second"} {
			var got string
			if err := cluster.Direct(t, owner(id)).QueryRowContext(ctx, "SELECT label FROM items WHERE id = $1", id).Scan(&got); err != nil || got != want {
				t.Errorf("item %d label = %q (err %v), want %q", id, got, err, want)
			}
		}
	})

	t.Run("the shard key cannot be changed, and nothing is written", func(t *testing.T) {
		before, _ := countAll(t, cluster, "SELECT count(*) FROM items")
		for name, tc := range map[string]struct {
			sql  string
			args []any
		}{
			"update":   {"UPDATE items SET id = 999 WHERE id = $1", []any{int64(1)}},
			"upsert":   {"INSERT INTO items (id, label) VALUES ($1, 'x') ON CONFLICT (id) DO UPDATE SET id = 999", []any{int64(1)}},
			"set many": {"UPDATE items SET (label, id) = ('x', 999) WHERE id = $1", []any{int64(1)}},
		} {
			if _, err := db.Exec(ctx, tc.sql, tc.args...); !errors.Is(err, shard.ErrShardKeyImmutable) {
				t.Errorf("%s: error = %v, want ErrShardKeyImmutable", name, err)
			}
		}
		if cluster.Count(t, owner(1), "SELECT count(*) FROM items WHERE id = 999") != 0 {
			t.Error("the key was changed")
		}
		if after, _ := countAll(t, cluster, "SELECT count(*) FROM items"); after != before {
			t.Errorf("row count changed from %d to %d", before, after)
		}
	})

	t.Run("INSERTs that cannot be placed are refused and write nothing", func(t *testing.T) {
		before, _ := countAll(t, cluster, "SELECT count(*) FROM items")
		for name, sql := range map[string]string{
			"no key column":     "INSERT INTO items (label) VALUES ('x')",
			"DEFAULT key":       "INSERT INTO items (id, label) VALUES (DEFAULT, 'x')",
			"function key":      "INSERT INTO items (id, label) VALUES (abs(-3), 'x')",
			"no column list":    "INSERT INTO items VALUES (1, 'x')",
			"one row unplaced":  "INSERT INTO items (id, label) VALUES (900, 'ok'), (abs(-4), 'bad')",
			"INSERT ... SELECT": "INSERT INTO items (id, label) SELECT id, label FROM items",
		} {
			if _, err := db.Exec(ctx, sql); err == nil {
				t.Errorf("%s: expected a refusal", name)
			}
		}
		if after, _ := countAll(t, cluster, "SELECT count(*) FROM items"); after != before {
			t.Errorf("a refused INSERT wrote rows: %d before, %d after (the good row of a half-bad batch must not be written either)", before, after)
		}
	})

	t.Run("global tables are written on every shard", func(t *testing.T) {
		res, err := db.Exec(ctx, "INSERT INTO countries (code) VALUES ('US'), ('CA')")
		if err != nil || len(res.PerShard) != 3 || res.RowsAffected() != 6 {
			t.Fatalf("result %+v, err %v", res.PerShard, err)
		}
		_, per := countAll(t, cluster, "SELECT count(*) FROM countries")
		for sid, n := range per {
			if n != 2 {
				t.Errorf("%s holds %d countries, want 2", sid, n)
			}
		}
	})

	t.Run("after a partial failure FailedRows says what to send again", func(t *testing.T) {
		// Make shard-02 refuse the write by removing its table.
		victim := cluster.IDs()[1]
		if _, err := cluster.Direct(t, victim).ExecContext(ctx, "ALTER TABLE items RENAME TO items_away"); err != nil {
			t.Fatal(err)
		}

		const from, to = 1001, 1030
		b := query.InsertInto("items").Columns("id", "label")
		for id := int64(from); id <= to; id++ {
			b.Row(id, fmt.Sprintf("p-%d", id))
		}
		st, _ := b.Build()
		res, err := db.ExecStatement(ctx, st)

		var se *shard.ShardError
		if !errors.As(err, &se) || se.Shard != victim {
			t.Fatalf("error = %v, want a ShardError for %s", err, victim)
		}
		if failed := res.Failed(); len(failed) != 1 || failed[victim] == nil {
			t.Errorf("Failed() = %v, want only %s", failed, victim)
		}
		var wantFailed []int
		for id := int64(from); id <= to; id++ {
			if owner(id) == victim {
				wantFailed = append(wantFailed, int(id-from))
			}
		}
		if fmt.Sprint(res.FailedRows()) != fmt.Sprint(wantFailed) {
			t.Fatalf("FailedRows = %v, want %v", res.FailedRows(), wantFailed)
		}
		if got := res.RowsAffected(); got != int64(to-from+1-len(wantFailed)) {
			t.Errorf("RowsAffected = %d: the shards that worked must still count", got)
		}

		// The other shards committed their rows.
		for _, sid := range cluster.IDs() {
			if sid == victim {
				continue
			}
			var want int64
			for id := int64(from); id <= to; id++ {
				if owner(id) == sid {
					want++
				}
			}
			if got := cluster.Count(t, sid, "SELECT count(*) FROM items WHERE id BETWEEN $1 AND $2", int64(from), int64(to)); got != want {
				t.Errorf("%s holds %d of its %d rows", sid, got, want)
			}
		}

		// Fix the shard and send only the failed rows.
		if _, err := cluster.Direct(t, victim).ExecContext(ctx, "ALTER TABLE items_away RENAME TO items"); err != nil {
			t.Fatal(err)
		}
		retry := query.InsertInto("items").Columns("id", "label")
		for _, r := range res.FailedRows() {
			retry.Row(int64(from+r), fmt.Sprintf("p-%d", from+r))
		}
		rst, _ := retry.Build()
		if res2, err := db.ExecStatement(ctx, rst); err != nil || res2.RowsAffected() != int64(len(wantFailed)) {
			t.Fatalf("retry: %+v, %v", res2, err)
		}
		if got, _ := countAll(t, cluster, "SELECT count(*) FROM items WHERE id BETWEEN 1001 AND 1030"); got != 30 {
			t.Errorf("after the retry %d of 30 rows exist, want exactly 30 (no duplicates, none missing)", got)
		}
	})
}

// This is M4's central scenario: a batch where one shard fails and the others
// commit, then a retry with the same idempotency key that applies the write only
// where it had not been applied.
func TestIdempotentRetryAfterPartialFailure(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, itemsDDL)
	db := cluster.Open(t, writesRegistry(t))
	if err := db.EnsureIdempotencyTable(ctx); err != nil {
		t.Fatal(err)
	}
	expected, _ := router.New(router.Even(cluster.IDs()...)...)
	owner := func(id int64) shard.ShardID { s, _ := expected.ShardFor(id); return s }

	victim := cluster.IDs()[1]
	if _, err := cluster.Direct(t, victim).ExecContext(ctx, "ALTER TABLE items RENAME TO items_away"); err != nil {
		t.Fatal(err)
	}

	const key = "batch-2026-001"
	kctx := shard.WithIdempotencyKey(ctx, key)
	sql, args := batchSQL(1, 30)

	// Attempt 1: shard-02 fails, the other two commit.
	first, err := db.Exec(kctx, sql, args...)
	if err == nil {
		t.Fatal("attempt 1 should fail on the shard without a table")
	}
	committed := map[shard.ShardID]int64{}
	for sid, o := range first.PerShard {
		switch {
		case sid == victim && o.Err == nil:
			t.Errorf("%s should have failed", sid)
		case sid != victim && (o.Err != nil || o.Replayed):
			t.Errorf("%s: outcome %+v, want a fresh success", sid, o)
		case sid != victim:
			committed[sid] = o.RowsAffected
		default:
			// the failed shard: nothing to record
		}
	}
	if cluster.Count(t, victim, "SELECT count(*) FROM go_shard_idempotency_keys") != 0 {
		t.Error("the key must not be recorded on the shard where the write failed (it rolled back)")
	}
	for sid := range committed {
		if cluster.Count(t, sid, "SELECT count(*) FROM go_shard_idempotency_keys WHERE key = $1", key) != 1 {
			t.Errorf("%s should have recorded the key", sid)
		}
	}

	// Fix the shard, then retry with the SAME key and statement.
	if _, err := cluster.Direct(t, victim).ExecContext(ctx, "ALTER TABLE items_away RENAME TO items"); err != nil {
		t.Fatal(err)
	}
	second, err := db.Exec(kctx, sql, args...)
	if err != nil {
		t.Fatalf("the retry should succeed: %v", err)
	}
	for sid, o := range second.PerShard {
		if sid == victim {
			if o.Replayed || o.RowsAffected == 0 {
				t.Errorf("%s had not applied the write, so the retry must apply it now: %+v", sid, o)
			}
			continue
		}
		if !o.Replayed || o.RowsAffected != committed[sid] {
			t.Errorf("%s had already applied the write: want a replay reporting %d rows, got %+v", sid, committed[sid], o)
		}
	}
	if got := second.RowsAffected(); got != 30 {
		t.Errorf("the retry reports %d rows in total, want all 30", got)
	}

	// Every row exists exactly once, on the shard that owns it.
	if total, _ := countAll(t, cluster, "SELECT count(*) FROM items"); total != 30 {
		t.Errorf("%d rows exist, want exactly 30", total)
	}
	for id := int64(1); id <= 30; id++ {
		if cluster.Count(t, owner(id), "SELECT count(*) FROM items WHERE id = $1", id) != 1 {
			t.Errorf("item %d is not on its shard exactly once", id)
		}
	}

	// A third attempt changes nothing and reports the same totals.
	third, err := db.Exec(kctx, sql, args...)
	if err != nil || third.RowsAffected() != 30 {
		t.Fatalf("third attempt: %+v, %v", third.PerShard, err)
	}
	for sid, o := range third.PerShard {
		if !o.Replayed {
			t.Errorf("%s: want a replay on the third attempt", sid)
		}
	}

	// Without a key, the same blind retry would have hit duplicate keys.
	if _, err := db.Exec(ctx, sql, args...); err == nil {
		t.Error("an unkeyed repeat of an applied batch should fail on duplicates")
	}

	t.Run("a key cannot be reused for a different statement", func(t *testing.T) {
		other, otherArgs := batchSQL(40, 45)
		if _, err := db.Exec(kctx, other, otherArgs...); !errors.Is(err, shard.ErrIdempotencyKeyReused) {
			t.Errorf("error = %v, want ErrIdempotencyKeyReused", err)
		}
		// Same SQL, different values is also a different statement.
		changed := append([]any(nil), args...)
		changed[1] = "different label"
		if _, err := db.Exec(kctx, sql, changed...); !errors.Is(err, shard.ErrIdempotencyKeyReused) {
			t.Errorf("changed arguments: error = %v, want ErrIdempotencyKeyReused", err)
		}
		if total, _ := countAll(t, cluster, "SELECT count(*) FROM items"); total != 30 {
			t.Errorf("a refused reuse wrote rows: %d", total)
		}
	})

	t.Run("a different key is a different operation", func(t *testing.T) {
		other, otherArgs := batchSQL(40, 45)
		res, err := db.Exec(shard.WithIdempotencyKey(ctx, "batch-2026-002"), other, otherArgs...)
		if err != nil || res.RowsAffected() != 6 {
			t.Errorf("result %+v, err %v", res.PerShard, err)
		}
	})

	t.Run("old keys can be pruned", func(t *testing.T) {
		keys, _ := countAll(t, cluster, `SELECT count(*) FROM go_shard_idempotency_keys`)
		if keys == 0 {
			t.Fatal("expected recorded keys")
		}
		if n, err := db.PruneIdempotencyKeys(ctx, time.Hour); err != nil || n != 0 {
			t.Errorf("pruning keys newer than an hour removed %d (err %v), want 0", n, err)
		}
		time.Sleep(20 * time.Millisecond)
		n, err := db.PruneIdempotencyKeys(ctx, 10*time.Millisecond)
		if err != nil || n != keys {
			t.Errorf("pruned %d (err %v), want all %d", n, err, keys)
		}
		if left, _ := countAll(t, cluster, `SELECT count(*) FROM go_shard_idempotency_keys`); left != 0 {
			t.Errorf("%d keys left", left)
		}
		if _, err := db.PruneIdempotencyKeys(ctx, -time.Second); err == nil {
			t.Error("a negative age should be refused")
		}
	})
}

func TestIdempotencyUnderContention(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, itemsDDL)
	db := cluster.Open(t, writesRegistry(t))
	if err := db.EnsureIdempotencyTable(ctx); err != nil {
		t.Fatal(err)
	}

	// Many callers send the same write with the same key at once. Exactly one
	// applies it; the rest see that it was applied.
	const callers = 12
	kctx := shard.WithIdempotencyKey(ctx, "race")
	results := make([]shard.WriteResult, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = db.Exec(kctx, "INSERT INTO items (id, label) VALUES ($1, 'once')", int64(42))
		}()
	}
	close(start)
	wg.Wait()

	applied, replayed := 0, 0
	for i := range results {
		if errs[i] != nil {
			t.Errorf("caller %d: %v", i, errs[i])
			continue
		}
		for _, o := range results[i].PerShard {
			switch {
			case o.Replayed:
				replayed++
			default:
				applied++
			}
			if o.RowsAffected != 1 {
				t.Errorf("caller %d reports %d rows, want 1 whether it applied or replayed", i, o.RowsAffected)
			}
		}
	}
	if applied != 1 || replayed != callers-1 {
		t.Errorf("%d callers applied the write and %d replayed it; want exactly 1 and %d", applied, replayed, callers-1)
	}
	if total, _ := countAll(t, cluster, "SELECT count(*) FROM items"); total != 1 {
		t.Errorf("%d rows exist, want 1", total)
	}
}

func TestIdempotencyFailureLeavesNoKeyBehind(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 2)
	cluster.ExecAll(t, itemsDDL)
	db := cluster.Open(t, writesRegistry(t))
	if err := db.EnsureIdempotencyTable(ctx); err != nil {
		t.Fatal(err)
	}
	expected, _ := router.New(router.Even(cluster.IDs()...)...)
	owner, _ := expected.ShardFor(int64(7))

	// A conflicting row makes the write fail.
	if _, err := db.Exec(ctx, "INSERT INTO items (id, label) VALUES (7, 'existing')"); err != nil {
		t.Fatal(err)
	}
	kctx := shard.WithIdempotencyKey(ctx, "retry-me")
	if _, err := db.Exec(kctx, "INSERT INTO items (id, label) VALUES (7, 'new')"); err == nil {
		t.Fatal("expected a duplicate key error")
	}
	if n := cluster.Count(t, owner, "SELECT count(*) FROM go_shard_idempotency_keys"); n != 0 {
		t.Errorf("%d keys recorded after a failed write: it must roll back with the write", n)
	}

	// Remove the conflict: the same key now works, because nothing was recorded.
	cluster.ExecAll(t, "DELETE FROM items")
	res, err := db.Exec(kctx, "INSERT INTO items (id, label) VALUES (7, 'new')")
	if err != nil || res.RowsAffected() != 1 || res.PerShard[owner].Replayed {
		t.Errorf("result %+v, err %v: the retry must apply the write", res.PerShard, err)
	}
}

func TestIdempotencyAppliesToGlobalWrites(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, "CREATE TABLE countries (code text PRIMARY KEY)")
	db := cluster.Open(t, writesRegistry(t))
	if err := db.EnsureIdempotencyTable(ctx); err != nil {
		t.Fatal(err)
	}

	kctx := shard.WithIdempotencyKey(ctx, "seed-countries")
	for attempt := 1; attempt <= 2; attempt++ {
		res, err := db.Exec(kctx, "INSERT INTO countries (code) VALUES ('US')")
		if err != nil || len(res.PerShard) != 3 || res.RowsAffected() != 3 {
			t.Fatalf("attempt %d: %+v, %v", attempt, res.PerShard, err)
		}
		for sid, o := range res.PerShard {
			if o.Replayed != (attempt == 2) {
				t.Errorf("attempt %d, %s: Replayed = %v", attempt, sid, o.Replayed)
			}
		}
	}
}

func TestIdempotencyTableMissing(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 2)
	cluster.ExecAll(t, itemsDDL)
	cfg := cluster.Config(writesRegistry(t))
	cfg.IdempotencyTable = "never_created"
	db, err := shard.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(shard.WithIdempotencyKey(ctx, "k"), "INSERT INTO items (id, label) VALUES (1, 'x')")
	if !errors.Is(err, shard.ErrIdempotencyTableMissing) {
		t.Fatalf("error = %v, want ErrIdempotencyTableMissing", err)
	}
	for _, w := range []string{"EnsureIdempotencyTable", "never_created", "CREATE TABLE IF NOT EXISTS"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("the error should say how to fix it; missing %q: %v", w, err)
		}
	}
	if total, _ := countAll(t, cluster, "SELECT count(*) FROM items"); total != 0 {
		t.Errorf("the write must not happen when the key cannot be recorded: %d rows", total)
	}

	// And the table can be created under a schema-qualified name.
	cfg.IdempotencyTable = "public.custom_keys"
	custom, err := shard.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer custom.Close()
	if err := custom.EnsureIdempotencyTable(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := custom.Exec(shard.WithIdempotencyKey(ctx, "k"), "INSERT INTO items (id, label) VALUES (1, 'x')"); err != nil {
		t.Errorf("with the table in place: %v", err)
	}
}

// A real outage: a shard stops. The writes that belong to the other shards
// commit, and the result says which rows were lost. Containers only.
func TestWriteWhileAShardIsDown(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, itemsDDL)
	db := cluster.Open(t, writesRegistry(t))
	expected, _ := router.New(router.Even(cluster.IDs()...)...)
	owner := func(id int64) shard.ShardID { s, _ := expected.ShardFor(id); return s }

	down := cluster.IDs()[1]
	cluster.Stop(t, down) // skips when the cluster uses existing databases

	sql, args := batchSQL(1, 30)
	res, err := db.Exec(ctx, sql, args...)

	var se *shard.ShardError
	if !errors.As(err, &se) || se.Shard != down {
		t.Fatalf("error = %v, want a ShardError for %s", err, down)
	}
	var lost []int
	for id := int64(1); id <= 30; id++ {
		if owner(id) == down {
			lost = append(lost, int(id-1))
		}
	}
	if fmt.Sprint(res.FailedRows()) != fmt.Sprint(lost) {
		t.Errorf("FailedRows = %v, want the rows %v that belong to the stopped shard", res.FailedRows(), lost)
	}
	for _, sid := range cluster.IDs() {
		if sid == down {
			continue
		}
		if o := res.PerShard[sid]; o.Err != nil || o.RowsAffected == 0 {
			t.Errorf("%s: outcome %+v, want it to have committed", sid, o)
		}
		var want int64
		for id := int64(1); id <= 30; id++ {
			if owner(id) == sid {
				want++
			}
		}
		if got := cluster.Count(t, sid, "SELECT count(*) FROM items"); got != want {
			t.Errorf("%s holds %d rows, want %d", sid, got, want)
		}
	}

	// Health says which shard is down.
	for _, h := range db.Health(ctx) {
		if h.Healthy() == (h.ID == down) {
			t.Errorf("%s: healthy = %v", h.ID, h.Healthy())
		}
	}
}
