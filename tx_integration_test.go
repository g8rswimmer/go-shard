//go:build integration

package shard_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
	"github.com/g8rswimmer/go-shard/shardtest"
)

func txRegistry(t *testing.T) *registry.Registry {
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

func TestTransactions(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, "CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL)")
	cluster.ExecAll(t, "CREATE TABLE addresses (id bigint PRIMARY KEY, profile_id bigint NOT NULL, city text)")
	cluster.ExecAll(t, "CREATE TABLE countries (code text PRIMARY KEY)")
	cluster.ExecAll(t, "INSERT INTO countries VALUES ('US'), ('CA')")
	// A statement that blocks (on a lock a leaked transaction still holds, say)
	// fails after a few seconds instead of hanging the test.
	cfg := cluster.Config(txRegistry(t))
	cfg.ShardTimeout = 5 * time.Second
	db, err := shard.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	expected, _ := router.New(router.Even(cluster.IDs()...)...)
	owner := func(id int64) shard.ShardID { s, _ := expected.ShardFor(id); return s }
	// An id on a different shard from `id`.
	elsewhere := func(id int64) int64 {
		for k := id + 1; ; k++ {
			if owner(k) != owner(id) {
				return k
			}
		}
	}
	profilesWithName := func(name string) int64 {
		var n int64
		for _, sid := range cluster.IDs() {
			n += cluster.Count(t, sid, "SELECT count(*) FROM profiles WHERE name = $1", name)
		}
		return n
	}
	addressesFor := func(id int64) int64 {
		var n int64
		for _, sid := range cluster.IDs() {
			n += cluster.Count(t, sid, "SELECT count(*) FROM addresses WHERE profile_id = $1", id)
		}
		return n
	}

	t.Run("a profile and its addresses commit together", func(t *testing.T) {
		const id = int64(1)
		tx, err := db.Begin(ctx, shard.ForTable("profiles", id))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if tx.Shard() != owner(id) {
			t.Fatalf("the transaction is on %s, want %s", tx.Shard(), owner(id))
		}

		if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'committed')", id); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO addresses (id, profile_id, city) VALUES (10, $1, 'Austin'), (11, $1, 'Boston')", id); err != nil {
			t.Fatal(err)
		}

		// Nothing is visible to anyone else until Commit.
		if profilesWithName("committed") != 0 || addressesFor(id) != 0 {
			t.Error("uncommitted rows are visible outside the transaction")
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if profilesWithName("committed") != 1 || addressesFor(id) != 2 {
			t.Error("committed rows are missing")
		}
		if got := cluster.Count(t, owner(id), "SELECT count(*) FROM addresses WHERE profile_id = $1", id); got != 2 {
			t.Errorf("the addresses are not on the profile's shard: %d", got)
		}
	})

	t.Run("a profile and its addresses roll back together", func(t *testing.T) {
		const id = int64(2)
		tx, err := db.Begin(ctx, shard.ForTable("profiles", id))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'rolled-back')", id); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO addresses (id, profile_id, city) VALUES (20, $1, 'Austin')", id); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if profilesWithName("rolled-back") != 0 || addressesFor(id) != 0 {
			t.Error("rolled-back rows exist")
		}
	})

	t.Run("a failing statement lets the whole unit be rolled back", func(t *testing.T) {
		const id = int64(3)
		err := db.InTx(ctx, shard.ForTable("profiles", id), func(tx shard.Tx) error {
			if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'half')", id); err != nil {
				return err
			}
			// The second insert violates the primary key of addresses.
			if _, err := tx.Exec(ctx, "INSERT INTO addresses (id, profile_id) VALUES (30, $1)", id); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO addresses (id, profile_id) VALUES (30, $1)", id)
			return err
		})
		var se *shard.ShardError
		if !errors.As(err, &se) || !strings.Contains(err.Error(), "duplicate key") {
			t.Fatalf("error = %v, want the duplicate key failure", err)
		}
		if profilesWithName("half") != 0 || addressesFor(id) != 0 {
			t.Error("a failed transaction left rows behind: the profile must not exist without its addresses")
		}
	})

	t.Run("a statement for another shard is refused without side effects", func(t *testing.T) {
		const id = int64(4)
		stray := elsewhere(id)
		tx, err := db.Begin(ctx, shard.ForTable("profiles", id))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'here')", id); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'stray')", stray)
		if !errors.Is(err, shard.ErrCrossShardTx) {
			t.Fatalf("error = %v, want ErrCrossShardTx", err)
		}
		for _, w := range []string{string(owner(id)), string(owner(stray)), "colocation"} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("the error should mention %q: %v", w, err)
			}
		}
		// Reads and updates for the other shard are refused too.
		if _, err := tx.Query(ctx, "SELECT * FROM profiles WHERE id = $1", stray); !errors.Is(err, shard.ErrCrossShardTx) {
			t.Errorf("read: error = %v", err)
		}
		if _, err := tx.Exec(ctx, "UPDATE profiles SET name = 'x' WHERE id = $1", stray); !errors.Is(err, shard.ErrCrossShardTx) {
			t.Errorf("update: error = %v", err)
		}

		// The refusal happened before anything was sent, so the transaction is
		// intact and can continue.
		if _, err := tx.Exec(ctx, "INSERT INTO addresses (id, profile_id) VALUES (40, $1)", id); err != nil {
			t.Fatalf("the transaction should still be usable: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if profilesWithName("here") != 1 || profilesWithName("stray") != 0 || addressesFor(id) != 1 {
			t.Error("the committed work is wrong")
		}
		if got := cluster.Count(t, owner(stray), "SELECT count(*) FROM profiles WHERE id = $1", stray); got != 0 {
			t.Error("the stray row was written to its own shard")
		}
	})

	t.Run("a transaction reads its own writes, including joins", func(t *testing.T) {
		const id = int64(5)
		err := db.InTx(ctx, shard.ForTable("profiles", id), func(tx shard.Tx) error {
			if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'joined')", id); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "INSERT INTO addresses (id, profile_id, city) VALUES (50, $1, 'Austin'), (51, $1, 'Boston')", id); err != nil {
				return err
			}
			rows, err := tx.Query(ctx, `SELECT a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id
				WHERE p.id = $1 ORDER BY a.city`, id)
			if err != nil {
				return err
			}
			defer rows.Close()
			var cities []string
			for rows.Next() {
				var c string
				if err := rows.Scan(&c); err != nil {
					return err
				}
				cities = append(cities, c)
			}
			if fmt.Sprint(cities) != "[Austin Boston]" {
				return fmt.Errorf("the join inside the transaction returned %v", cities)
			}
			return rows.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("built statements and batches work inside a transaction", func(t *testing.T) {
		const id = int64(6)
		st, _ := query.InsertInto("addresses").Columns("id", "profile_id", "city").
			Row(60, id, "a").Row(61, id, "b").Row(62, id, "c").Build()
		err := db.InTx(ctx, shard.ForTable("profiles", id), func(tx shard.Tx) error {
			if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'built')", id); err != nil {
				return err
			}
			res, err := tx.ExecStatement(ctx, st)
			if err != nil {
				return err
			}
			if res.RowsAffected() != 3 || fmt.Sprint(res.PerShard[tx.Shard()].Rows) != "[0 1 2]" {
				return fmt.Errorf("result %+v", res.PerShard)
			}
			rows, err := tx.QueryStatement(ctx, mustBuild(t, query.From("addresses").Columns("city").Where(query.Eq("profile_id", id)).OrderBy("city", query.Asc)))
			if err != nil {
				return err
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				n++
			}
			if n != 3 {
				return fmt.Errorf("read %d rows", n)
			}
			return rows.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
		if addressesFor(id) != 3 {
			t.Error("the batch was not committed")
		}
	})

	t.Run("global tables: reads work, writes are refused", func(t *testing.T) {
		for _, id := range []int64{7, elsewhere(7), elsewhere(elsewhere(7))} {
			err := db.InTx(ctx, shard.ForTable("profiles", id), func(tx shard.Tx) error {
				rows, err := tx.Query(ctx, "SELECT code FROM countries ORDER BY code")
				if err != nil {
					return err
				}
				defer rows.Close()
				n := 0
				for rows.Next() {
					n++
				}
				if n != 2 {
					return fmt.Errorf("transaction on %s read %d countries, want 2", tx.Shard(), n)
				}
				_, err = tx.Exec(ctx, "INSERT INTO countries (code) VALUES ('MX')")
				if !errors.Is(err, shard.ErrCrossShardTx) || !strings.Contains(err.Error(), "global table") {
					return fmt.Errorf("a global write should be refused: %w", err)
				}
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		}
		for _, sid := range cluster.IDs() {
			if cluster.Count(t, sid, "SELECT count(*) FROM countries WHERE code = 'MX'") != 0 {
				t.Errorf("%s: the refused write happened", sid)
			}
		}
	})

	t.Run("Unchecked runs what the router cannot place", func(t *testing.T) {
		const id = int64(8)
		err := db.InTx(ctx, shard.ForTable("profiles", id), func(tx shard.Tx) error {
			if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'scan')", id); err != nil {
				return err
			}
			// Checked: refused, because a scan by name does not say which shard.
			if _, err := tx.Query(ctx, "SELECT count(*) FROM profiles WHERE name = 'scan'"); !errors.Is(err, shard.ErrShardKeyRequired) {
				return fmt.Errorf("a checked scan should be refused: %w", err)
			}
			// Unchecked: runs on this transaction's shard, seeing the uncommitted row.
			rows, err := tx.Unchecked().Query(ctx, "WITH x AS (SELECT count(*) AS n FROM profiles WHERE name = 'scan') SELECT n FROM x")
			if err != nil {
				return err
			}
			defer rows.Close()
			var n int
			if !rows.Next() || rows.Scan(&n) != nil || n != 1 {
				return fmt.Errorf("unchecked scan returned %d, rows error: %w", n, rows.Err())
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("InTx commits on nil, rolls back on error and on panic", func(t *testing.T) {
		// A transaction left open would hold its connection for good, so check
		// that it has ended, not only that its rows are absent.
		ended := func(t *testing.T, tx shard.Tx) {
			t.Helper()
			if _, err := tx.Exec(ctx, "SELECT 1"); !errors.Is(err, shard.ErrTxDone) {
				t.Errorf("the transaction should have ended, but a statement gave: %v", err)
			}
		}

		boom := errors.New("boom")
		var failed shard.Tx
		err := db.InTx(ctx, shard.ForTable("profiles", 9), func(tx shard.Tx) error {
			failed = tx
			_, _ = tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES (9, 'fn-error')")
			return boom
		})
		if !errors.Is(err, boom) || profilesWithName("fn-error") != 0 {
			t.Errorf("error return: err %v, rows %d", err, profilesWithName("fn-error"))
		}
		ended(t, failed)

		var panicked shard.Tx
		func() {
			defer func() {
				if recover() == nil {
					t.Error("the panic must continue")
				}
			}()
			_ = db.InTx(ctx, shard.ForTable("profiles", 10), func(tx shard.Tx) error {
				panicked = tx
				_, _ = tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES (10, 'fn-panic')")
				panic("oops")
			})
		}()
		if profilesWithName("fn-panic") != 0 {
			t.Error("a panic must roll back")
		}
		ended(t, panicked)

		var committed shard.Tx
		if err := db.InTx(ctx, shard.ForTable("profiles", 10), func(tx shard.Tx) error {
			committed = tx
			_, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES (10, 'after-panic')")
			return err
		}); err != nil || profilesWithName("after-panic") != 1 {
			t.Errorf("a successful InTx: %v", err)
		}
		ended(t, committed)
	})

	t.Run("a transaction that has ended refuses more work", func(t *testing.T) {
		tx, _ := db.Begin(ctx, shard.ForShard(cluster.IDs()[0]))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "SELECT 1"); !errors.Is(err, shard.ErrTxDone) {
			t.Errorf("exec: %v", err)
		}
		if err := tx.Commit(); !errors.Is(err, shard.ErrTxDone) {
			t.Errorf("commit: %v", err)
		}
		if err := tx.Rollback(); !errors.Is(err, shard.ErrTxDone) {
			t.Errorf("rollback: %v", err)
		}
	})

	t.Run("a read-only transaction refuses writes", func(t *testing.T) {
		tx, err := db.Begin(ctx, shard.ForTable("profiles", 11), shard.ReadOnly())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		_, err = tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES (11, 'ro')")
		if err == nil || !strings.Contains(err.Error(), "read-only") {
			t.Errorf("error = %v, want PostgreSQL's read-only refusal", err)
		}
	})

	t.Run("isolation levels are passed through", func(t *testing.T) {
		tx, err := db.Begin(ctx, shard.ForShard(cluster.IDs()[0]), shard.Isolation(sql.LevelSerializable))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		rows, err := tx.Query(ctx, "SELECT current_setting('transaction_isolation')")
		if err != nil {
			// A bare SELECT without tables is routed to any shard, which is this one.
			t.Fatal(err)
		}
		defer rows.Close()
		var level string
		if !rows.Next() || rows.Scan(&level) != nil || level != "serializable" {
			t.Errorf("isolation = %q (err %v), want serializable", level, rows.Err())
		}
	})

	t.Run("cancelling the context rolls the transaction back", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		tx, err := db.Begin(cctx, shard.ForTable("profiles", 12))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(cctx, "INSERT INTO profiles (id, name) VALUES (12, 'cancelled')"); err != nil {
			t.Fatal(err)
		}
		cancel()
		time.Sleep(200 * time.Millisecond)
		if err := tx.Commit(); err == nil {
			t.Error("commit after cancellation must fail")
		}
		if profilesWithName("cancelled") != 0 {
			t.Error("a cancelled transaction left rows behind")
		}
	})

	t.Run("transactions on one shard are isolated from each other", func(t *testing.T) {
		const id = int64(13)
		a, _ := db.Begin(ctx, shard.ForTable("profiles", id))
		b, _ := db.Begin(ctx, shard.ForTable("profiles", id))
		defer func() { _ = a.Rollback() }()
		defer func() { _ = b.Rollback() }()

		if _, err := a.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'mine')", id); err != nil {
			t.Fatal(err)
		}
		rows, err := b.Query(ctx, "SELECT count(*) FROM profiles WHERE id = $1", id)
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if rows.Next() {
			_ = rows.Scan(&n)
		}
		_ = rows.Close()
		if n != 0 {
			t.Error("a second transaction saw the first one's uncommitted row")
		}
	})

	t.Run("bad targets and keys are refused", func(t *testing.T) {
		if _, err := db.Begin(ctx, shard.ForShard("nope")); !errors.Is(err, shard.ErrUnknownShard) {
			t.Errorf("unknown shard: %v", err)
		}
		if _, err := db.Begin(ctx, shard.ForTable("mystery", 1)); !errors.Is(err, shard.ErrUnknownTable) {
			t.Errorf("unknown table: %v", err)
		}
		if _, err := db.Begin(ctx, shard.TxTarget{}); err == nil {
			t.Error("a zero target should be refused")
		}
		// ForTable converts the key; the text and the number pick the same shard.
		a, _ := db.Begin(ctx, shard.ForTable("profiles", "42"))
		b, _ := db.Begin(ctx, shard.ForTable("profiles", 42))
		if a.Shard() != b.Shard() {
			t.Errorf("the text and the number picked %s and %s", a.Shard(), b.Shard())
		}
		_ = a.Rollback()
		_ = b.Rollback()
	})
}

func mustBuild(t *testing.T, b interface {
	Build() (query.Statement, error)
}) query.Statement {
	t.Helper()
	st, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return st
}
