//go:build integration

package shard_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/shardtest"
)

// Failure injection: shards that stop, hang, lose their connections, or are
// abandoned by the caller. In every case the call must return (with an error
// naming the shard), healthy shards must stay usable, and nothing may be left
// running: each test ends with a goroutine-leak check.

const (
	rowsPerShard = 20000
	slowTimeout  = 300 * time.Millisecond
	mustReturnIn = 3 * time.Second // generous: a hang would be far longer
)

// failureCluster is three shards with a profiles table and rowsPerShard rows
// on each (inserted directly, so the keys are only meant for fan-out reads).
func failureCluster(t *testing.T) *shardtest.Cluster {
	t.Helper()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, "CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL)")
	for i, id := range cluster.IDs() {
		lo := int64(i * rowsPerShard)
		_, err := cluster.Direct(t, id).Exec("INSERT INTO profiles SELECT g, 'p' || g FROM generate_series($1::bigint, $2::bigint) g", lo, lo+rowsPerShard-1)
		if err != nil {
			t.Fatal(err)
		}
	}
	return cluster
}

// openDB opens a DB on the cluster that is closed before the leak check.
func openDB(t *testing.T, cluster *shardtest.Cluster, timeout time.Duration) *shard.DB {
	t.Helper()
	cfg := cluster.Config(txRegistry(t))
	cfg.ShardTimeout = timeout
	db, err := shard.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// noLeaks fails the test if goroutines started after this call are still
// running when the test function returns. Call it first, so its check runs
// last (after the DB is closed).
func noLeaks(t *testing.T) {
	t.Helper()
	opt := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, opt) })
}

// holdLock blocks every statement on profiles on one shard until the returned
// function is called, as a long-running migration or a stuck transaction would.
func holdLock(t *testing.T, cluster *shardtest.Cluster, id shard.ShardID) (release func()) {
	t.Helper()
	tx, err := cluster.Direct(t, id).BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("LOCK TABLE profiles IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	return func() { _ = tx.Rollback() }
}

// within runs f and fails the test if it takes longer than mustReturnIn.
func within(t *testing.T, what string, f func()) time.Duration {
	t.Helper()
	start := time.Now()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
		return time.Since(start)
	case <-time.After(mustReturnIn):
		t.Fatalf("%s did not return within %v", what, mustReturnIn)
		return 0
	}
}

// waitingOnLock counts the backends on a shard that are stuck waiting for a lock.
func waitingOnLock(t *testing.T, cluster *shardtest.Cluster, id shard.ShardID) int64 {
	t.Helper()
	return cluster.Count(t, id, "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()")
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestFailureInjection(t *testing.T) {
	cluster := failureCluster(t)
	ids := cluster.IDs()
	slow := ids[1]
	ctx := context.Background()

	t.Run("a slow shard hits the shard timeout", func(t *testing.T) {
		noLeaks(t)
		release := holdLock(t, cluster, slow)
		defer release()
		db := openDB(t, cluster, slowTimeout)
		defer db.Close()

		var err error
		took := within(t, "the fan-out query", func() {
			_, err = db.WithAllShards().Query(ctx, "SELECT id FROM profiles ORDER BY id")
		})
		var se *shard.ShardError
		if !errors.As(err, &se) || se.Shard != slow {
			t.Fatalf("err = %v, want a ShardError for %s", err, slow)
		}
		if took > 2*time.Second {
			t.Errorf("took %v with a %v shard timeout", took, slowTimeout)
		}

		// The statement was cancelled on the server too, not left waiting.
		eventually(t, "the blocked statement to be cancelled on the shard", func() bool { return waitingOnLock(t, cluster, slow) == 0 })

		// With AllowPartial the healthy shards still answer.
		var rows shard.Rows
		within(t, "the partial query", func() {
			rows, err = db.WithAllShards().Query(shard.AllowPartial(ctx), "SELECT id FROM profiles ORDER BY id")
		})
		if err != nil {
			t.Fatalf("AllowPartial: %v", err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		if err := rows.Err(); err != nil {
			t.Errorf("reading: %v", err)
		}
		_ = rows.Close()
		if failed := shard.ShardErrors(rows); n != 2*rowsPerShard || len(failed) != 1 || failed[0].Shard != slow {
			t.Errorf("read %d rows, shard errors %v; want %d rows and only %s failed", n, failed, 2*rowsPerShard, slow)
		}

		// A write is reported per shard: the others are applied.
		res, err := db.WithAllShards().Exec(ctx, "UPDATE profiles SET name = 'changed' WHERE id < 5")
		if err == nil || res.PerShard[slow].Err == nil {
			t.Errorf("the write should fail on %s: %v, %+v", slow, err, res)
		}
		if got := res.PerShard[ids[0]]; got.Err != nil || got.RowsAffected != 5 {
			t.Errorf("%s = %+v, want 5 rows updated", ids[0], got)
		}

		// Once the shard recovers, the same DB works again.
		release()
		rows2, err := db.WithAllShards().Query(ctx, "SELECT count(*) FROM profiles")
		if err != nil {
			t.Fatalf("after the lock was released: %v", err)
		}
		defer rows2.Close()
		var total int
		if rows2.Next() {
			_ = rows2.Scan(&total)
		}
		if total != 3*rowsPerShard {
			t.Errorf("count = %d, want %d", total, 3*rowsPerShard)
		}
	})

	t.Run("the caller cancels while a shard is blocked", func(t *testing.T) {
		noLeaks(t)
		release := holdLock(t, cluster, slow)
		defer release()
		db := openDB(t, cluster, 0)
		defer db.Close()

		cctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(200*time.Millisecond, cancel)
		var err error
		within(t, "the cancelled query", func() {
			_, err = db.WithAllShards().Query(cctx, "SELECT id FROM profiles ORDER BY id")
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
		eventually(t, "the blocked statement to be cancelled on the shard", func() bool { return waitingOnLock(t, cluster, slow) == 0 })
	})

	t.Run("the caller cancels in the middle of a merge", func(t *testing.T) {
		noLeaks(t)
		db := openDB(t, cluster, 0)
		defer db.Close()

		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		rows, err := db.WithAllShards().Query(cctx, "SELECT id, name FROM profiles ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		read := 0
		for rows.Next() {
			if read++; read == 100 {
				cancel()
			}
		}
		within(t, "finishing the cancelled merge", func() {})
		if err := rows.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("rows.Err() = %v, want context.Canceled", err)
		}
		if read >= 3*rowsPerShard {
			t.Errorf("read all %d rows: the cancellation did not stop the merge", read)
		}
		_ = rows.Close()
		eventually(t, "every connection to be released", func() bool { return connectionsInUse(db) == 0 })
	})

	t.Run("the caller abandons a merge without reading it", func(t *testing.T) {
		noLeaks(t)
		db := openDB(t, cluster, 0)
		defer db.Close()

		for i := 0; i < 5; i++ {
			rows, err := db.WithAllShards().Query(ctx, "SELECT id, name FROM profiles ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			rows.Next()
			_ = rows.Close() // early: most rows unread
		}
		eventually(t, "every connection to be released", func() bool { return connectionsInUse(db) == 0 })
	})

	t.Run("connections are killed under a running query", func(t *testing.T) {
		noLeaks(t)
		release := holdLock(t, cluster, slow)
		defer release()
		db := openDB(t, cluster, 0)
		defer db.Close()

		go func() { // kill the backend of the blocked statement once it is waiting
			for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
				if waitingOnLock(t, cluster, slow) > 0 {
					_, _ = cluster.Direct(t, slow).Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()")
					return
				}
			}
		}()
		var err error
		within(t, "the query whose connection was killed", func() {
			_, err = db.WithAllShards().Query(ctx, "SELECT id FROM profiles ORDER BY id")
		})
		var se *shard.ShardError
		if !errors.As(err, &se) || se.Shard != slow {
			t.Fatalf("err = %v, want a ShardError for %s", err, slow)
		}
	})

	t.Run("idle connections are killed between statements", func(t *testing.T) {
		noLeaks(t)
		db := openDB(t, cluster, 0)
		defer db.Close()

		count := func() (int, error) {
			rows, err := db.WithShard(slow).Query(ctx, "SELECT count(*) FROM profiles")
			if err != nil {
				return 0, err
			}
			defer rows.Close()
			var n int
			if rows.Next() {
				err = rows.Scan(&n)
			}
			return n, err
		}
		if _, err := count(); err != nil { // warm the pool
			t.Fatal(err)
		}
		if _, err := cluster.Direct(t, slow).Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"); err != nil {
			t.Fatal(err)
		}

		// The pool may hand out a dead connection once: that statement fails with
		// an error naming the shard, and the next one must work.
		var recovered bool
		for attempt := 0; attempt < 3 && !recovered; attempt++ {
			n, err := count()
			switch {
			case err == nil && n == rowsPerShard:
				recovered = true
			case err == nil:
				t.Fatalf("count = %d", n)
			default:
				var se *shard.ShardError
				if !errors.As(err, &se) || se.Shard != slow {
					t.Fatalf("err = %v, want a ShardError for %s", err, slow)
				}
			}
		}
		if !recovered {
			t.Error("the pool never recovered from killed connections")
		}
		// Other shards were never affected.
		rows, err := db.WithShard(ids[0]).Query(ctx, "SELECT count(*) FROM profiles")
		if err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
	})
}

// A shard whose database is stopped: container mode only (the test is skipped
// when the shards are ones you run yourself).
func TestShardStoppedMidQuery(t *testing.T) {
	if os.Getenv("SHARDTEST_DSNS") != "" {
		t.Skip("needs containers it can stop; SHARDTEST_DSNS shards are not stopped")
	}
	cluster := failureCluster(t)
	ids := cluster.IDs()
	stopped := ids[2]
	noLeaks(t)
	release := holdLock(t, cluster, stopped)
	defer release()
	db := openDB(t, cluster, 0)
	defer db.Close()
	ctx := context.Background()

	time.AfterFunc(300*time.Millisecond, func() { cluster.Stop(t, stopped) })
	var err error
	took := within(t, "the query on a shard that was stopped", func() {
		_, err = db.WithAllShards().Query(ctx, "SELECT id FROM profiles ORDER BY id")
	})
	var se *shard.ShardError
	if !errors.As(err, &se) || se.Shard != stopped {
		t.Fatalf("err = %v, want a ShardError for %s", err, stopped)
	}
	t.Logf("failed after %v: %v", took, se)

	// New statements fail fast instead of hanging, and say which shard.
	within(t, "a statement on the stopped shard", func() {
		_, err = db.WithShard(stopped).Query(ctx, "SELECT 1")
	})
	if !errors.As(err, &se) || se.Shard != stopped {
		t.Errorf("err = %v, want a ShardError for %s", err, stopped)
	}

	// The others carry on, alone or with AllowPartial.
	rows, err := db.WithAllShards().Query(shard.AllowPartial(ctx), "SELECT count(*) FROM profiles")
	if err != nil {
		t.Fatalf("AllowPartial: %v", err)
	}
	var n int
	if rows.Next() {
		_ = rows.Scan(&n)
	}
	_ = rows.Close()
	if failed := shard.ShardErrors(rows); n != 2*rowsPerShard || len(failed) != 1 || failed[0].Shard != stopped {
		t.Errorf("count = %d, failed = %v; want %d from the two shards left", n, failed, 2*rowsPerShard)
	}

	// Health says which one is down.
	for _, h := range db.Health(ctx) {
		if h.Healthy() == (h.ID == stopped) {
			t.Errorf("health of %s = %+v", h.ID, h)
		}
	}
}

func connectionsInUse(db *shard.DB) int {
	n := 0
	for _, s := range db.PoolStats() {
		n += s.InUse
	}
	return n
}
