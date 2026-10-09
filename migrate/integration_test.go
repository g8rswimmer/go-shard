//go:build integration

package migrate_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/g8rswimmer/go-shard/migrate"
	"github.com/g8rswimmer/go-shard/shardtest"
)

// write puts migration files in a new directory and returns its file:// URL.
func write(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return "file://" + filepath.ToSlash(dir)
}

func mustExec(t *testing.T, c *shardtest.Cluster, id migrate.ShardID, query string) {
	t.Helper()
	if _, err := c.Direct(t, id).Exec(query); err != nil {
		t.Fatal(err)
	}
}

var migrations = map[string]string{
	"1_profiles.up.sql": `CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL);`,
	// Two statements: if the second fails, the first must not stay behind.
	"2_countries.up.sql": `CREATE TABLE country_marker (n int);
CREATE TABLE countries (code text PRIMARY KEY, name text NOT NULL);
INSERT INTO countries VALUES ('NZ', 'New Zealand'), ('PT', 'Portugal');`,
	"3_nickname.up.sql": `ALTER TABLE profiles ADD COLUMN nickname text;`,
}

func TestMigrationsAcrossShards(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	ids := cluster.IDs()
	url := write(t, migrations)

	// shard-02 already has a table that migration 2 wants to create.
	mustExec(t, cluster, ids[1], "CREATE TABLE countries (junk int)")

	runner := cluster.Migrator(t, migrate.FromURL(url), migrate.WithPolicy(migrate.ContinueOnFailure))

	// Nothing is applied yet: all shards agree.
	st, err := runner.Status(ctx)
	if err != nil || st.Drift() || len(st.Behind()) != 3 || st.Latest != 3 {
		t.Fatalf("before: %v\n%s", err, st)
	}

	// 1. One shard fails.
	res, err := runner.Up(ctx)
	var se *migrate.ShardError
	if !errors.Is(err, migrate.ErrFailed) || !errors.As(err, &se) || se.Shard != ids[1] {
		t.Fatalf("Up error = %v, want a failure on %s", err, ids[1])
	}
	t.Logf("the failure: %v", se)
	if !strings.Contains(se.Error(), "migration 2:") || !strings.Contains(se.Error(), "countries") {
		t.Errorf("the error should say which migration and what failed: %v", se)
	}
	if got := res.PerShard[ids[0]].After.Version; got != 3 {
		t.Errorf("%s at %d, want 3", ids[0], got)
	}

	// 2. Status shows the drift, and the failed shard is clean at its last good
	// version, not dirty.
	st, err = runner.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("status after the failure:\n%s", st)
	if !st.Drift() || len(st.Dirty()) != 0 {
		t.Errorf("want drift and no dirty shard:\n%s", st)
	}
	if b := st.Behind(); len(b) != 1 || b[0] != ids[1] {
		t.Errorf("Behind = %v, want [%s]", b, ids[1])
	}
	if got := st.Shards[ids[1]].State; !got.Applied || got.Version != 1 {
		t.Errorf("%s = %v, want 1", ids[1], got)
	}

	// 3. The failed migration left nothing behind on the shard.
	if n := cluster.Count(t, ids[1], "SELECT count(*) FROM pg_class WHERE relname = 'country_marker'"); n != 0 {
		t.Error("the failed migration's first statement was left behind: a migration is not atomic")
	}

	// 4. Fix the shard and run again: everything ends at the same version.
	if _, err := cluster.Direct(t, ids[1]).Exec("DROP TABLE countries"); err != nil {
		t.Fatal(err)
	}
	if res, err = runner.Up(ctx); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if got := res.PerShard[ids[0]]; got.Before.Version != 3 || got.After.Version != 3 {
		t.Errorf("a shard already at 3 should be left alone: %+v", got)
	}
	if got := res.PerShard[ids[1]]; got.Before.Version != 1 || got.After.Version != 3 {
		t.Errorf("%s: %+v, want 1 -> 3", ids[1], got)
	}
	st, err = runner.Status(ctx)
	if err != nil || st.Drift() || !st.UpToDate() {
		t.Fatalf("after the fix: %v\n%s", err, st)
	}
	for _, id := range ids {
		if n := cluster.Count(t, id, "SELECT count(*) FROM countries"); n != 2 {
			t.Errorf("%s has %d countries, want 2 (global data on every shard)", id, n)
		}
		if n := cluster.Count(t, id, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'profiles' AND column_name = 'nickname'"); n != 1 {
			t.Errorf("%s is missing profiles.nickname", id)
		}
	}
}

func TestHaltOnFailureStopsAtTheFirstBadShard(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	ids := cluster.IDs()
	mustExec(t, cluster, ids[0], "CREATE TABLE countries (junk int)")

	runner := cluster.Migrator(t, migrate.FromURL(write(t, migrations)), migrate.WithConcurrency(1))
	res, err := runner.Up(ctx)
	if !errors.Is(err, migrate.ErrFailed) {
		t.Fatalf("error = %v", err)
	}
	if f, s := res.Failed(), res.Skipped(); len(f) != 1 || f[0] != ids[0] || len(s) != 2 {
		t.Errorf("failed %v, skipped %v; want %s failed and the other two not started", f, s, ids[0])
	}
	for _, id := range ids[1:] {
		if st := res.PerShard[id].After; st.Applied {
			t.Errorf("%s was touched: %v", id, st)
		}
	}
}

func TestConcurrentRunnersTakeTurns(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 2)
	src := migrate.FromURL(write(t, migrations))

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = cluster.Migrator(t, src).Up(ctx)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("runner %d: %v (the advisory lock should make them take turns)", i, err)
		}
	}
	if st, _ := cluster.Migrator(t, src).Status(ctx); !st.UpToDate() {
		t.Errorf("after four runners:\n%s", st)
	}
}

func TestDirtyShardIsReportedAndRepaired(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 2)
	ids := cluster.IDs()
	runner := cluster.Migrator(t, migrate.FromURL(write(t, migrations)))
	if _, err := runner.UpTo(ctx, 1); err != nil {
		t.Fatal(err)
	}

	// A crash in the middle of migration 2 looks like this.
	if _, err := cluster.Direct(t, ids[0]).Exec("UPDATE schema_migrations SET version = 2, dirty = true"); err != nil {
		t.Fatal(err)
	}
	st, _ := runner.Status(ctx)
	if d := st.Dirty(); len(d) != 1 || d[0] != ids[0] || !st.Drift() {
		t.Fatalf("status:\n%s", st)
	}
	if _, err := runner.Up(ctx); err == nil || !strings.Contains(err.Error(), "irty") {
		t.Errorf("Up on a dirty shard: %v, want it refused", err)
	}

	if err := runner.Force(ctx, ids[0], 1); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Up(ctx); err != nil {
		t.Fatalf("after Force: %v", err)
	}
	if st, _ := runner.Status(ctx); !st.UpToDate() {
		t.Errorf("after the repair:\n%s", st)
	}
}

func TestStatusNamesAShardThatIsDown(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	ids := cluster.IDs()
	runner := cluster.Migrator(t, migrate.FromURL(write(t, migrations)))
	if _, err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}

	cluster.Stop(t, ids[2])
	st, err := runner.Status(ctx)
	var se *migrate.ShardError
	if !errors.As(err, &se) || se.Shard != ids[2] || !st.Drift() {
		t.Errorf("Status = %v (drift %v), want an error naming %s", err, st.Drift(), ids[2])
	}
	if u := st.Unreadable(); len(u) != 1 || u[0] != ids[2] {
		t.Errorf("Unreadable = %v", u)
	}
}

func TestCustomTableAndEmbeddedStyleSource(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 2)
	dir := t.TempDir()
	for name, body := range migrations {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runner := cluster.Migrator(t, migrate.FromFS(os.DirFS(dir), "."), migrate.WithTable("app_migrations"))
	if _, err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range cluster.IDs() {
		if n := cluster.Count(t, id, "SELECT version FROM app_migrations"); n != 3 {
			t.Errorf("%s: app_migrations says version %d", id, n)
		}
		if n := cluster.Count(t, id, "SELECT count(*) FROM pg_class WHERE relname = 'schema_migrations'"); n != 0 {
			t.Errorf("%s: the default table was used as well", id)
		}
	}
}

func TestClusterWithMigrations(t *testing.T) {
	cluster := shardtest.NewCluster(t, 2, shardtest.WithMigrations(write(t, migrations)))
	for _, id := range cluster.IDs() {
		if n := cluster.Count(t, id, "SELECT count(*) FROM countries"); n != 2 {
			t.Errorf("%s has %d countries, want the migrated 2", id, n)
		}
	}
}

func TestFirstMigrationFailingLeavesTheShardEmptyAndRerunnable(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 2)
	ids := cluster.IDs()
	mustExec(t, cluster, ids[1], "CREATE TABLE profiles (junk int)")

	runner := cluster.Migrator(t, migrate.FromURL(write(t, migrations)), migrate.WithPolicy(migrate.ContinueOnFailure))
	if _, err := runner.Up(ctx); !errors.Is(err, migrate.ErrFailed) {
		t.Fatalf("error = %v", err)
	}
	st, _ := runner.Status(ctx)
	if got := st.Shards[ids[1]].State; got.Applied || got.Dirty {
		t.Errorf("%s = %v, want clean with nothing applied", ids[1], got)
	}

	if _, err := cluster.Direct(t, ids[1]).Exec("DROP TABLE profiles"); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Up(ctx); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if st, _ := runner.Status(ctx); !st.UpToDate() {
		t.Errorf("after the re-run:\n%s", st)
	}
}
