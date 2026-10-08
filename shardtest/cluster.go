// Package shardtest provides test support for code that uses go-shard: a
// throwaway cluster of real PostgreSQL shards, seeding helpers, and direct
// access to each shard for assertions.
//
//	cluster := shardtest.NewCluster(t, 3)
//	cluster.ExecAll(t, "CREATE TABLE profiles (id bigint PRIMARY KEY, name text)")
//	db := cluster.Open(t, reg)
//
// By default NewCluster starts one PostgreSQL container per shard (Docker is
// required) and removes them when the test ends. To use PostgreSQL instances
// you already run, for example in CI, set SHARDTEST_DSNS to a comma-separated
// list of connection strings:
//
//	SHARDTEST_DSNS='postgres://shard:shard@localhost:5441/shard,postgres://shard:shard@localhost:5442/shard,...'
//
// WARNING: with SHARDTEST_DSNS set, NewCluster drops and recreates the public
// schema of every database it is given at the start of each test. Only point
// it at databases that exist for testing.
//
// Tests that share SHARDTEST_DSNS must not run in parallel with each other,
// including across packages: use go test -p 1.
package shardtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/registry"
)

const (
	envDSNs    = "SHARDTEST_DSNS"
	envVersion = "SHARDTEST_POSTGRES_VERSION"

	// DefaultPostgresVersion is the oldest supported PostgreSQL release.
	DefaultPostgresVersion = "14"

	setupTimeout = 2 * time.Minute
)

// Option customizes NewCluster.
type Option func(*options)

type options struct {
	pgVersion string
}

// WithPostgresVersion sets the PostgreSQL image tag for containers. The default
// is SHARDTEST_POSTGRES_VERSION, or DefaultPostgresVersion if that is unset.
// It has no effect when SHARDTEST_DSNS is set.
func WithPostgresVersion(v string) Option { return func(o *options) { o.pgVersion = v } }

// Cluster is a set of empty PostgreSQL shards for one test.
type Cluster struct {
	ids  []shard.ShardID
	dsns map[shard.ShardID]string

	mu     sync.Mutex
	direct map[shard.ShardID]*sql.DB
}

// NewCluster provides n empty shards named shard-01, shard-02, ... and cleans
// up when the test ends. It fails the test if the shards cannot be started.
func NewCluster(t testing.TB, n int, opts ...Option) *Cluster {
	t.Helper()
	if n < 1 {
		t.Fatalf("shardtest: NewCluster needs at least one shard, got %d", n)
	}
	o := options{pgVersion: DefaultPostgresVersion}
	if v := os.Getenv(envVersion); v != "" {
		o.pgVersion = v
	}
	for _, opt := range opts {
		opt(&o)
	}

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()

	env := os.Getenv(envDSNs)
	external := env != ""
	var dsns []string
	switch {
	case external:
		var err error
		if dsns, err = externalDSNs(env, n); err != nil {
			t.Fatalf("shardtest: %v", err)
		}
		t.Logf("shardtest: using %d existing shards from %s; resetting their public schema", n, envDSNs)
	default:
		dsns = startContainers(ctx, t, n, o.pgVersion)
	}

	c := &Cluster{dsns: map[shard.ShardID]string{}, direct: map[shard.ShardID]*sql.DB{}}
	for i, dsn := range dsns {
		id := shard.ShardID(fmt.Sprintf("shard-%02d", i+1))
		c.ids = append(c.ids, id)
		c.dsns[id] = dsn
	}
	t.Cleanup(c.closeDirect)

	if external {
		for _, id := range c.ids {
			if _, err := c.Direct(t, id).ExecContext(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
				t.Fatalf("shardtest: resetting %s: %v", id, err)
			}
		}
	}
	return c
}

// externalDSNs parses SHARDTEST_DSNS and returns the first n entries.
func externalDSNs(env string, n int) ([]string, error) {
	var dsns []string
	for _, d := range strings.Split(env, ",") {
		if d = strings.TrimSpace(d); d != "" {
			dsns = append(dsns, d)
		}
	}
	if len(dsns) < n {
		return nil, fmt.Errorf("%s lists %d databases but the test needs %d", envDSNs, len(dsns), n)
	}
	return dsns[:n], nil
}

// startContainers starts n PostgreSQL containers in parallel.
func startContainers(ctx context.Context, t testing.TB, n int, version string) []string {
	t.Helper()
	type result struct {
		ctr *postgres.PostgresContainer
		dsn string
		err error
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctr, err := postgres.Run(ctx, "postgres:"+version,
				postgres.WithDatabase("shard"),
				postgres.WithUsername("shard"),
				postgres.WithPassword("shard"),
				postgres.BasicWaitStrategies(),
			)
			results[i].ctr = ctr
			if err != nil {
				results[i].err = err
				return
			}
			results[i].dsn, results[i].err = ctr.ConnectionString(ctx, "sslmode=disable")
		}()
	}
	wg.Wait()

	dsns := make([]string, n)
	var errs []error
	for i, r := range results {
		if r.ctr != nil {
			testcontainers.CleanupContainer(t, r.ctr)
		}
		if r.err != nil {
			errs = append(errs, fmt.Errorf("shard %d: %w", i+1, r.err))
		}
		dsns[i] = r.dsn
	}
	if err := errors.Join(errs...); err != nil {
		t.Fatalf("shardtest: starting postgres:%s containers (is Docker running?): %v", version, err)
	}
	return dsns
}

// IDs returns the shard IDs in order: shard-01, shard-02, ...
func (c *Cluster) IDs() []shard.ShardID { return append([]shard.ShardID(nil), c.ids...) }

// DSN returns the connection string for a shard.
func (c *Cluster) DSN(id shard.ShardID) string { return c.dsns[id] }

// Config returns a shard.Config for the cluster with buckets split evenly.
// Adjust the result before passing it to shard.Open if a test needs to.
func (c *Cluster) Config(reg *registry.Registry) shard.Config {
	cfg := shard.Config{Registry: reg}
	for _, id := range c.ids {
		cfg.Shards = append(cfg.Shards, shard.ShardConfig{ID: id, DSN: c.dsns[id]})
	}
	return cfg
}

// Open opens a shard.DB on the cluster and closes it when the test ends.
func (c *Cluster) Open(t testing.TB, reg *registry.Registry) *shard.DB {
	t.Helper()
	db, err := shard.Open(context.Background(), c.Config(reg))
	if err != nil {
		t.Fatalf("shardtest: opening DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Direct returns a plain connection to one shard, bypassing go-shard. Use it
// to assert where data physically is. It is closed when the test ends.
func (c *Cluster) Direct(t testing.TB, id shard.ShardID) *sql.DB {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if db, ok := c.direct[id]; ok {
		return db
	}
	dsn, ok := c.dsns[id]
	if !ok {
		t.Fatalf("shardtest: no shard %q (have %v)", id, c.ids)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("shardtest: connecting to %s: %v", id, err)
	}
	c.direct[id] = db
	return db
}

func (c *Cluster) closeDirect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, db := range c.direct {
		_ = db.Close()
	}
}

// ExecAll runs a statement, typically DDL, directly on every shard.
func (c *Cluster) ExecAll(t testing.TB, query string, args ...any) {
	t.Helper()
	for _, id := range c.ids {
		if _, err := c.Direct(t, id).ExecContext(context.Background(), query, args...); err != nil {
			t.Fatalf("shardtest: %s: %v", id, err)
		}
	}
}

// Count runs a query that returns one integer, such as SELECT count(*), on one
// shard and returns it.
func (c *Cluster) Count(t testing.TB, id shard.ShardID, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := c.Direct(t, id).QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("shardtest: %s: %v", id, err)
	}
	return n
}

// Seed inserts rows into a table registered in db's registry, each on the shard
// that owns it: sharded and colocated rows by their shard key column, global
// rows on every shard. Each row maps column names to values.
func (c *Cluster) Seed(t testing.TB, db *shard.DB, table string, rows ...map[string]any) {
	t.Helper()
	tbl, ok := db.Registry().Table(table)
	if !ok {
		t.Fatalf("shardtest: table %q is not in the registry", table)
	}
	for i, row := range rows {
		q := insertSQL(table, row)
		args := insertArgs(row)
		var err error
		switch key, hasKey := row[tbl.KeyCol]; {
		case tbl.KeyCol == "": // global table
			_, err = db.WithAllShards().Exec(context.Background(), q, args...)
		case !hasKey:
			t.Fatalf("shardtest: row %d for %q has no shard key column %q", i, table, tbl.KeyCol)
		default:
			_, err = db.WithShardKey(key).Exec(context.Background(), q, args...)
		}
		if err != nil {
			t.Fatalf("shardtest: seeding row %d of %q: %v", i, table, err)
		}
	}
}
