// Package shard is a library for maintaining a sharded PostgreSQL database.
//
// It routes each statement to the shard that owns its shard key, and applies
// the same migrations to every shard. See docs/REQUIREMENTS.md and
// docs/ARCHITECTURE.md.
//
// Open a DB and run SQL. The library reads the shard key from the statement
// and runs it on the shard that owns it:
//
//	db, err := shard.Open(ctx, cfg)
//	...
//	rows, err := db.Query(ctx, "SELECT name FROM profiles WHERE id = $1", id)
//
// A statement it cannot place is refused, not guessed: no condition on the
// shard key gives ErrShardKeyRequired, and tables not tied together by their
// shard key give ErrCrossShardJoin. You can always say where a statement goes:
//
//	db.WithShardKey(id).Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, $2)", id, name)
//	db.WithShard("shard-02").Query(ctx, "SELECT ...")
//	db.WithAllShards().Exec(ctx, "CREATE TABLE ...")
//
// A query that runs on several shards (WithAllShards, or key conditions that
// name more than one) is merged into one result, as a single database would
// give it: ORDER BY, LIMIT, OFFSET, DISTINCT, aggregates, GROUP BY and HAVING.
// Constructs that cannot be merged are refused with ErrUnsupportedQuery. Use
// AllowPartial to tolerate shards that do not answer.
//
//	rows, err := db.WithAllShards().Query(ctx,
//	    "SELECT team, count(*) FROM players GROUP BY team ORDER BY 2 DESC")
//
// Statements built with package query carry their routing and need no SQL
// parsing.
//
// Explain says where a statement would run, and why, without running it; it
// returns the same refusals running the statement would. In a unit test,
// package shardtest's NewFake answers the same question with no database.
//
//	e, err := db.Explain(ctx, "SELECT name FROM profiles WHERE id = $1", id)
//	fmt.Println(e) // strategy, targets, reason, shard SQL, merge steps
//
// Config.Hooks (package observe) reports every statement's routing, each
// shard's work, the merge and the outcome, for logs (observe.Slog), traces and
// metrics (observe/otel). Health and PoolStats report the shards and their
// connection pools.
//
// Migrations returns a migrate.Runner for the configured shards, which applies
// the same versioned migrations to every shard and reports drift.
//
//	runner, err := db.Migrations(migrate.FromURL("file://./migrations"))
//	res, err := runner.Up(ctx)
package shard

import (
	"context"
	"database/sql"
	"sync/atomic"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/migrate"
	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
)

// DB is a set of shards. It is safe for concurrent use. Close it when done.
type DB struct {
	registry  *registry.Registry
	router    *router.HashRouter
	pool      *exec.Pool
	exec      *exec.Executor
	analyzer  analyze.Analyzer // nil when raw SQL cannot be analyzed (no cgo)
	idemTable string           // where idempotency keys are recorded
	maxMerge  int              // rows or groups a merge may hold
	next      atomic.Uint64    // spreads statements any shard can answer
	shards    []migrate.Shard  // for Migrations
	hooks     observe.Hooks    // never nil
}

var _ Querier = (*DB)(nil)

// Open validates the config, connects to every shard and pings it. If any
// shard cannot be reached, nothing is left open and the error names the shard.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	r, err := cfg.validate()
	if err != nil {
		return nil, err
	}

	specs := make([]exec.Spec, len(cfg.Shards))
	for i, s := range cfg.Shards {
		maxConns := s.MaxConns
		if maxConns == 0 {
			maxConns = defaultMaxConns
		}
		specs[i] = exec.Spec{ID: s.ID, DSN: s.DSN, MaxConns: maxConns}
	}
	pool, err := exec.Open(ctx, specs)
	if err != nil {
		return nil, err
	}

	ex := exec.NewExecutor(pool, exec.Options{ShardTimeout: cfg.ShardTimeout, MaxFanout: cfg.MaxFanout, Hooks: cfg.Hooks})
	return newDB(cfg, r, pool, ex), nil
}

// newDB applies the config's defaults. ex is nil for a Planner, which never
// runs anything.
func newDB(cfg Config, r *router.HashRouter, pool *exec.Pool, ex *exec.Executor) *DB {
	analyzer := cfg.Analyzer
	if analyzer == nil {
		analyzer = defaultAnalyzer()
	}
	idemTable := cfg.IdempotencyTable
	if idemTable == "" {
		idemTable = exec.DefaultIdempotencyTable
	}
	maxMerge := cfg.MaxMergeRows
	if maxMerge == 0 {
		maxMerge = defaultMaxMergeRows
	}
	hooks := cfg.Hooks
	if hooks == nil {
		hooks = observe.Nop{}
	}
	shards := make([]migrate.Shard, len(cfg.Shards))
	for i, sc := range cfg.Shards {
		shards[i] = migrate.Shard{ID: sc.ID, DSN: sc.DSN}
	}
	return &DB{
		shards:    shards,
		hooks:     hooks,
		idemTable: idemTable,
		maxMerge:  maxMerge,
		registry:  cfg.Registry,
		router:    r,
		pool:      pool,
		exec:      ex,
		analyzer:  analyzer,
	}
}

// anyShard picks the shard for a statement every shard can answer, such as a
// read of a global table, taking turns so no shard carries them all.
func (db *DB) anyShard() ShardID {
	ids := db.pool.IDs()
	return ids[(db.next.Add(1)-1)%uint64(len(ids))]
}

// Close closes every shard's connection pool.
func (db *DB) Close() error { return db.pool.Close() }

// Registry returns the table registry the DB was opened with.
func (db *DB) Registry() *registry.Registry { return db.registry }

// Shards returns every shard ID, sorted.
func (db *DB) Shards() []ShardID { return db.pool.IDs() }

// ShardHealth is the health of one shard.
type ShardHealth = exec.ShardHealth

// Health pings every shard in parallel and returns one entry per shard, sorted
// by ID, with latency and connection pool statistics.
func (db *DB) Health(ctx context.Context) []ShardHealth { return db.pool.Health(ctx) }

// PoolStats returns each shard's connection pool statistics without contacting
// the shards (Health pings them). It is cheap enough to call on every scrape.
func (db *DB) PoolStats() map[ShardID]sql.DBStats { return db.pool.Stats() }
