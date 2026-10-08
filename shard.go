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
// Statements built with package query carry their routing and need no SQL
// parsing.
package shard

import (
	"context"
	"sync/atomic"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/exec"
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
	next      atomic.Uint64    // spreads statements any shard can answer
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

	analyzer := cfg.Analyzer
	if analyzer == nil {
		analyzer = defaultAnalyzer()
	}
	idemTable := cfg.IdempotencyTable
	if idemTable == "" {
		idemTable = exec.DefaultIdempotencyTable
	}
	return &DB{
		idemTable: idemTable,
		registry:  cfg.Registry,
		router:    r,
		pool:      pool,
		exec:      exec.NewExecutor(pool, exec.Options{ShardTimeout: cfg.ShardTimeout, MaxFanout: cfg.MaxFanout}),
		analyzer:  analyzer,
	}, nil
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
