// Package shard is a library for maintaining a sharded PostgreSQL database.
//
// It routes each statement to the shard that owns its shard key, and applies
// the same migrations to every shard. See docs/REQUIREMENTS.md and
// docs/ARCHITECTURE.md.
//
// Open a DB, then tell it where each statement belongs:
//
//	db, err := shard.Open(ctx, cfg)
//	...
//	_, err = db.WithShardKey(id).Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, $2)", id, name)
//	rows, err := db.WithShardKey(id).Query(ctx, "SELECT name FROM profiles WHERE id = $1", id)
//
// Routing from the SQL itself is added in a later milestone; until then a
// statement without WithShardKey, WithShard or WithAllShards fails with
// ErrShardKeyRequired.
package shard

import (
	"context"

	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
)

// DB is a set of shards. It is safe for concurrent use. Close it when done.
type DB struct {
	registry *registry.Registry
	router   *router.HashRouter
	pool     *exec.Pool
	exec     *exec.Executor
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

	return &DB{
		registry: cfg.Registry,
		router:   r,
		pool:     pool,
		exec:     exec.NewExecutor(pool, exec.Options{ShardTimeout: cfg.ShardTimeout, MaxFanout: cfg.MaxFanout}),
	}, nil
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
