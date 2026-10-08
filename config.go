package shard

import (
	"fmt"
	"strings"
	"time"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
)

// ShardID identifies a shard. IDs are stable names such as "shard-01"; they
// appear in errors, logs and Explain output.
type ShardID = router.ShardID

// Range returns the inclusive bucket range [from, to] for ShardConfig.Buckets.
func Range(from, to int) router.BucketRange { return router.Buckets(from, to) }

const defaultMaxConns = 10

// ShardConfig describes one shard.
type ShardConfig struct {
	ID ShardID
	// DSN is a PostgreSQL connection string. It is never logged or included in
	// errors.
	DSN string
	// MaxConns caps open connections to this shard. Zero uses 10.
	MaxConns int
	// Buckets are the virtual buckets (0-1023) this shard owns. Leave empty on
	// every shard to split the buckets evenly in the order the shards are
	// listed. Otherwise every shard must list its buckets, and together they
	// must cover 0-1023 exactly once.
	Buckets []router.BucketRange
}

// Config configures Open.
type Config struct {
	Shards []ShardConfig
	// Registry declares which tables are sharded, colocated or global.
	Registry *registry.Registry
	// MaxFanout bounds how many shards one statement runs on at once. Zero
	// uses 8.
	MaxFanout int
	// ShardTimeout limits each shard's work, including streaming its rows.
	// Zero means no limit beyond the caller's context.
	ShardTimeout time.Duration
	// Analyzer reads raw SQL to find where it belongs. The default uses
	// PostgreSQL's own parser, which needs cgo; without cgo the default is
	// none, and only WithShardKey / WithShard / WithAllShards and built
	// statements (package query) can route. See docs/BUILDING.md.
	Analyzer analyze.Analyzer
}

// validate checks the config and builds the router. The error wraps
// ErrInvalidConfig and lists every problem found.
func (c Config) validate() (*router.HashRouter, error) {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	if len(c.Shards) == 0 {
		add("at least one shard is required")
	}
	if c.Registry == nil {
		add("Registry is required: build one with registry.New")
	}
	if c.MaxFanout < 0 {
		add("MaxFanout cannot be negative")
	}
	if c.ShardTimeout < 0 {
		add("ShardTimeout cannot be negative")
	}

	seen := map[ShardID]bool{}
	withBuckets := 0
	for i, s := range c.Shards {
		switch {
		case s.ID == "":
			add("shard %d has no ID", i)
		case seen[s.ID]:
			add("shard %q is listed more than once", s.ID)
		default:
			seen[s.ID] = true
		}
		if s.DSN == "" {
			add("shard %q has no DSN", s.ID)
		}
		if s.MaxConns < 0 {
			add("shard %q: MaxConns cannot be negative", s.ID)
		}
		if len(s.Buckets) > 0 {
			withBuckets++
		}
	}

	var assignments []router.Assignment
	switch withBuckets {
	case 0:
		ids := make([]ShardID, len(c.Shards))
		for i, s := range c.Shards {
			ids[i] = s.ID
		}
		assignments = router.Even(ids...)
	case len(c.Shards):
		for _, s := range c.Shards {
			assignments = append(assignments, router.Assignment{Shard: s.ID, Buckets: s.Buckets})
		}
	default:
		add("%d of %d shards list Buckets: list them on every shard, or on none to split evenly", withBuckets, len(c.Shards))
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("%w:\n  - %s", ErrInvalidConfig, strings.Join(problems, "\n  - "))
	}
	r, err := router.New(assignments...)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return r, nil
}
