package shard

import (
	"errors"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/plan"
)

var (
	// ErrInvalidConfig is wrapped by every error from Open caused by a bad
	// Config. The message lists every problem found.
	ErrInvalidConfig = errors.New("shard: invalid config")

	// ErrShardKeyRequired is returned when a statement does not say which shard
	// it belongs to: no usable condition on the shard key, and no WithShardKey,
	// WithShard or WithAllShards.
	ErrShardKeyRequired = plan.ErrShardKeyRequired

	// ErrCrossShardJoin is returned when a statement combines tables that are
	// not guaranteed to be on the same shard: tables in different colocation
	// groups, or tables not tied together by their shard key.
	ErrCrossShardJoin = plan.ErrCrossShardJoin

	// ErrUnknownTable is returned for a table that is not in the registry.
	ErrUnknownTable = plan.ErrUnknownTable

	// ErrUnknownShard is returned when WithShard names a shard that is not
	// configured.
	ErrUnknownShard = exec.ErrUnknownShard

	// ErrUnsupportedQuery is returned for SQL the library cannot route safely,
	// or cannot run yet. The message names the construct and what to do instead.
	ErrUnsupportedQuery = analyze.ErrUnsupportedQuery

	// ErrMissingArgument is returned when the SQL uses a parameter, such as $3,
	// that has no argument.
	ErrMissingArgument = analyze.ErrMissingArgument
)

// ShardError reports which shard a failure came from. Use errors.As to read it.
type ShardError = exec.ShardError
