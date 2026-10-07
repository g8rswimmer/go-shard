package shard

import (
	"errors"

	"github.com/g8rswimmer/go-shard/exec"
)

var (
	// ErrInvalidConfig is wrapped by every error from Open caused by a bad
	// Config. The message lists every problem found.
	ErrInvalidConfig = errors.New("shard: invalid config")

	// ErrShardKeyRequired is returned when a statement cannot be routed because
	// no shard key or target was given.
	ErrShardKeyRequired = errors.New("shard: no shard key")

	// ErrUnknownShard is returned when WithShard names a shard that is not
	// configured.
	ErrUnknownShard = exec.ErrUnknownShard

	// ErrUnsupportedQuery is returned for a statement the library cannot run
	// yet or does not support.
	ErrUnsupportedQuery = errors.New("shard: unsupported query")
)

// ShardError reports which shard a failure came from. Use errors.As to read it.
type ShardError = exec.ShardError
