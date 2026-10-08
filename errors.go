package shard

import (
	"errors"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/merge"
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

	// ErrCrossShardTx is returned when a statement inside a transaction belongs
	// to a different shard from the transaction's, or to several. Nothing is
	// sent to any shard, and the transaction remains usable.
	ErrCrossShardTx = errors.New("shard: statement is outside the transaction's shard")

	// ErrMergeLimitExceeded is returned when merging the results of several
	// shards would hold more than Config.MaxMergeRows groups or distinct rows.
	ErrMergeLimitExceeded = merge.ErrLimitExceeded

	// ErrUnknownShard is returned when WithShard names a shard that is not
	// configured.
	ErrUnknownShard = exec.ErrUnknownShard

	// ErrUnsupportedQuery is returned for SQL the library cannot route safely,
	// or cannot run yet. The message names the construct and what to do instead.
	ErrUnsupportedQuery = analyze.ErrUnsupportedQuery

	// ErrMissingArgument is returned when the SQL uses a parameter, such as $3,
	// that has no argument.
	ErrMissingArgument = analyze.ErrMissingArgument

	// ErrShardKeyImmutable is returned for a statement that would change a
	// row's shard key: an UPDATE that sets it, or an INSERT ... ON CONFLICT DO
	// UPDATE that does. To move a row, delete it and insert it with the new key.
	ErrShardKeyImmutable = plan.ErrShardKeyImmutable

	// ErrIdempotencyKeyReused is returned when an idempotency key that was used
	// for one statement is used for a different one.
	ErrIdempotencyKeyReused = exec.ErrIdempotencyKeyReused

	// ErrIdempotencyTableMissing is returned when an idempotency key is used but
	// the table that records keys does not exist on a shard. Create it with
	// DB.EnsureIdempotencyTable.
	ErrIdempotencyTableMissing = exec.ErrIdempotencyTableMissing
)

// ShardError reports which shard a failure came from. Use errors.As to read it.
type ShardError = exec.ShardError
