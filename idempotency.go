package shard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/plan"
)

type idempotencyKey struct{}

// WithIdempotencyKey returns a context that makes writes safe to retry.
//
// Exec and ExecStatement called with this context record the key on each shard,
// in the same transaction as the write. If the call is repeated with the same
// key, a shard that already applied the write does not apply it again and
// reports what the first attempt did (ShardOutcome.Replayed); a shard where the
// write failed or never ran applies it now. After a partial failure, calling
// again with the same key finishes the job without duplicating anything.
//
//	ctx = shard.WithIdempotencyKey(ctx, "order-8841")
//	res, err := db.Exec(ctx, "INSERT INTO ...", ...)
//	// if err != nil, call again with the same ctx and the same statement
//
// Choose one key per logical operation. Using a key for a different statement
// or different arguments is an error (ErrIdempotencyKeyReused): the statement
// text and arguments are compared, so a retry must send exactly what the first
// attempt sent. A context that carries a key should be used for that one write;
// reads (Query) ignore it.
//
// Keys are recorded in the table named by Config.IdempotencyTable, which must
// exist on every shard (see EnsureIdempotencyTable). Remove old keys with
// PruneIdempotencyKeys.
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, idempotencyKey{}, key)
}

// IdempotencyKey returns the key set by WithIdempotencyKey.
func IdempotencyKey(ctx context.Context) (string, bool) {
	k, ok := ctx.Value(idempotencyKey{}).(string)
	return k, ok
}

// IdempotencyDDL returns the statements that create the table idempotency keys
// are recorded in, for use in your own migrations. table is a name or
// schema.name; pass "" for the default.
func IdempotencyDDL(table string) string {
	if table == "" {
		table = exec.DefaultIdempotencyTable
	}
	return exec.IdempotencyDDL(table)
}

// EnsureIdempotencyTable creates the idempotency table on every shard if it is
// not there. It is safe to call repeatedly. If you manage your schema with
// migrations, add IdempotencyDDL to them instead.
func (db *DB) EnsureIdempotencyTable(ctx context.Context) error {
	p := plan.Plan{SQL: exec.IdempotencyDDL(db.idemTable), Targets: db.pool.IDs(), Strategy: plan.All}
	outcomes, err := db.exec.Exec(ctx, p)
	if err != nil {
		return err
	}
	var errs []error
	for _, o := range outcomes {
		if o.Err != nil {
			errs = append(errs, &ShardError{Shard: o.Shard, Err: o.Err})
		}
	}
	return errors.Join(errs...)
}

// PruneIdempotencyKeys removes keys recorded more than olderThan ago, on every
// shard, and returns how many were removed. Keep keys at least as long as a
// retry can happen. Run it periodically.
func (db *DB) PruneIdempotencyKeys(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan < 0 {
		return 0, fmt.Errorf("shard: olderThan cannot be negative")
	}
	p := plan.Plan{
		SQL:      exec.PruneSQL(db.idemTable),
		Args:     []any{time.Now().Add(-olderThan)},
		Targets:  db.pool.IDs(),
		Strategy: plan.All,
	}
	outcomes, err := db.exec.Exec(ctx, p)
	if err != nil {
		return 0, err
	}
	var removed int64
	var errs []error
	for _, o := range outcomes {
		if o.Err != nil {
			errs = append(errs, &ShardError{Shard: o.Shard, Err: o.Err})
			continue
		}
		removed += o.RowsAffected
	}
	return removed, errors.Join(errs...)
}
