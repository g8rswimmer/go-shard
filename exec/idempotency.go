package exec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/router"
)

// DefaultIdempotencyTable is the table idempotency keys are recorded in, on
// every shard.
const DefaultIdempotencyTable = "go_shard_idempotency_keys"

var (
	// ErrIdempotencyKeyReused is returned when a key that was used for one
	// statement is used for a different one.
	ErrIdempotencyKeyReused = errors.New("shard: idempotency key reused for a different statement")

	// ErrIdempotencyTableMissing is returned when idempotency keys are used but
	// the table that records them does not exist on a shard.
	ErrIdempotencyTableMissing = errors.New("shard: idempotency table is missing")
)

// Idempotency asks for a write to be applied at most once per shard.
type Idempotency struct {
	// Key identifies the operation. Retrying with the same key applies the
	// write only on shards where it has not already been applied.
	Key string
	// Table is where keys are recorded.
	Table string
}

var tableNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$`)

// ValidTableName reports whether name is a table name, or schema.table, that
// can be used for the idempotency table.
func ValidTableName(name string) bool { return tableNameRE.MatchString(name) }

// quoteTable quotes a name or schema.name that passed ValidTableName.
func quoteTable(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = `"` + p + `"`
	}
	return strings.Join(parts, ".")
}

// IdempotencyDDL returns the statements that create the idempotency table. Run
// them on every shard, for example from your migrations, or call
// DB.EnsureIdempotencyTable.
func IdempotencyDDL(table string) string {
	base := table[strings.LastIndex(table, ".")+1:]
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
    "key"           text PRIMARY KEY,
    request_hash    text NOT NULL,
    rows_affected   bigint NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "%[2]s_created_at_idx" ON %[1]s (created_at)`, quoteTable(table), base)
}

// PruneSQL returns a DELETE that removes keys recorded before the time given
// as its only argument.
func PruneSQL(table string) string {
	return `DELETE FROM ` + quoteTable(table) + ` WHERE created_at < $1`
}

// requestHash identifies a statement and its arguments, so a key used for a
// different statement can be told from a retry of the same one.
func requestHash(sql string, args []any) string {
	h := sha256.New()
	h.Write([]byte(sql))
	for _, a := range args {
		h.Write([]byte{0})
		h.Write([]byte(canonicalArg(a)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// canonicalArg renders an argument the same way on every run. Values whose
// default formatting varies (times carry a monotonic clock reading) are
// normalised.
func canonicalArg(a any) string {
	switch v := a.(type) {
	case nil:
		return "nil"
	case time.Time:
		return "time:" + v.UTC().Format(time.RFC3339Nano)
	case []byte:
		return "bytes:" + hex.EncodeToString(v)
	case fmt.Stringer:
		return fmt.Sprintf("%T:%s", a, v.String())
	default:
		return fmt.Sprintf("%T:%v", a, a)
	}
}

// ExecIdempotent is Exec with an idempotency key: on each shard, the key is
// recorded in the same transaction as the write. If the key is already there
// the write is not repeated, and the outcome reports what the first attempt
// did. If the write fails, nothing is recorded and a retry applies it.
func (e *Executor) ExecIdempotent(ctx context.Context, p plan.Plan, idem Idempotency) ([]ShardExec, error) {
	return e.exec(ctx, p, &idem)
}

// runIdempotent applies one shard's write at most once for a key.
func runIdempotent(ctx context.Context, conn Conn, id router.ShardID, query string, args []any, idem Idempotency) (affected int64, replayed bool, err error) {
	table := quoteTable(idem.Table)
	hash := requestHash(query, args)

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer func() {
		if err != nil || replayed {
			_ = tx.Rollback()
		}
	}()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO `+table+` ("key", request_hash) VALUES ($1, $2) ON CONFLICT ("key") DO NOTHING`, idem.Key, hash)
	if err != nil {
		return 0, false, tableMissing(err, idem.Table)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// The key is already recorded: this shard has applied the write before.
		var storedHash string
		var stored int64
		err = tx.QueryRowContext(ctx, `SELECT request_hash, rows_affected FROM `+table+` WHERE "key" = $1`, idem.Key).Scan(&storedHash, &stored)
		switch {
		case err != nil:
			return 0, false, err
		case storedHash != hash:
			return 0, false, fmt.Errorf("%w: %q on %s was used for another statement or other arguments", ErrIdempotencyKeyReused, idem.Key, id)
		default:
			return stored, true, nil
		}
	}

	wres, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, false, err
	}
	affected, _ = wres.RowsAffected()
	if _, err = tx.ExecContext(ctx, `UPDATE `+table+` SET rows_affected = $2 WHERE "key" = $1`, idem.Key, affected); err != nil {
		return 0, false, err
	}
	if err = tx.Commit(); err != nil {
		return 0, false, err
	}
	return affected, false, nil
}

// tableMissing turns "relation does not exist" into an error that says what to do.
func tableMissing(err error, table string) error {
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "42P01" {
		return fmt.Errorf("%w: %q does not exist on this shard; create it with DB.EnsureIdempotencyTable, or add this to your migrations:\n%s",
			ErrIdempotencyTableMissing, table, IdempotencyDDL(table))
	}
	return err
}
