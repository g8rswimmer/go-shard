package exec

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/g8rswimmer/go-shard/router"
)

// Tx is a transaction on one shard. Every statement runs on the same
// connection until Commit or Rollback.
//
// The context given to Begin controls the transaction's life: cancelling it
// rolls the transaction back. The per-shard timeout applies to each statement,
// not to the transaction as a whole.
type Tx struct {
	shard   router.ShardID
	tx      *sql.Tx
	timeout time.Duration
}

// Begin starts a transaction on one shard.
func (e *Executor) Begin(ctx context.Context, id router.ShardID, opts *sql.TxOptions) (*Tx, error) {
	conn, ok := e.pool.Conn(id)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownShard, id)
	}
	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, &ShardError{Shard: id, Err: err}
	}
	return &Tx{shard: id, tx: tx, timeout: e.timeout}, nil
}

// Shard returns the shard the transaction runs on.
func (t *Tx) Shard() router.ShardID { return t.shard }

func (t *Tx) statementContext(parent context.Context) (context.Context, context.CancelFunc) {
	if t.timeout > 0 {
		return context.WithTimeout(parent, t.timeout)
	}
	return context.WithCancel(parent)
}

// Query runs a statement that returns rows. Close the rows before running the
// next statement: a transaction has one connection, which is busy until then.
func (t *Tx) Query(ctx context.Context, query string, args []any) (*Rows, error) {
	sctx, cancel := t.statementContext(ctx)
	rows, err := t.tx.QueryContext(sctx, query, args...)
	if err != nil {
		cancel()
		return nil, &ShardError{Shard: t.shard, Err: err}
	}
	return &Rows{Rows: rows, cancel: cancel}, nil
}

// Exec runs a statement and returns the rows it affected.
func (t *Tx) Exec(ctx context.Context, query string, args []any) (int64, error) {
	sctx, cancel := t.statementContext(ctx)
	defer cancel()
	res, err := t.tx.ExecContext(sctx, query, args...)
	if err != nil {
		return 0, &ShardError{Shard: t.shard, Err: err}
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// Commit commits the transaction. After it, further statements fail with
// sql.ErrTxDone.
func (t *Tx) Commit() error {
	if err := t.tx.Commit(); err != nil {
		return &ShardError{Shard: t.shard, Err: err}
	}
	return nil
}

// Rollback abandons the transaction. It returns sql.ErrTxDone, wrapped, if the
// transaction has already ended, so a deferred Rollback after Commit is harmless.
func (t *Tx) Rollback() error {
	if err := t.tx.Rollback(); err != nil {
		return &ShardError{Shard: t.shard, Err: err}
	}
	return nil
}
