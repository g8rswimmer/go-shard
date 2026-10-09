package exec

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/router"
)

// ExplainShards asks every target for PostgreSQL's own plan of the statement it
// would run, in parallel and at most MaxFanout at a time, and returns the text
// by shard. It stops at the first failure and returns it as a *ShardError.
//
// With analyze, PostgreSQL runs the statement to measure it. That is done in a
// transaction that is rolled back, so an INSERT, UPDATE or DELETE leaves no
// rows behind; side effects that a rollback does not undo (a sequence advancing,
// a trigger that calls out) still happen.
func (e *Executor) ExplainShards(ctx context.Context, p plan.Plan, analyze bool) (map[router.ShardID]string, error) {
	conns, err := e.conns(p.Targets)
	if err != nil {
		return nil, err
	}

	var (
		mu  sync.Mutex
		out = make(map[router.ShardID]string, len(p.Targets))
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(e.maxFanout)
	for i, id := range p.Targets {
		g.Go(func() error {
			sctx, cancel := e.shardContext(gctx)
			defer cancel()
			query, args := p.StatementFor(id)
			text, err := explainOne(sctx, conns[i], query, args, analyze)
			if err != nil {
				return &ShardError{Shard: id, Err: err}
			}
			mu.Lock()
			out[id] = text
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// queryer is what both a connection pool and a transaction offer.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// explainOne runs EXPLAIN for one statement on one shard.
func explainOne(ctx context.Context, c Conn, query string, args []any, analyze bool) (string, error) {
	prefix := "EXPLAIN "
	var q queryer = c
	if analyze {
		tx, err := c.BeginTx(ctx, nil)
		if err != nil {
			return "", err
		}
		defer func() { _ = tx.Rollback() }() // never commit: ANALYZE really runs the statement
		prefix = "EXPLAIN (ANALYZE) "
		q = tx
	}
	return readPlan(ctx, q, prefix+query, args)
}

// readPlan runs an EXPLAIN and joins its lines.
func readPlan(ctx context.Context, q queryer, query string, args []any) (string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", fmt.Errorf("reading the EXPLAIN output: %w", err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(lines, "\n"), nil
}

// Explain returns PostgreSQL's plan of a statement run in this transaction. It
// never uses ANALYZE, which would run the statement inside the transaction.
func (t *Tx) Explain(ctx context.Context, query string, args []any) (string, error) {
	ctx, cancel := t.statementContext(ctx)
	defer cancel()
	text, err := readPlan(ctx, t.tx, "EXPLAIN "+query, args)
	if err != nil {
		return "", &ShardError{Shard: t.shard, Err: err}
	}
	return text, nil
}
