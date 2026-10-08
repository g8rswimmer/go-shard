package exec

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/router"
)

// DefaultMaxFanout bounds how many shards one statement runs on at once when
// Options.MaxFanout is not set.
const DefaultMaxFanout = 8

// Options configure an Executor.
type Options struct {
	// ShardTimeout limits each shard's work, including streaming its rows.
	// Zero means no limit beyond the caller's context.
	ShardTimeout time.Duration
	// MaxFanout bounds concurrent shard calls per statement. Zero uses
	// DefaultMaxFanout.
	MaxFanout int
}

// Executor runs plans on the shards of a Pool.
type Executor struct {
	pool      *Pool
	timeout   time.Duration
	maxFanout int
}

// NewExecutor returns an Executor for the pool.
func NewExecutor(pool *Pool, opts Options) *Executor {
	e := &Executor{pool: pool, timeout: opts.ShardTimeout, maxFanout: opts.MaxFanout}
	if e.maxFanout <= 0 {
		e.maxFanout = DefaultMaxFanout
	}
	return e
}

// Rows is one shard's result set. Closing it also releases the per-shard
// timeout, so it must always be closed.
type Rows struct {
	*sql.Rows
	cancel context.CancelFunc
}

// Close closes the result set and releases its context.
func (r *Rows) Close() error {
	err := r.Rows.Close()
	r.cancel()
	return err
}

// ShardRows is the result of a query on one shard.
type ShardRows struct {
	Shard router.ShardID
	Rows  *Rows
}

// ShardExec is the outcome of a statement on one shard.
type ShardExec struct {
	Shard  router.ShardID
	Result sql.Result // nil when Err is set
	Err    error
}

// Query runs the plan's SQL on every target in parallel, at most MaxFanout at
// a time. It fails fast: the first error cancels the shards still running,
// closes any result sets already open, and is returned as a *ShardError. On
// success the caller must close every returned Rows.
func (e *Executor) Query(ctx context.Context, p plan.Plan) ([]ShardRows, error) {
	conns, err := e.conns(p.Targets)
	if err != nil {
		return nil, err
	}

	failCtx, fail := context.WithCancel(ctx)
	defer fail()

	var (
		once  sync.Once
		first error
	)
	setErr := func(err error) { once.Do(func() { first = err; fail() }) }

	results := make([]ShardRows, len(p.Targets))
	var g errgroup.Group
	g.SetLimit(e.maxFanout)
	for i, id := range p.Targets {
		if failCtx.Err() != nil {
			break
		}
		g.Go(func() error {
			sctx, cancel := e.shardContext(ctx)
			// Cancel this shard's query if another shard fails first. The hook
			// is removed once the query returns, so failCtx being cancelled when
			// Query exits does not cut off rows the caller is still reading.
			stop := context.AfterFunc(failCtx, cancel)
			rows, err := conns[i].QueryContext(sctx, p.SQL, p.Args...)
			stop()
			if err != nil {
				cancel()
				setErr(&ShardError{Shard: id, Err: err})
				return nil
			}
			results[i] = ShardRows{Shard: id, Rows: &Rows{Rows: rows, cancel: cancel}}
			return nil
		})
	}
	_ = g.Wait() // goroutines record errors themselves; they never return one

	switch {
	case first != nil:
		closeAll(results)
		return nil, first
	case ctx.Err() != nil:
		closeAll(results)
		return nil, ctx.Err()
	default:
		return results, nil
	}
}

// Exec runs the plan's SQL on every target in parallel, at most MaxFanout at a
// time. Unlike Query it does not fail fast: every shard is attempted and its
// outcome reported, because writes that succeeded on some shards must be
// visible to the caller. Outcomes are in the order of p.Targets.
func (e *Executor) Exec(ctx context.Context, p plan.Plan) ([]ShardExec, error) {
	conns, err := e.conns(p.Targets)
	if err != nil {
		return nil, err
	}

	out := make([]ShardExec, len(p.Targets))
	var g errgroup.Group
	g.SetLimit(e.maxFanout)
	for i, id := range p.Targets {
		g.Go(func() error {
			sctx, cancel := e.shardContext(ctx)
			defer cancel()
			res, err := conns[i].ExecContext(sctx, p.SQL, p.Args...)
			out[i] = ShardExec{Shard: id, Result: res, Err: err}
			return nil
		})
	}
	_ = g.Wait()
	return out, nil
}

// conns resolves targets to connections, rejecting unknown or repeated shards
// before anything runs.
func (e *Executor) conns(targets []router.ShardID) ([]Conn, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("exec: plan has no target shards")
	}
	seen := make(map[router.ShardID]bool, len(targets))
	conns := make([]Conn, len(targets))
	for i, id := range targets {
		c, ok := e.pool.Conn(id)
		switch {
		case !ok:
			return nil, fmt.Errorf("%w: %q", ErrUnknownShard, id)
		case seen[id]:
			return nil, fmt.Errorf("exec: plan targets shard %q more than once", id)
		default:
			seen[id] = true
			conns[i] = c
		}
	}
	return conns, nil
}

func (e *Executor) shardContext(parent context.Context) (context.Context, context.CancelFunc) {
	if e.timeout > 0 {
		return context.WithTimeout(parent, e.timeout)
	}
	return context.WithCancel(parent)
}

func closeAll(results []ShardRows) {
	for _, r := range results {
		if r.Rows != nil {
			_ = r.Rows.Close()
		}
	}
}
