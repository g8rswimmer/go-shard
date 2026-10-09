package exec

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/g8rswimmer/go-shard/observe"
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
	// Hooks hear about the work on each shard. Nil means none.
	Hooks observe.Hooks
}

// Executor runs plans on the shards of a Pool.
type Executor struct {
	pool      *Pool
	timeout   time.Duration
	maxFanout int
	hooks     shardHooks
}

// NewExecutor returns an Executor for the pool.
func NewExecutor(pool *Pool, opts Options) *Executor {
	e := &Executor{pool: pool, timeout: opts.ShardTimeout, maxFanout: opts.MaxFanout, hooks: shardHooks{opts.Hooks}}
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
	Shard router.ShardID
	// RowsAffected is zero when Err is set.
	RowsAffected int64
	// Replayed is true when an idempotency key showed the write had already
	// been applied on this shard, so it was not repeated. RowsAffected is then
	// what the first attempt reported.
	Replayed bool
	// Rows are the indexes of the INSERT's VALUES rows this shard received.
	Rows []int
	Err  error
}

// Query runs the plan's SQL on every target in parallel, at most MaxFanout at
// a time. It fails fast: the first error cancels the shards still running,
// closes any result sets already open, and is returned as a *ShardError. On
// success the caller must close every returned Rows.
func (e *Executor) Query(ctx context.Context, p plan.Plan) ([]ShardRows, error) {
	rows, _, err := e.query(ctx, p, false)
	return rows, err
}

// QueryPartial is Query that tolerates failing shards: every target is
// attempted, and the shards that could not start their query are returned as
// failures next to the rows of those that could. It fails only when no shard
// answered, or the context ended. The caller must close every returned Rows.
//
// Only a failure to start the query is tolerated. An error while a shard is
// streaming its rows surfaces when the rows are read.
func (e *Executor) QueryPartial(ctx context.Context, p plan.Plan) ([]ShardRows, []*ShardError, error) {
	return e.query(ctx, p, true)
}

func (e *Executor) query(ctx context.Context, p plan.Plan, partial bool) ([]ShardRows, []*ShardError, error) {
	conns, err := e.conns(p.Targets)
	if err != nil {
		return nil, nil, err
	}

	failCtx, fail := context.WithCancel(ctx)
	defer fail()

	var (
		mu     sync.Mutex
		failed []*ShardError
	)
	setErr := func(err *ShardError) {
		mu.Lock()
		defer mu.Unlock()
		failed = append(failed, err)
		if !partial {
			fail()
		}
	}

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
			query, args := p.StatementFor(id)
			hctx, began := e.hooks.start(sctx, observe.Query, id)
			rows, err := conns[i].QueryContext(hctx, query, args...)
			e.hooks.done(hctx, observe.Query, id, began, 0, false, err)
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

	if partial {
		// Report failures in plan order, not in the order they happened. (When
		// failing fast, the first error to happen is the one that matters.)
		sort.Slice(failed, func(i, j int) bool {
			return slices.Index(p.Targets, failed[i].Shard) < slices.Index(p.Targets, failed[j].Shard)
		})
	}

	switch {
	case ctx.Err() != nil:
		closeAll(results)
		return nil, nil, ctx.Err()
	case !partial && len(failed) > 0:
		closeAll(results)
		return nil, nil, failed[0]
	case partial && len(failed) == len(p.Targets):
		return nil, nil, failed[0] // nobody answered
	default:
		return compact(results), failed, nil
	}
}

// compact drops the empty slots of shards that failed.
func compact(results []ShardRows) []ShardRows {
	out := results[:0]
	for _, r := range results {
		if r.Rows != nil {
			out = append(out, r)
		}
	}
	return out
}

// Exec runs the plan's SQL on every target in parallel, at most MaxFanout at a
// time. Unlike Query it does not fail fast: every shard is attempted and its
// outcome reported, because writes that succeeded on some shards must be
// visible to the caller. Outcomes are in the order of p.Targets.
func (e *Executor) Exec(ctx context.Context, p plan.Plan) ([]ShardExec, error) {
	return e.exec(ctx, p, nil)
}

func (e *Executor) exec(ctx context.Context, p plan.Plan, idem *Idempotency) ([]ShardExec, error) {
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
			query, args := p.StatementFor(id)
			o := ShardExec{Shard: id, Rows: p.Rows[id]}
			hctx, began := e.hooks.start(sctx, observe.Exec, id)
			switch {
			case idem != nil:
				o.RowsAffected, o.Replayed, o.Err = runIdempotent(hctx, conns[i], id, query, args, *idem)
			default:
				res, err := conns[i].ExecContext(hctx, query, args...)
				o.Err = err
				if err == nil {
					o.RowsAffected, _ = res.RowsAffected()
				}
			}
			e.hooks.done(hctx, observe.Exec, id, began, o.RowsAffected, o.Replayed, o.Err)
			out[i] = o
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
