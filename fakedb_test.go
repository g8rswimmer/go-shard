package shard

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/observe"
)

// fakeShard is a database/sql driver whose behavior a test sets, so a DB can
// run statements with no PostgreSQL. A query returns `rows` rows of one int
// column; an exec reports `affected` rows.
type fakeShard struct {
	rows     int
	affected int64
	queryErr error
	execErr  error
}

func (f *fakeShard) db() *sql.DB { return sql.OpenDB(fakeConnector{f}) }

type fakeConnector struct{ f *fakeShard }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return &fakeConn{c.f}, nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, io.ErrUnexpectedEOF }

type fakeConn struct{ f *fakeShard }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, io.ErrUnexpectedEOF }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return fakeTx{}, nil }
func (c *fakeConn) Ping(context.Context) error          { return nil }

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

func (c *fakeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.f.queryErr != nil {
		return nil, c.f.queryErr
	}
	return &fakeRows{n: c.f.rows}, nil
}

func (c *fakeConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if c.f.execErr != nil {
		return nil, c.f.execErr
	}
	return driver.RowsAffected(c.f.affected), nil
}

type fakeRows struct{ n, pos int }

func (r *fakeRows) Columns() []string { return []string{"id"} }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= r.n {
		return io.EOF
	}
	dest[0] = int64(r.pos)
	r.pos++
	return nil
}

// fakeDB opens a DB over fake shards named shard-01, shard-02, ..., using the
// explain registry (profiles sharded by id, addresses colocated).
func fakeDB(t *testing.T, hooks observe.Hooks, shards ...*fakeShard) *DB {
	t.Helper()
	return fakeDBFor(t, Config{Registry: explainRegistry(t), Hooks: hooks}, shards...)
}

// fakeDBFor is fakeDB for a Config that already has a Registry.
func fakeDBFor(t testing.TB, cfg Config, shards ...*fakeShard) *DB {
	t.Helper()
	conns := map[ShardID]exec.Conn{}
	for i, f := range shards {
		id := ShardID(fmt.Sprintf("shard-%02d", i+1))
		cfg.Shards = append(cfg.Shards, ShardConfig{ID: id})
		conns[id] = f.db()
	}
	r, err := cfg.validateShape(false)
	if err != nil {
		t.Fatal(err)
	}
	pool := exec.NewPool(conns)
	t.Cleanup(func() { _ = pool.Close() })
	return newDB(cfg, r, pool, exec.NewExecutor(pool, exec.Options{Hooks: cfg.Hooks}))
}

// recorder is a Hooks that remembers what it was told, in order.
type recorder struct {
	mu     sync.Mutex
	events []string
	plan   []observe.PlanEvent
	starts []observe.ShardStartEvent
	dones  []observe.ShardDoneEvent
	merges []observe.MergeEvent
	ends   []observe.DoneEvent
	ctxErr []string // problems with the contexts passed between hooks
}

type ctxKey string

func (r *recorder) OnPlan(ctx context.Context, e observe.PlanEvent) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "plan")
	r.plan = append(r.plan, e)
	return context.WithValue(ctx, ctxKey("plan"), "seen")
}

func (r *recorder) OnShardStart(ctx context.Context, e observe.ShardStartEvent) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "start "+string(e.Shard))
	r.starts = append(r.starts, e)
	if ctx.Value(ctxKey("plan")) != "seen" {
		r.ctxErr = append(r.ctxErr, "OnShardStart did not get the context OnPlan returned")
	}
	return context.WithValue(ctx, ctxKey("shard"), string(e.Shard))
}

func (r *recorder) OnShardDone(ctx context.Context, e observe.ShardDoneEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "done "+string(e.Shard))
	r.dones = append(r.dones, e)
	if ctx.Value(ctxKey("shard")) != string(e.Shard) {
		r.ctxErr = append(r.ctxErr, "OnShardDone did not get the context OnShardStart returned for "+string(e.Shard))
	}
}

func (r *recorder) OnMerge(ctx context.Context, e observe.MergeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "merge")
	r.merges = append(r.merges, e)
	if ctx.Value(ctxKey("plan")) != "seen" {
		r.ctxErr = append(r.ctxErr, "OnMerge did not get the context OnPlan returned")
	}
}

func (r *recorder) OnDone(ctx context.Context, e observe.DoneEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "done")
	r.ends = append(r.ends, e)
	if ctx.Value(ctxKey("plan")) != "seen" {
		r.ctxErr = append(r.ctxErr, "OnDone did not get the context OnPlan returned")
	}
}

// kinds lists the events with the shard names removed: plan, start, done...
func (r *recorder) kinds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	for i, e := range r.events {
		switch {
		case len(e) > 5 && e[:5] == "start":
			out[i] = "start"
		case len(e) > 4 && e[:4] == "done":
			out[i] = "shard-done"
		default:
			out[i] = e
		}
	}
	return out
}
