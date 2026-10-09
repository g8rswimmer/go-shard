// Package exec runs plans against shards with bounded concurrency.
package exec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	// Registers the "pgx" driver for database/sql.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/g8rswimmer/go-shard/router"
)

// ErrUnknownShard is returned when a plan names a shard that is not in the pool.
var ErrUnknownShard = errors.New("exec: unknown shard")

// ShardError attaches the shard a failure came from.
type ShardError struct {
	Shard router.ShardID
	Err   error
}

// Error names the shard and gives the underlying error.
func (e *ShardError) Error() string { return fmt.Sprintf("shard %s: %v", e.Shard, e.Err) }

// Unwrap returns the underlying error, for errors.Is and errors.As.
func (e *ShardError) Unwrap() error { return e.Err }

// Conn is the part of *sql.DB the executor uses. It exists so the executor can
// be tested without a database.
type Conn interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	PingContext(ctx context.Context) error
	Stats() sql.DBStats
	Close() error
}

// Spec describes one shard to connect to.
type Spec struct {
	ID       router.ShardID
	DSN      string
	MaxConns int
}

// Pool holds one connection pool per shard. It is immutable after creation and
// safe for concurrent use.
type Pool struct {
	conns map[router.ShardID]Conn
	ids   []router.ShardID // sorted
}

// NewPool wraps existing connections. Open is the usual way to build one.
func NewPool(conns map[router.ShardID]Conn) *Pool {
	p := &Pool{conns: make(map[router.ShardID]Conn, len(conns))}
	for id, c := range conns {
		p.conns[id] = c
		p.ids = append(p.ids, id)
	}
	sort.Slice(p.ids, func(i, j int) bool { return p.ids[i] < p.ids[j] })
	return p
}

// Open connects to every shard and pings it. If any shard cannot be reached,
// everything already opened is closed and the error names the shard. DSNs are
// never included in errors.
func Open(ctx context.Context, specs []Spec) (*Pool, error) {
	conns := make(map[router.ShardID]Conn, len(specs))
	closeAll := func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}

	for _, s := range specs {
		db, err := sql.Open("pgx", s.DSN)
		if err != nil {
			closeAll()
			return nil, &ShardError{Shard: s.ID, Err: fmt.Errorf("invalid connection settings: %w", errors.Unwrap(err))}
		}
		if s.MaxConns > 0 {
			db.SetMaxOpenConns(s.MaxConns)
		}
		conns[s.ID] = db
	}

	pool := NewPool(conns)
	for _, h := range pool.Health(ctx) {
		if h.Err != nil {
			closeAll()
			return nil, &ShardError{Shard: h.ID, Err: fmt.Errorf("ping: %w", h.Err)}
		}
	}
	return pool, nil
}

// IDs returns every shard ID, sorted.
func (p *Pool) IDs() []router.ShardID { return append([]router.ShardID(nil), p.ids...) }

// Conn returns the connection pool for a shard.
func (p *Pool) Conn(id router.ShardID) (Conn, bool) {
	c, ok := p.conns[id]
	return c, ok
}

// Close closes every shard's pool and returns the errors joined.
func (p *Pool) Close() error {
	var errs []error
	for _, id := range p.ids {
		if err := p.conns[id].Close(); err != nil {
			errs = append(errs, &ShardError{Shard: id, Err: err})
		}
	}
	return errors.Join(errs...)
}

// Stats returns each shard's pool statistics. It does not contact the shards.
func (p *Pool) Stats() map[router.ShardID]sql.DBStats {
	out := make(map[router.ShardID]sql.DBStats, len(p.ids))
	for _, id := range p.ids {
		out[id] = p.conns[id].Stats()
	}
	return out
}

// ShardHealth is the result of pinging one shard.
type ShardHealth struct {
	ID      router.ShardID
	Err     error // nil when healthy
	Latency time.Duration
	Pool    sql.DBStats
}

// Healthy reports whether the ping succeeded.
func (h ShardHealth) Healthy() bool { return h.Err == nil }

// Health pings every shard in parallel and returns the results sorted by ID.
func (p *Pool) Health(ctx context.Context) []ShardHealth {
	out := make([]ShardHealth, len(p.ids))
	var wg sync.WaitGroup
	for i, id := range p.ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := p.conns[id]
			start := time.Now()
			err := c.PingContext(ctx)
			out[i] = ShardHealth{ID: id, Err: err, Latency: time.Since(start), Pool: c.Stats()}
		}()
	}
	wg.Wait()
	return out
}
