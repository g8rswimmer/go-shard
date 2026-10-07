package shard

import (
	"context"
	"errors"
	"fmt"

	"github.com/g8rswimmer/go-shard/plan"
)

// Rows is a result set. *sql.Rows satisfies it, and so will merged results from
// several shards. Always Close it.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	Err() error
	Close() error
}

// ShardOutcome is what a write did on one shard.
type ShardOutcome struct {
	RowsAffected int64
	// Err is nil when the write succeeded on this shard.
	Err error
}

// WriteResult reports a write shard by shard. A write that reached several
// shards is not atomic: some can succeed while others fail.
type WriteResult struct {
	PerShard map[ShardID]ShardOutcome
}

// RowsAffected totals the rows affected on the shards where the write
// succeeded.
func (r WriteResult) RowsAffected() int64 {
	var n int64
	for _, o := range r.PerShard {
		if o.Err == nil {
			n += o.RowsAffected
		}
	}
	return n
}

// Querier runs statements. *DB and the values returned by WithShardKey,
// WithShard and WithAllShards implement it. Application code should depend on
// this interface so tests can substitute a fake.
type Querier interface {
	// Query runs a read on the target shard and returns its rows. The caller
	// must close them.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	// Exec runs a write on the target shard(s). It returns an error if the
	// write failed on any shard; the WriteResult still reports every shard.
	Exec(ctx context.Context, sql string, args ...any) (WriteResult, error)
}

type routeKind int

const (
	routeNone routeKind = iota
	routeByKey
	routeByShard
	routeAll
)

// route is a caller-supplied routing decision.
type route struct {
	kind  routeKind
	key   any
	shard ShardID
}

// scoped is a Querier with a fixed routing decision.
type scoped struct {
	db    *DB
	route route
}

// WithShardKey routes statements to the shard that owns key.
func (db *DB) WithShardKey(key any) Querier { return &scoped{db, route{kind: routeByKey, key: key}} }

// WithShard routes statements to one named shard.
func (db *DB) WithShard(id ShardID) Querier { return &scoped{db, route{kind: routeByShard, shard: id}} }

// WithAllShards runs statements on every shard. Exec works today; Query needs
// result merging and returns ErrUnsupportedQuery until that is added.
func (db *DB) WithAllShards() Querier { return &scoped{db, route{kind: routeAll}} }

// Query implements Querier. Without WithShardKey, WithShard or WithAllShards it
// returns ErrShardKeyRequired.
func (db *DB) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	return (&scoped{db: db}).Query(ctx, sql, args...)
}

// Exec implements Querier. Without WithShardKey, WithShard or WithAllShards it
// returns ErrShardKeyRequired.
func (db *DB) Exec(ctx context.Context, sql string, args ...any) (WriteResult, error) {
	return (&scoped{db: db}).Exec(ctx, sql, args...)
}

func (s *scoped) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	p, err := s.plan(sql, args)
	if err != nil {
		return nil, err
	}
	if len(p.Targets) > 1 {
		return nil, fmt.Errorf("%w: reading from %d shards needs result merging, which is not available yet; use WithShardKey or WithShard", ErrUnsupportedQuery, len(p.Targets))
	}
	res, err := s.db.exec.Query(ctx, p)
	if err != nil {
		return nil, err
	}
	return res[0].Rows, nil
}

func (s *scoped) Exec(ctx context.Context, sql string, args ...any) (WriteResult, error) {
	p, err := s.plan(sql, args)
	if err != nil {
		return WriteResult{}, err
	}
	outcomes, err := s.db.exec.Exec(ctx, p)
	if err != nil {
		return WriteResult{}, err
	}

	res := WriteResult{PerShard: make(map[ShardID]ShardOutcome, len(outcomes))}
	var failed []error
	for _, o := range outcomes {
		switch {
		case o.Err != nil:
			res.PerShard[o.Shard] = ShardOutcome{Err: &ShardError{Shard: o.Shard, Err: o.Err}}
			failed = append(failed, res.PerShard[o.Shard].Err)
		default:
			n, _ := o.Result.RowsAffected()
			res.PerShard[o.Shard] = ShardOutcome{RowsAffected: n}
		}
	}
	switch len(failed) {
	case 0:
		return res, nil
	case 1:
		return res, failed[0]
	default:
		return res, fmt.Errorf("write failed on %d of %d shards: %w", len(failed), len(outcomes), errors.Join(failed...))
	}
}

// plan turns the routing decision into a Plan.
func (s *scoped) plan(sql string, args []any) (plan.Plan, error) {
	p := plan.Plan{SQL: sql, Args: args}
	switch s.route.kind {
	case routeByKey:
		bucket, err := s.db.router.Bucket(s.route.key)
		if err != nil {
			return plan.Plan{}, fmt.Errorf("shard key %v: %w", s.route.key, err)
		}
		id, err := s.db.router.ShardFor(s.route.key)
		if err != nil {
			return plan.Plan{}, fmt.Errorf("shard key %v: %w", s.route.key, err)
		}
		p.Targets = []ShardID{id}
		p.Strategy = plan.Single
		p.Reason = fmt.Sprintf("WithShardKey(%v): bucket %d belongs to %s", s.route.key, bucket, id)
	case routeByShard:
		if _, ok := s.db.pool.Conn(s.route.shard); !ok {
			return plan.Plan{}, fmt.Errorf("%w: %q (configured shards: %v)", ErrUnknownShard, s.route.shard, s.db.pool.IDs())
		}
		p.Targets = []ShardID{s.route.shard}
		p.Strategy = plan.Single
		p.Reason = fmt.Sprintf("WithShard(%s)", s.route.shard)
	case routeAll:
		p.Targets = s.db.pool.IDs()
		p.Strategy = plan.All
		p.Reason = "WithAllShards()"
	default:
		return plan.Plan{}, fmt.Errorf("%w: use db.WithShardKey(key), db.WithShard(id) or db.WithAllShards() (routing from the SQL itself is not available yet)", ErrShardKeyRequired)
	}
	return p, nil
}
