package shard

import (
	"context"
	"fmt"

	"github.com/g8rswimmer/go-shard/exec"
	"github.com/g8rswimmer/go-shard/merge"
	"github.com/g8rswimmer/go-shard/plan"
)

type allowPartial struct{}

// AllowPartial returns a context that lets a query running on several shards
// succeed when some of them fail to answer.
//
// By default such a query fails fast: the first shard error cancels the rest
// and is returned. With AllowPartial the shards that answer are merged, and the
// shards that did not are reported by ShardErrors on the returned rows. The
// merged result then covers only the shards that answered, so a count or sum
// is too small by what the others hold: check ShardErrors before trusting it.
//
// Only a shard that fails to start its query is tolerated. An error while a
// shard is streaming its rows stops the iteration and appears in Rows.Err. The
// query still fails if no shard answers.
func AllowPartial(ctx context.Context) context.Context {
	return context.WithValue(ctx, allowPartial{}, true)
}

func partialAllowed(ctx context.Context) bool {
	v, _ := ctx.Value(allowPartial{}).(bool)
	return v
}

// ShardErrors returns the shards that did not answer a query run with
// AllowPartial, in shard order. It is nil when every shard answered, and for
// rows that did not come from several shards.
func ShardErrors(rows Rows) []*ShardError {
	if p, ok := rows.(interface{ ShardErrors() []*ShardError }); ok {
		return p.ShardErrors()
	}
	return nil
}

// fanRows is the merged rows of a query that ran on several shards.
type fanRows struct {
	*merge.Rows
	failed []*ShardError
}

func (f *fanRows) ShardErrors() []*ShardError { return f.failed }

// shardSource labels a shard's read errors with the shard.
type shardSource struct {
	*exec.Rows
	id ShardID
}

func (s shardSource) Err() error {
	if err := s.Rows.Err(); err != nil {
		return &ShardError{Shard: s.id, Err: err}
	}
	return nil
}

// fanOut runs a SELECT on several shards and merges the rows.
func (s *scoped) fanOut(ctx context.Context, r request, p plan.Plan) (Rows, error) {
	if r.merge == nil {
		return nil, fmt.Errorf("%w: running a query on %d shards needs its results merged, and merging needs the SQL rewritten, "+
			"which needs an analyzer that supports it (the library was built without cgo); build the statement with package query, "+
			"narrow it to one shard key, use WithShardKey / WithShard, or set Config.Analyzer to one that also implements merge.Planner",
			ErrUnsupportedQuery, len(p.Targets))
	}
	mp, err := r.merge()
	if err != nil {
		return nil, err
	}
	p.SQL, p.Args, p.PerShard = mp.SQL, mp.Args, nil

	var (
		results []exec.ShardRows
		failed  []*ShardError
	)
	switch {
	case partialAllowed(ctx):
		results, failed, err = s.db.exec.QueryPartial(ctx, p)
	default:
		results, err = s.db.exec.Query(ctx, p)
	}
	if err != nil {
		return nil, err
	}

	srcs := make([]merge.Source, len(results))
	for i, res := range results {
		srcs[i] = shardSource{Rows: res.Rows, id: res.Shard}
	}
	rows, err := merge.Merge(mp.Spec, srcs, merge.Options{MaxRows: s.db.maxMerge})
	if err != nil {
		return nil, err
	}
	return &fanRows{Rows: rows, failed: failed}, nil
}
