package exec

import (
	"context"
	"time"

	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/router"
)

// shardHooks reports the work on one shard. The zero value reports nothing.
type shardHooks struct{ h observe.Hooks }

// start tells the hooks a statement is about to run on a shard. Use the context
// it returns for the work, and pass it to done.
func (s shardHooks) start(ctx context.Context, kind observe.Kind, id router.ShardID) (context.Context, time.Time) {
	began := time.Now()
	if s.h == nil {
		return ctx, began
	}
	return s.h.OnShardStart(ctx, observe.ShardStartEvent{Kind: kind, Shard: id}), began
}

func (s shardHooks) done(ctx context.Context, kind observe.Kind, id router.ShardID, began time.Time, affected int64, replayed bool, err error) {
	if s.h == nil {
		return
	}
	s.h.OnShardDone(ctx, observe.ShardDoneEvent{
		Kind: kind, Shard: id, Duration: time.Since(began),
		RowsAffected: affected, Replayed: replayed, Err: err,
	})
}
