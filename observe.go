package shard

import (
	"context"
	"time"

	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/plan"
)

// statement follows one statement through the Config.Hooks.
type statement struct {
	hooks    observe.Hooks
	kind     observe.Kind
	started  time.Time
	strategy string
	targets  []ShardID
	// ctx is what OnPlan returned: the context for the rest of the statement.
	ctx context.Context
}

// begin reports the routing of a statement that started at start. planErr is
// why routing failed, if it did; the statement is then only done. Use the
// returned context for the rest of the statement.
func (db *DB) begin(ctx context.Context, kind observe.Kind, sql string, start time.Time, p plan.Plan, planErr error) (context.Context, *statement) {
	h := db.hooks
	if h == nil {
		h = observe.Nop{} // a DB built by hand in a test
	}
	st := &statement{hooks: h, kind: kind, started: start}
	if planErr == nil {
		st.strategy, st.targets = p.Strategy.String(), p.Targets
	}
	st.ctx = h.OnPlan(ctx, observe.PlanEvent{
		Kind: kind, SQL: sql, Strategy: st.strategy, Targets: st.targets, Reason: p.Reason,
		Err: planErr, Duration: time.Since(start),
	})
	return st.ctx, st
}

// merged reports the merge of a query that ran on several shards.
func (s *statement) merged(shards, failed int, began time.Time, err error) {
	s.hooks.OnMerge(s.ctx, observe.MergeEvent{Shards: shards, Failed: failed, Duration: time.Since(began), Err: err})
}

// done ends the statement; err is what it returns to the caller.
func (s *statement) done(err error) {
	s.hooks.OnDone(s.ctx, observe.DoneEvent{
		Kind: s.kind, Strategy: s.strategy, Targets: s.targets, Duration: time.Since(s.started), Err: err,
	})
}
