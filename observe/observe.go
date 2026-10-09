package observe

import (
	"context"
	"fmt"
	"time"

	"github.com/g8rswimmer/go-shard/router"
)

// ShardID names a shard; it is the same type as shard.ShardID.
type ShardID = router.ShardID

// Kind says whether a statement reads or writes.
type Kind int

const (
	// Query is a statement run with Query or QueryStatement.
	Query Kind = iota + 1
	// Exec is a statement run with Exec or ExecStatement.
	Exec
)

// String returns "query" or "exec".
func (k Kind) String() string {
	switch k {
	case Query:
		return "query"
	case Exec:
		return "exec"
	default:
		return fmt.Sprintf("Kind(%d)", int(k))
	}
}

// PlanEvent is the routing decision for a statement.
type PlanEvent struct {
	Kind Kind
	// SQL is the statement as the caller wrote it. Arguments are not included.
	SQL string
	// Strategy is "single", "multi" or "all": how many shards it runs on.
	Strategy string
	Targets  []ShardID
	// Reason says which rule or override chose the targets.
	Reason string
	// Err is why the statement could not be routed. No shard is contacted.
	Err error
	// Duration is how long routing took.
	Duration time.Duration
}

// ShardStartEvent says a statement is about to run on a shard.
type ShardStartEvent struct {
	Kind  Kind
	Shard ShardID
}

// ShardDoneEvent says a statement finished on a shard.
type ShardDoneEvent struct {
	Kind     Kind
	Shard    ShardID
	Duration time.Duration
	// RowsAffected is what a write changed. It is zero for queries and failures.
	RowsAffected int64
	// Replayed is true for a write an idempotency key showed was already applied.
	Replayed bool
	Err      error
}

// MergeEvent describes combining the rows of a query that ran on several shards.
type MergeEvent struct {
	// Shards is how many shards' rows were merged.
	Shards int
	// Failed is how many shards did not answer and were left out (AllowPartial).
	Failed int
	// Duration is the time Merge took to set up. Rows of an ordered merge are
	// produced as they are read, so this does not include reading them.
	Duration time.Duration
	Err      error
}

// DoneEvent ends a statement.
type DoneEvent struct {
	Kind     Kind
	Strategy string
	Targets  []ShardID
	// Duration runs from the start of routing until the statement returned: for a
	// query, until its rows are ready to read.
	Duration time.Duration
	// Err is the error the statement returned. A write that failed on some
	// shards has one.
	Err error
}

// Hooks receives the events of every statement. See the package documentation
// for the order and the rules. OnPlan and OnShardStart return the context to
// use for the rest of the statement, or for that shard's work: that is how a
// tracer makes spans nested. Return ctx itself when there is nothing to add.
type Hooks interface {
	OnPlan(ctx context.Context, e PlanEvent) context.Context
	OnShardStart(ctx context.Context, e ShardStartEvent) context.Context
	OnShardDone(ctx context.Context, e ShardDoneEvent)
	OnMerge(ctx context.Context, e MergeEvent)
	OnDone(ctx context.Context, e DoneEvent)
}

// Nop does nothing. It is the default, and can be embedded to implement only
// some of the hooks.
type Nop struct{}

var _ Hooks = Nop{}

// OnPlan does nothing and returns ctx.
func (Nop) OnPlan(ctx context.Context, _ PlanEvent) context.Context { return ctx }

// OnShardStart does nothing and returns ctx.
func (Nop) OnShardStart(ctx context.Context, _ ShardStartEvent) context.Context { return ctx }

// OnShardDone does nothing.
func (Nop) OnShardDone(context.Context, ShardDoneEvent) {}

// OnMerge does nothing.
func (Nop) OnMerge(context.Context, MergeEvent) {}

// OnDone does nothing.
func (Nop) OnDone(context.Context, DoneEvent) {}

// Multi calls several Hooks in turn, for example Slog and the OpenTelemetry
// hooks. Each one's returned context is passed on to the next, and on to the
// statement.
func Multi(hooks ...Hooks) Hooks {
	switch len(hooks) {
	case 0:
		return Nop{}
	case 1:
		return hooks[0]
	default:
		return multi(append([]Hooks(nil), hooks...))
	}
}

type multi []Hooks

func (m multi) OnPlan(ctx context.Context, e PlanEvent) context.Context {
	for _, h := range m {
		ctx = h.OnPlan(ctx, e)
	}
	return ctx
}

func (m multi) OnShardStart(ctx context.Context, e ShardStartEvent) context.Context {
	for _, h := range m {
		ctx = h.OnShardStart(ctx, e)
	}
	return ctx
}

func (m multi) OnShardDone(ctx context.Context, e ShardDoneEvent) {
	for _, h := range m {
		h.OnShardDone(ctx, e)
	}
}

func (m multi) OnMerge(ctx context.Context, e MergeEvent) {
	for _, h := range m {
		h.OnMerge(ctx, e)
	}
}

func (m multi) OnDone(ctx context.Context, e DoneEvent) {
	for _, h := range m {
		h.OnDone(ctx, e)
	}
}
