package observe

import (
	"context"
	"log/slog"
	"strings"
)

// SlogOption customizes Slog.
type SlogOption func(*slogHooks)

// WithSQL includes the SQL text of each statement in the log. Arguments are
// never logged. Off by default: SQL can carry values written inline.
func WithSQL() SlogOption { return func(h *slogHooks) { h.sql = true } }

// WithLevel sets the level of the line logged for a statement that succeeded
// (default slog.LevelDebug). Failures are logged at Error, and a shard that
// failed within a statement at Warn.
func WithLevel(l slog.Level) SlogOption { return func(h *slogHooks) { h.level = l } }

// WithShardEvents also logs one line per shard (default: only a failed shard).
func WithShardEvents() SlogOption { return func(h *slogHooks) { h.perShard = true } }

// Slog logs one line per statement, with its routing and duration, to logger:
//
//	level=DEBUG msg="shard statement" kind=query strategy=all targets=shard-01,shard-02 duration=3.1ms
//
// A statement that fails is logged at Error with the error; a shard that fails
// is logged at Warn, which matters when AllowPartial hides it from the caller.
func Slog(logger *slog.Logger, opts ...SlogOption) Hooks {
	h := &slogHooks{log: logger, level: slog.LevelDebug}
	for _, o := range opts {
		o(h)
	}
	return h
}

type slogHooks struct {
	Nop
	log      *slog.Logger
	level    slog.Level
	sql      bool
	perShard bool
}

type slogPlan struct{}

// OnPlan remembers the SQL for the statement's final line.
func (h *slogHooks) OnPlan(ctx context.Context, e PlanEvent) context.Context {
	if !h.sql {
		return ctx
	}
	return context.WithValue(ctx, slogPlan{}, e.SQL)
}

func (h *slogHooks) OnShardDone(ctx context.Context, e ShardDoneEvent) {
	switch {
	case e.Err != nil:
		h.log.LogAttrs(ctx, slog.LevelWarn, "shard statement failed",
			slog.String("kind", e.Kind.String()), slog.String("shard", string(e.Shard)),
			slog.Duration("duration", e.Duration), slog.Any("error", e.Err))
	case h.perShard:
		attrs := []slog.Attr{
			slog.String("kind", e.Kind.String()), slog.String("shard", string(e.Shard)),
			slog.Duration("duration", e.Duration),
		}
		if e.Kind == Exec {
			attrs = append(attrs, slog.Int64("rows_affected", e.RowsAffected), slog.Bool("replayed", e.Replayed))
		}
		h.log.LogAttrs(ctx, h.level, "shard statement done", attrs...)
	default:
		// quiet unless asked
	}
}

func (h *slogHooks) OnMerge(ctx context.Context, e MergeEvent) {
	if h.perShard || e.Err != nil || e.Failed > 0 {
		level := h.level
		if e.Err != nil || e.Failed > 0 {
			level = slog.LevelWarn
		}
		attrs := []slog.Attr{slog.Int("shards", e.Shards), slog.Int("failed", e.Failed), slog.Duration("duration", e.Duration)}
		if e.Err != nil {
			attrs = append(attrs, slog.Any("error", e.Err))
		}
		h.log.LogAttrs(ctx, level, "shard merge", attrs...)
	}
}

func (h *slogHooks) OnDone(ctx context.Context, e DoneEvent) {
	targets := make([]string, len(e.Targets))
	for i, t := range e.Targets {
		targets[i] = string(t)
	}
	attrs := []slog.Attr{slog.String("kind", e.Kind.String())}
	if e.Strategy != "" { // empty when the statement could not be routed
		attrs = append(attrs, slog.String("strategy", e.Strategy), slog.String("targets", strings.Join(targets, ",")))
	}
	attrs = append(attrs, slog.Duration("duration", e.Duration))
	if sql, ok := ctx.Value(slogPlan{}).(string); ok {
		attrs = append(attrs, slog.String("sql", sql))
	}
	switch {
	case e.Err != nil:
		h.log.LogAttrs(ctx, slog.LevelError, "shard statement failed", append(attrs, slog.Any("error", e.Err))...)
	default:
		h.log.LogAttrs(ctx, h.level, "shard statement", attrs...)
	}
}
