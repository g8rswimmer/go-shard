// Package otel reports go-shard statements as OpenTelemetry spans and metrics.
//
//	hooks, err := otel.New()
//	db, err := shard.Open(ctx, shard.Config{ /* ... */ Hooks: hooks})
//
// Each statement is a span ("shard.query" or "shard.exec") with the routing
// decision as attributes, and each shard it runs on is a child span
// ("shard.shard"). A query that was merged adds a "merge" event to the
// statement span. The spans are children of whatever span is in the context
// given to Query or Exec.
//
// Span attributes:
//
//	shard.kind          query or exec
//	shard.strategy      single, multi or all
//	shard.targets       the shard IDs it runs on
//	shard.fanout        how many shards that is
//	shard.reason        which rule or override chose them
//	db.system           postgresql
//	db.statement        the SQL text, only with WithStatement (never arguments)
//	shard.id            on a shard span: which shard
//	shard.rows_affected on a shard span of a write
//	shard.replayed      on a shard span of a write an idempotency key skipped
//
// Metrics (durations are in seconds):
//
//	go_shard.statement.duration   histogram: kind, strategy, error
//	go_shard.shard.duration       histogram: shard.id, kind, error
//	go_shard.fanout.width         histogram: how many shards a statement ran on
//	go_shard.shard.errors         counter:   shard.id, kind
//
// RegisterPoolMetrics adds gauges for each shard's connection pool.
package otel

import (
	"context"
	"time"

	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/g8rswimmer/go-shard/observe"
)

const instrumentation = "github.com/g8rswimmer/go-shard/observe/otel"

// Option customizes New.
type Option func(*config)

type config struct {
	tp        trace.TracerProvider
	mp        metric.MeterProvider
	statement bool
}

// WithTracerProvider sets where spans go. The default is the global provider.
func WithTracerProvider(tp trace.TracerProvider) Option { return func(c *config) { c.tp = tp } }

// WithMeterProvider sets where metrics go. The default is the global provider.
func WithMeterProvider(mp metric.MeterProvider) Option { return func(c *config) { c.mp = mp } }

// WithStatement puts the SQL text on the span as db.statement. Off by default,
// since SQL can carry values written inline. Arguments are never recorded.
func WithStatement() Option { return func(c *config) { c.statement = true } }

type hooks struct {
	observe.Nop
	tracer    trace.Tracer
	statement bool

	statementDur metric.Float64Histogram
	shardDur     metric.Float64Histogram
	fanout       metric.Int64Histogram
	shardErrors  metric.Int64Counter
}

// New returns hooks that emit spans and metrics. Set them as Config.Hooks, or
// combine them with others using observe.Multi.
func New(opts ...Option) (observe.Hooks, error) {
	var c config
	for _, o := range opts {
		o(&c)
	}
	if c.tp == nil {
		c.tp = otelapi.GetTracerProvider()
	}
	if c.mp == nil {
		c.mp = otelapi.GetMeterProvider()
	}
	h := &hooks{tracer: c.tp.Tracer(instrumentation), statement: c.statement}
	m := c.mp.Meter(instrumentation)

	var err error
	if h.statementDur, err = m.Float64Histogram("go_shard.statement.duration",
		metric.WithUnit("s"), metric.WithDescription("Time from routing a statement until it returned (for a query, until its rows were ready).")); err != nil {
		return nil, err
	}
	if h.shardDur, err = m.Float64Histogram("go_shard.shard.duration",
		metric.WithUnit("s"), metric.WithDescription("Time a shard took to run a statement (for a query, to start answering).")); err != nil {
		return nil, err
	}
	if h.fanout, err = m.Int64Histogram("go_shard.fanout.width",
		metric.WithUnit("{shard}"), metric.WithDescription("How many shards a statement ran on.")); err != nil {
		return nil, err
	}
	if h.shardErrors, err = m.Int64Counter("go_shard.shard.errors",
		metric.WithDescription("Statements that failed on a shard.")); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *hooks) OnPlan(ctx context.Context, e observe.PlanEvent) context.Context {
	targets := make([]string, len(e.Targets))
	for i, t := range e.Targets {
		targets[i] = string(t)
	}
	attrs := []attribute.KeyValue{
		attribute.String("db.system", "postgresql"),
		attribute.String("shard.kind", e.Kind.String()),
		attribute.String("shard.strategy", e.Strategy),
		attribute.StringSlice("shard.targets", targets),
		attribute.Int("shard.fanout", len(targets)),
		attribute.String("shard.reason", e.Reason),
	}
	if h.statement {
		attrs = append(attrs, attribute.String("db.statement", e.SQL))
	}
	// the span includes the routing that already happened
	ctx, _ = h.tracer.Start(ctx, "shard."+e.Kind.String(),
		trace.WithTimestamp(time.Now().Add(-e.Duration)), trace.WithAttributes(attrs...))
	return ctx
}

func (h *hooks) OnShardStart(ctx context.Context, e observe.ShardStartEvent) context.Context {
	ctx, _ = h.tracer.Start(ctx, "shard.shard", trace.WithAttributes(
		attribute.String("shard.id", string(e.Shard)),
		attribute.String("shard.kind", e.Kind.String()),
		attribute.String("db.system", "postgresql"),
	))
	return ctx
}

func (h *hooks) OnShardDone(ctx context.Context, e observe.ShardDoneEvent) {
	span := trace.SpanFromContext(ctx)
	if e.Kind == observe.Exec {
		span.SetAttributes(attribute.Int64("shard.rows_affected", e.RowsAffected), attribute.Bool("shard.replayed", e.Replayed))
	}
	fail(span, e.Err)
	span.End()

	id, kind := attribute.String("shard.id", string(e.Shard)), attribute.String("shard.kind", e.Kind.String())
	h.shardDur.Record(ctx, e.Duration.Seconds(), metric.WithAttributes(id, kind, attribute.Bool("error", e.Err != nil)))
	if e.Err != nil {
		h.shardErrors.Add(ctx, 1, metric.WithAttributes(id, kind))
	}
}

func (h *hooks) OnMerge(ctx context.Context, e observe.MergeEvent) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("merge", trace.WithAttributes(
		attribute.Int("shard.merged", e.Shards),
		attribute.Int("shard.left_out", e.Failed),
		attribute.Float64("merge.duration_s", e.Duration.Seconds()),
	))
	if e.Err != nil {
		span.RecordError(e.Err)
	}
}

func (h *hooks) OnDone(ctx context.Context, e observe.DoneEvent) {
	span := trace.SpanFromContext(ctx)
	fail(span, e.Err)
	span.End()

	kind, strategy := attribute.String("shard.kind", e.Kind.String()), attribute.String("shard.strategy", e.Strategy)
	h.statementDur.Record(ctx, e.Duration.Seconds(), metric.WithAttributes(kind, strategy, attribute.Bool("error", e.Err != nil)))
	if len(e.Targets) > 0 {
		h.fanout.Record(ctx, int64(len(e.Targets)), metric.WithAttributes(kind, strategy))
	}
}

func fail(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
