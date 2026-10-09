package otel

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/g8rswimmer/go-shard/observe"
)

type rig struct {
	hooks  observe.Hooks
	spans  *tracetest.SpanRecorder
	reader *sdkmetric.ManualReader
	mp     *sdkmetric.MeterProvider
}

func newRig(t *testing.T, opts ...Option) *rig {
	t.Helper()
	r := &rig{spans: tracetest.NewSpanRecorder(), reader: sdkmetric.NewManualReader()}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(r.spans))
	r.mp = sdkmetric.NewMeterProvider(sdkmetric.WithReader(r.reader))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()); _ = r.mp.Shutdown(context.Background()) })
	var err error
	r.hooks, err = New(append([]Option{WithTracerProvider(tp), WithMeterProvider(r.mp)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func attrs(kvs []attribute.KeyValue) map[string]attribute.Value {
	m := map[string]attribute.Value{}
	for _, kv := range kvs {
		m[string(kv.Key)] = kv.Value
	}
	return m
}

// fanOut plays the events of a query on two shards, one of which fails.
func (r *rig) fanOut(ctx context.Context, shardErr error) {
	targets := []observe.ShardID{"shard-01", "shard-02"}
	ctx = r.hooks.OnPlan(ctx, observe.PlanEvent{
		Kind: observe.Query, SQL: "SELECT name FROM profiles", Strategy: "all", Targets: targets,
		Reason: "WithAllShards()", Duration: time.Millisecond,
	})
	for i, id := range targets {
		sctx := r.hooks.OnShardStart(ctx, observe.ShardStartEvent{Kind: observe.Query, Shard: id})
		var err error
		if i == 1 {
			err = shardErr
		}
		r.hooks.OnShardDone(sctx, observe.ShardDoneEvent{Kind: observe.Query, Shard: id, Duration: 3 * time.Millisecond, Err: err})
	}
	r.hooks.OnMerge(ctx, observe.MergeEvent{Shards: 2, Duration: time.Millisecond})
	r.hooks.OnDone(ctx, observe.DoneEvent{Kind: observe.Query, Strategy: "all", Targets: targets, Duration: 5 * time.Millisecond, Err: shardErr})
}

func TestSpansCarryTheRoutingDecision(t *testing.T) {
	r := newRig(t)
	r.fanOut(context.Background(), nil)

	spans := r.spans.Ended()
	if len(spans) != 3 {
		t.Fatalf("%d spans, want 1 statement + 2 shards", len(spans))
	}
	var stmt sdktrace.ReadOnlySpan
	shards := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		switch s.Name() {
		case "shard.query":
			stmt = s
		case "shard.shard":
			shards[attrs(s.Attributes())["shard.id"].AsString()] = s
		default:
			t.Errorf("unexpected span %q", s.Name())
		}
	}

	a := attrs(stmt.Attributes())
	if a["shard.strategy"].AsString() != "all" || a["shard.fanout"].AsInt64() != 2 || a["shard.kind"].AsString() != "query" ||
		a["shard.reason"].AsString() != "WithAllShards()" || a["db.system"].AsString() != "postgresql" {
		t.Errorf("statement attributes = %v", stmt.Attributes())
	}
	if got := a["shard.targets"].AsStringSlice(); len(got) != 2 || got[0] != "shard-01" || got[1] != "shard-02" {
		t.Errorf("shard.targets = %v", got)
	}
	if _, ok := a["db.statement"]; ok {
		t.Error("the SQL text must be opt-in")
	}
	if len(shards) != 2 {
		t.Fatalf("shard spans = %v", shards)
	}
	for id, s := range shards {
		if s.Parent().SpanID() != stmt.SpanContext().SpanID() {
			t.Errorf("%s span is not a child of the statement span", id)
		}
	}
	if len(stmt.Events()) != 1 || stmt.Events()[0].Name != "merge" ||
		attrs(stmt.Events()[0].Attributes)["shard.merged"].AsInt64() != 2 {
		t.Errorf("events = %+v", stmt.Events())
	}
	if stmt.Status().Code == codes.Error {
		t.Error("statement span marked failed")
	}
	// the span starts when routing did
	if d := stmt.EndTime().Sub(stmt.StartTime()); d < time.Millisecond {
		t.Errorf("statement span lasted %v, want it to include the routing time", d)
	}
}

func TestStatementTextIsOptIn(t *testing.T) {
	r := newRig(t, WithStatement())
	r.fanOut(context.Background(), nil)
	for _, s := range r.spans.Ended() {
		if s.Name() == "shard.query" {
			if got := attrs(s.Attributes())["db.statement"].AsString(); got != "SELECT name FROM profiles" {
				t.Errorf("db.statement = %q", got)
			}
		}
	}
}

func TestAFailedShardMarksItsSpanAndTheStatement(t *testing.T) {
	r := newRig(t)
	boom := errors.New("boom")
	r.fanOut(context.Background(), boom)

	failed := map[string]bool{}
	for _, s := range r.spans.Ended() {
		if s.Status().Code == codes.Error {
			failed[s.Name()+" "+attrs(s.Attributes())["shard.id"].AsString()] = true
		}
	}
	if len(failed) != 2 || !failed["shard.shard shard-02"] || !failed["shard.query "] {
		t.Errorf("failed spans = %v, want the statement and shard-02", failed)
	}
}

func TestSpansAreChildrenOfTheCallersSpan(t *testing.T) {
	r := newRig(t)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(r.spans))
	ctx, parent := tp.Tracer("app").Start(context.Background(), "handler")
	r.fanOut(ctx, nil)
	parent.End()

	for _, s := range r.spans.Ended() {
		if s.Name() == "shard.query" && s.Parent().SpanID() != parent.SpanContext().SpanID() {
			t.Error("the statement span should be a child of the span in the context")
		}
	}
}

func collect(t *testing.T, r *rig) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func TestMetrics(t *testing.T) {
	r := newRig(t)
	boom := errors.New("boom")
	r.fanOut(context.Background(), nil)
	r.fanOut(context.Background(), boom)
	m := collect(t, r)

	fan := m["go_shard.fanout.width"].Data.(metricdata.Histogram[int64])
	if len(fan.DataPoints) != 1 || fan.DataPoints[0].Count != 2 || fan.DataPoints[0].Sum != 4 {
		t.Errorf("fanout width = %+v, want two statements of width 2", fan.DataPoints)
	}

	dur := m["go_shard.statement.duration"].Data.(metricdata.Histogram[float64])
	byErr := map[bool]uint64{}
	for _, dp := range dur.DataPoints {
		v, _ := dp.Attributes.Value("error")
		byErr[v.AsBool()] += dp.Count
	}
	if byErr[false] != 1 || byErr[true] != 1 {
		t.Errorf("statement durations by error = %v", byErr)
	}

	errs := m["go_shard.shard.errors"].Data.(metricdata.Sum[int64])
	if len(errs.DataPoints) != 1 || errs.DataPoints[0].Value != 1 {
		t.Fatalf("shard errors = %+v", errs.DataPoints)
	}
	if v, _ := errs.DataPoints[0].Attributes.Value("shard.id"); v.AsString() != "shard-02" {
		t.Errorf("error counted for %v, want shard-02", v)
	}

	sd := m["go_shard.shard.duration"].Data.(metricdata.Histogram[float64])
	shards := map[string]uint64{}
	for _, dp := range sd.DataPoints {
		v, _ := dp.Attributes.Value("shard.id")
		shards[v.AsString()] += dp.Count
	}
	if shards["shard-01"] != 2 || shards["shard-02"] != 2 {
		t.Errorf("shard durations = %v", shards)
	}
}

type fakePools map[observe.ShardID]sql.DBStats

func (f fakePools) PoolStats() map[observe.ShardID]sql.DBStats { return f }

func TestPoolMetrics(t *testing.T) {
	r := newRig(t)
	reg, err := RegisterPoolMetrics(r.mp, fakePools{
		"shard-01": {OpenConnections: 5, InUse: 2, Idle: 3, WaitCount: 7, WaitDuration: 1500 * time.Millisecond},
		"shard-02": {OpenConnections: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reg.Unregister() }()
	m := collect(t, r)

	open := m["go_shard.pool.open"].Data.(metricdata.Gauge[int64])
	got := map[string]int64{}
	for _, dp := range open.DataPoints {
		v, _ := dp.Attributes.Value("shard.id")
		got[v.AsString()] = dp.Value
	}
	if got["shard-01"] != 5 || got["shard-02"] != 1 {
		t.Errorf("open connections = %v", got)
	}
	if w := m["go_shard.pool.wait_duration"].Data.(metricdata.Sum[float64]); len(w.DataPoints) != 2 {
		t.Errorf("wait_duration = %+v", w.DataPoints)
	}
	for _, name := range []string{"go_shard.pool.in_use", "go_shard.pool.idle", "go_shard.pool.wait_count"} {
		if _, ok := m[name]; !ok {
			t.Errorf("metric %s missing", name)
		}
	}
}
