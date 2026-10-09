package shard

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/observe/otel"
	"github.com/g8rswimmer/go-shard/query"
)

// The spans of a real DB (over fake shards) carry the routing decision, and
// Slog and OpenTelemetry can be used together.
func TestOpenTelemetrySpansOfAFanOutQuery(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	hooks, err := otel.New(otel.WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	db := fakeDB(t, observe.Multi(hooks, observe.Slog(logger)), &fakeShard{rows: 1}, &fakeShard{rows: 1}, &fakeShard{rows: 1})
	rows, err := db.WithAllShards().QueryStatement(context.Background(), builtSelect(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	var statement sdktrace.ReadOnlySpan
	shardSpans := 0
	for _, s := range spans.Ended() {
		switch s.Name() {
		case "shard.query":
			statement = s
		case "shard.shard":
			shardSpans++
		default:
			t.Errorf("unexpected span %q", s.Name())
		}
	}
	if statement == nil || shardSpans != 3 {
		t.Fatalf("want a statement span and 3 shard spans, got %v and %d", statement, shardSpans)
	}
	got := map[string]string{}
	for _, kv := range statement.Attributes() {
		got[string(kv.Key)] = kv.Value.String()
	}
	if got["shard.strategy"] != "all" || got["shard.fanout"] != "3" || got["shard.targets"] != `["shard-01","shard-02","shard-03"]` {
		t.Errorf("attributes = %v", got)
	}
	if !strings.Contains(logs.String(), "strategy=all targets=shard-01,shard-02,shard-03") {
		t.Errorf("log = %q", logs.String())
	}
}

func TestOpenTelemetrySpanOfASingleShardWrite(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	hooks, err := otel.New(otel.WithTracerProvider(tp))
	if err != nil {
		t.Fatal(err)
	}
	db := fakeDB(t, hooks, &fakeShard{affected: 3}, &fakeShard{affected: 3})
	st, err := query.Update("profiles").Set("name", "x").Where(query.Eq("id", 1)).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecStatement(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	var sawStatement, sawShard bool
	for _, s := range spans.Ended() {
		got := map[string]string{}
		for _, kv := range s.Attributes() {
			got[string(kv.Key)] = kv.Value.String()
		}
		switch s.Name() {
		case "shard.exec":
			sawStatement = got["shard.strategy"] == "single" && got["shard.fanout"] == "1"
		case "shard.shard":
			sawShard = got["shard.rows_affected"] == "3"
		default:
			t.Errorf("unexpected span %q", s.Name())
		}
	}
	if !sawStatement || !sawShard {
		t.Errorf("statement span ok: %v, shard span ok: %v", sawStatement, sawShard)
	}
}
