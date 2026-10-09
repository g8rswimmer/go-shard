// Observability shows how to see what go-shard does:
//
//   - observe.Slog logs one line per statement: where it went and how long it
//     took, and a failure at a higher level;
//   - observe/otel turns each statement into a span, with the shards it ran on
//     as child spans, and records latency, fan-out width and per-shard errors
//     as metrics;
//   - db.Health pings every shard, and db.PoolStats reports each shard's
//     connection pool without contacting it.
//
// observe.Multi uses the log and the OpenTelemetry hooks together. A real
// program would give OpenTelemetry an exporter (OTLP, Jaeger, ...); this one
// keeps the spans and metrics in memory so it can print them.
//
// It uses the three local shards from `make up`. Set SHARD_DSNS (comma
// separated) to use others.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/observe/otel"
	"github.com/g8rswimmer/go-shard/registry"
)

const defaultDSNs = "postgres://shard:shard@localhost:5441/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5442/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5443/shard?sslmode=disable"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Logs go to stdout without times or durations, so the output is the same
	// every run. Real programs keep both.
	logging := &atomic.Bool{} // off while the example sets up and cleans up
	logger := slog.New(gated{logging, slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey || a.Key == "duration" {
				return slog.Attr{}
			}
			return a
		},
	})})

	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	// Delta temporality, so each Collect reports only what happened since the last.
	reader := sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(sdkmetric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	traces, err := otel.New(otel.WithTracerProvider(tp), otel.WithMeterProvider(mp))
	if err != nil {
		return err
	}

	reg, err := registry.New(registry.Sharded("ex_players", registry.Key("id"), registry.Type(registry.KeyInt)))
	if err != nil {
		return err
	}
	dsns := os.Getenv("SHARD_DSNS")
	if dsns == "" {
		dsns = defaultDSNs
	}
	cfg := shard.Config{Registry: reg, Hooks: observe.Multi(observe.Slog(logger), traces)}
	for i, dsn := range strings.Split(dsns, ",") {
		cfg.Shards = append(cfg.Shards, shard.ShardConfig{
			ID:  shard.ShardID(fmt.Sprintf("shard-%02d", i+1)),
			DSN: strings.TrimSpace(dsn),
		})
	}
	db, err := shard.Open(ctx, cfg)
	if err != nil {
		return fmt.Errorf("is `make up` running? %w", err)
	}
	defer db.Close()
	if _, err := db.WithAllShards().Exec(ctx, "DROP TABLE IF EXISTS ex_players"); err != nil {
		return err
	}
	if _, err := db.WithAllShards().Exec(ctx, "CREATE TABLE ex_players (id bigint PRIMARY KEY, name text NOT NULL)"); err != nil {
		return err
	}
	defer func() { _, _ = db.WithAllShards().Exec(context.Background(), "DROP TABLE IF EXISTS ex_players") }()
	if _, err := db.Exec(ctx, "INSERT INTO ex_players (id, name) VALUES (1, 'ann'), (2, 'bo'), (3, 'cy'), (4, 'di')"); err != nil {
		return err
	}
	spans.Reset() // keep the setup out of the output
	if err := reader.Collect(ctx, &metricdata.ResourceMetrics{}); err != nil {
		return err
	}
	logging.Store(true)

	fmt.Println("== 1. the log: one line per statement")
	statements := []struct {
		run func() error
	}{
		{func() error { return drain(db.Query(ctx, "SELECT name FROM ex_players WHERE id = $1", 1)) }},
		{func() error { return drain(db.WithAllShards().Query(ctx, "SELECT name FROM ex_players ORDER BY name")) }},
		{func() error {
			_, err := db.Exec(ctx, "UPDATE ex_players SET name = 'ann' WHERE id = 1")
			return err
		}},
		{func() error { // no shard key: refused before any shard is contacted
			err := drain(db.Query(ctx, "SELECT name FROM ex_players"))
			fmt.Printf("  (the caller got: %v)\n", errors.Is(err, shard.ErrShardKeyRequired))
			return nil
		}},
	}
	for _, s := range statements {
		if err := s.run(); err != nil {
			return err
		}
	}

	fmt.Println("\n== 2. the spans: the fan-out query, with a child span per shard")
	for _, s := range spans.Ended() {
		if s.Name() != "shard.query" || attr(s, "shard.strategy") != "all" {
			continue
		}
		fmt.Printf("  %s  strategy=%s fanout=%s targets=%s\n", s.Name(), attr(s, "shard.strategy"), attr(s, "shard.fanout"), attr(s, "shard.targets"))
		var ids []string
		for _, c := range spans.Ended() {
			if c.Name() == "shard.shard" && c.Parent().SpanID() == s.SpanContext().SpanID() {
				ids = append(ids, attr(c, "shard.id"))
			}
		}
		sort.Strings(ids)
		for _, id := range ids {
			fmt.Printf("    shard.shard  shard.id=%s\n", id)
		}
		for _, e := range s.Events() {
			fmt.Printf("    event %s\n", e.Name)
		}
	}

	fmt.Println("\n== 3. the metrics")
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		return err
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Histogram[float64]:
				var n uint64
				for _, dp := range d.DataPoints {
					n += dp.Count
				}
				fmt.Printf("  %-30s %d recorded\n", m.Name, n)
			case metricdata.Histogram[int64]:
				var n uint64
				var sum int64
				for _, dp := range d.DataPoints {
					n += dp.Count
					sum += dp.Sum
				}
				fmt.Printf("  %-30s %d recorded, %d shards in total\n", m.Name, n, sum)
			default:
				// counters appear only when something failed
			}
		}
	}

	logging.Store(false)
	fmt.Println("\n== 4. health and pool statistics")
	for _, h := range db.Health(ctx) {
		fmt.Printf("  %s: healthy=%t\n", h.ID, h.Healthy())
	}
	stats := db.PoolStats()
	ids := make([]shard.ShardID, 0, len(stats))
	for id := range stats {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		fmt.Printf("  %s: max connections %d, in use %d\n", id, stats[id].MaxOpenConnections, stats[id].InUse)
	}
	return nil
}

// gated is a log handler that can be switched off.
type gated struct {
	on *atomic.Bool
	slog.Handler
}

func (g gated) Enabled(ctx context.Context, l slog.Level) bool {
	return g.on.Load() && g.Handler.Enabled(ctx, l)
}

// drain reads every row, so the statement is finished.
func drain(rows shard.Rows, err error) error {
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		// nothing to do with the values
	}
	return rows.Err()
}

func attr(s sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String()
		}
	}
	return ""
}
