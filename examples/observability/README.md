# Observability

Shows how to see what go-shard does: logs, traces, metrics, health.

Set `Config.Hooks` and every statement reports where it was routed, what each
shard did, how the rows were merged and how it ended.

1. **The log:** `observe.Slog(logger)` writes one line per statement, with the
   kind, strategy and target shards (and the duration, hidden here so the
   output is the same every run). A failure is logged at Error. A shard that
   fails inside a statement that still succeeds (`AllowPartial`) is logged at
   Warn, since the caller never sees it. Add `observe.WithSQL()` to include
   the SQL text, `observe.WithShardEvents()` for a line per shard. Arguments
   are never logged.
2. **Spans:** `otel.New()` (package `observe/otel`) makes each statement a span
   (`shard.query` or `shard.exec`) with `shard.strategy`, `shard.targets`,
   `shard.fanout` and `shard.reason`, a child `shard.shard` span for each shard,
   and a `merge` event on a fan-out query. They nest under the span in the
   context you pass to `Query` / `Exec`.
3. **Metrics:** `go_shard.statement.duration`, `go_shard.shard.duration`,
   `go_shard.fanout.width` and `go_shard.shard.errors`.
   `otel.RegisterPoolMetrics` adds gauges for each shard's connection pool.
4. **Health and pools:** `db.Health(ctx)` pings every shard and returns its
   latency and pool statistics. `db.PoolStats()` returns the statistics alone,
   without contacting the shards, so it is cheap enough for every scrape.

`observe.Multi(...)` runs several hooks together, as this example does with the
log and OpenTelemetry. To write your own, embed `observe.Nop` and override
what you need; see the `observe` package documentation for the order of the
calls and what the times mean (a query's time is until its rows are ready).

A real program gives OpenTelemetry an exporter (OTLP, ...); this example keeps
spans and metrics in memory so it can print them.

```sh
make up                              # three local Postgres shards
go run ./examples/observability
```

Output:

```
== 1. the log: one line per statement
level=DEBUG msg="shard statement" kind=query strategy=single targets=shard-03
level=DEBUG msg="shard statement" kind=query strategy=all targets=shard-01,shard-02,shard-03
level=DEBUG msg="shard statement" kind=exec strategy=single targets=shard-03
level=ERROR msg="shard statement failed" kind=query error="shard: no shard key: the query on \"ex_players\" does not say which shard it belongs to; add `id = $n` or `id IN (...)` as an AND-ed condition, or use db.WithShardKey / db.WithAllShards"
  (the caller got: true)

== 2. the spans: the fan-out query, with a child span per shard
  shard.query  strategy=all fanout=3 targets=["shard-01","shard-02","shard-03"]
    shard.shard  shard.id=shard-01
    shard.shard  shard.id=shard-02
    shard.shard  shard.id=shard-03
    event merge

== 3. the metrics
  go_shard.statement.duration    4 recorded
  go_shard.shard.duration        5 recorded
  go_shard.fanout.width          3 recorded, 5 shards in total

== 4. health and pool statistics
  shard-01: healthy=true
  shard-02: healthy=true
  shard-03: healthy=true
  shard-01: max connections 10, in use 0
  shard-02: max connections 10, in use 0
  shard-03: max connections 10, in use 0
```
