// Package observe defines hooks for logging, tracing and metrics.
//
// A Hooks value, set as Config.Hooks, is told about every statement a
// shard.DB runs: where it was routed, what each shard did and how long it
// took, how the rows were merged, and how the statement ended. Two ready-made
// implementations are provided: Slog, for structured logs, and the
// observe/otel package, for OpenTelemetry spans and metrics. To write your own,
// embed Nop and override what you need.
//
// For one statement the hooks are called in this order:
//
//	OnPlan                                  routing, always
//	OnShardStart, OnShardDone               once per shard contacted, in parallel
//	OnMerge                                 a query on several shards only
//	OnDone                                  always, last
//
// A statement that cannot be routed has OnPlan (with Err set) and OnDone only.
// OnShardStart for a shard always comes before its OnShardDone, and both get
// the context OnShardStart returned. Hooks run on the caller's goroutine, and
// on the shard goroutines for the shard events, so an implementation must be
// safe for concurrent use and fast: it is on the path of every statement.
//
// What the times mean: a query's time is how long the shard took to start
// answering (PostgreSQL has run the statement and the first rows are ready),
// not how long the caller then takes to read them. A write's time is the whole
// statement. Statement arguments are never passed to hooks, as they commonly
// hold personal data; the SQL text is, and it is only logged or attached to
// spans if you ask for that.
package observe
