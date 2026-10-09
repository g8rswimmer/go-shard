# Operating go-shard: timeouts, failures and capacity

How the library behaves when shards are slow, down or abandoned, and what to
set. Every behaviour below is pinned by a failure-injection test
(`failure_integration_test.go`) that also checks no goroutine is left running.

## Timeouts

- **`Config.ShardTimeout`** limits each shard's work for one statement
  (default: none, the caller's context still applies). For a **query** it is
  the time the shard has to *start answering*, and then the time to *read its
  rows*, counted from when the query returns to you. A shard that has answered
  waits for the slowest one without its clock running, so a slow shard cannot
  use up the time the fast ones have for being read. For a **write** it is the
  whole statement. In a transaction it applies to each statement.
- **The caller's context** always applies: cancelling it cancels every shard's
  query, on the server too (PostgreSQL stops the statement, it is not left
  waiting for a lock), and a merge in progress stops reading, closes every
  shard's rows and releases the connections.
- **Always close `Rows`.** Closing releases the shard's context and connection;
  closing early, without reading the rest, is fine.

## What happens when a shard misbehaves

| Situation | What you see |
|---|---|
| A shard hangs (a lock, a slow statement) past `ShardTimeout` | The query fails with a `*ShardError` for that shard wrapping `context.DeadlineExceeded`, after about the timeout; the other shards' queries are cancelled. With `shard.AllowPartial(ctx)` you get the other shards' rows and `shard.ShardErrors` names the slow one. A write on all shards applies on the healthy ones and reports the slow one in its `WriteResult`. |
| A shard is stopped or unreachable | `*ShardError` for that shard, promptly (a connection error, not a hang). Statements for other shards are unaffected. `db.Health(ctx)` reports which shard is down. |
| A connection is killed while a query runs | `*ShardError` for the shard; the query does not hang. |
| Idle connections are killed (a failover, a firewall) | The pool may hand out a dead connection once: that statement fails with a `*ShardError`, and the next one works. The library does not retry for you (see below). |
| The caller cancels | `context.Canceled`, promptly, and the connections go back to the pool. |
| The caller walks away from a result | `rows.Close()` releases everything; unread rows cost nothing. |

A query on several shards **fails fast** by default (the first error cancels
the rest); a write is **never** failed fast, so you always see which shards
applied it. See [ERRORS.md](ERRORS.md).

## Retries

The library does not retry. A retry that quietly hides a shard failure is a
decision for the caller:

- **Reads** are safe to retry yourself, once, when you get a `*ShardError`
  that is a connection error (for example after a failover).
- **Writes** are retried safely with `shard.WithIdempotencyKey`: a retry
  applies the write only on the shards where it did not already happen.

## Capacity

- `ShardConfig.MaxConns` (default 10) caps open connections to each shard.
  A query on N shards uses one connection on each of them for as long as its
  rows are open, so size `MaxConns` for your concurrency on the busiest shard,
  not for the total.
- `Config.MaxFanout` (default 8) bounds how many shards one statement contacts
  at once; wider statements run in waves.
- `Config.MaxMergeRows` (default 100,000) bounds the memory of aggregates,
  `GROUP BY` and `DISTINCT`; past it the query fails with `ErrMergeLimitExceeded`
  instead of exhausting memory. Ordered merges hold one row per shard.
- A deep `OFFSET` makes every shard return `offset + limit` rows; page with a
  keyset condition (`WHERE id > $last ORDER BY id LIMIT n`).
- `db.PoolStats()` returns each shard's pool statistics without contacting it
  (`observe/otel` exposes them as gauges); `db.Health(ctx)` pings every shard
  and reports latency and statistics.

## Watching it

Set `Config.Hooks` ([examples/observability](../examples/observability)). The
log line and the span of a statement carry the strategy, the shards it ran on
and its duration; a shard that failed inside a statement that still succeeded
(`AllowPartial`) is logged at Warn, since the caller never sees it.
