# Changelog

All notable changes are listed here, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com), and the project follows
[semantic versioning](docs/VERSIONING.md).

## [Unreleased]

The first release, v0.1.0, is being prepared (see
[the project plan](docs/PROJECT_PLAN.md)). Everything below is new in it.

### Added

- **Topology and routing.** `shard.Open` with a `Config` of shards, bucket
  ranges, fan-out, timeout and merge limits; a registry of sharded, colocated
  and global tables; a stable router (`xxhash64` over 1024 virtual buckets).
- **Query API.** Raw SQL routed by PostgreSQL's own parser (cgo), a builder in
  package `query` that needs no parser, and explicit routes `WithShardKey`,
  `WithShard`, `WithAllShards`. Statements without a shard key are refused.
- **Fan-out and merge.** `ORDER BY`, `LIMIT`/`OFFSET`, `DISTINCT`, `COUNT`,
  `SUM`, `MIN`, `MAX`, `AVG`, `GROUP BY`, `HAVING` across shards, with bounded
  memory (`MaxMergeRows`) and opt-in partial results (`AllowPartial`). Checked
  against a single combined database by a property test.
- **Writes.** Multi-row `INSERT` split by shard, per-shard results
  (`WriteResult`), idempotency keys for safe retries.
- **Transactions.** Single-shard transactions (`Begin`, `InTx`) that refuse
  statements for another shard.
- **Explain.** `Explain` / `ExplainStatement`, a configured `Explainer` for
  PostgreSQL's own plan per shard (`ShardPlans`, `Analyze`), and `shard.Planner`
  for routing with no connections.
- **Migrations.** Package `migrate`: parallel apply with a halt or continue
  policy, `Status` with drift detection, failed shards left re-runnable.
- **Observability.** `Config.Hooks` with `observe.Slog` and `observe/otel`
  (spans, metrics, pool gauges); `DB.Health` and `DB.PoolStats`.
- **Testing support.** `shardtest.NewCluster` (containers or your own
  databases), `shardtest.NewFake` with routing assertions.
- **Examples** for every feature, run against real PostgreSQL in CI.
- **Documentation:** supported and unsupported queries, error reference,
  operations guide, benchmarks, versioning policy.

### Fixed during hardening (M10)

- **`ShardTimeout` and slow shards.** The timeout started when each shard's
  query started, so a slow shard used up the time the faster shards had for
  being read: with `AllowPartial`, a shard that timed out also expired the
  rows of the shards that had answered. The clock of a shard that has answered
  now waits for the others, and starts again when the rows are ready to read.
- **Ordered merge speed.** Comparing integers and floats no longer goes through
  exact decimal arithmetic: ordering by an integer column across shards is
  5-6 times faster and allocates a fifth as much (see
  [benchmarks](docs/BENCHMARKS.md)).
