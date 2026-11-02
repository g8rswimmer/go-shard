# Changelog

All notable changes are listed here, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com), and the project follows
[semantic versioning](docs/VERSIONING.md).

## [Unreleased]

Nothing yet.

## [0.2.0] - 2026-11-01

**No library code changed in this release.** The root package and every package
under it are as in 0.1.0; this release is the examples, the demo guide, and the
files that help people and coding agents work on the repository. Nothing here
affects code that imports go-shard.

### Added

- **Guidance for contributors and coding agents:** `AGENTS.md` (code map,
  commands, scope, checklist), `docs/CHANGING.md` (how to make common changes),
  `docs/DECISIONS.md`, `docs/README.md` (an index by question), `make check`,
  issue and PR templates, `CODEOWNERS` and `SECURITY.md`.
- `make check`: build, vet, lint, and the tests with and without cgo, without
  Docker.

### Changed

- **The demo uses its own table names.** `examples/demo` now creates
  `demo_profiles`, `demo_addresses` and `demo_countries`, so `make demo-reset`
  cannot drop tables of the same name in a database it was pointed at by
  mistake. `make demo-reset` no longer drops the idempotency table. See
  [UPGRADING.md](docs/UPGRADING.md) if you ran the 0.1.0 demo.
- `go run ./examples/demo batch` refuses to run when `SHARD_DSNS` is set, since
  it stops and starts a `docker compose` service by name.

### Fixed

- `examples/demo`: the batch scenario restores the stopped shard, waits for it
  to answer, and only then deletes its rows, even after an error; errors from
  its count queries are returned instead of exiting the process.
- `examples/adopt`: the loader fails with an error on a non-integer key instead
  of re-reading the first batch until the timeout, and its helpers return
  errors instead of exiting.
- The guide test rejects code fences it would otherwise skip silently, so a
  block in `docs/DEMO.md` cannot go unchecked.

## [0.1.0] - 2026-10-10

The first release (see [the project plan](docs/PROJECT_PLAN.md)).

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
- **Adoption guide** ([docs/ADOPTING.md](docs/ADOPTING.md)) and the
  `examples/adopt` loader: choosing shard keys, colocation, loading existing
  data and verifying it.
- **Demo guide** ([docs/DEMO.md](docs/DEMO.md)): eight scenarios to try by hand
  against three local shards, with `make demo-setup`, `make demo-reset` and
  `make demo-check`, which runs the guide in CI and compares its output.
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

[Unreleased]: https://github.com/g8rswimmer/go-shard/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/g8rswimmer/go-shard/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/g8rswimmer/go-shard/releases/tag/v0.1.0
