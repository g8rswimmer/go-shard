# go-shard Project Plan

Status: Draft v1

How we build what [REQUIREMENTS.md](REQUIREMENTS.md) asks for, in the shape described in [ARCHITECTURE.md](ARCHITECTURE.md). Sizes are relative effort: S (a few days), M (about a week), L (1 to 2 weeks).

## 1. Approach

- **Walking skeleton first.** Milestones 0 to 2 deliver a thin end-to-end path (config, route by key, run on one shard, in a real test cluster). Everything later extends a working system.
- **Test support early.** `shardtest` is built in M1/M2, not at the end, because every later milestone's integration tests depend on it (FR-13).
- **Examples grow with features.** Each milestone that adds a user-visible capability also adds or updates its example, so examples are never a cleanup task (FR-12).
- **Small, reviewable steps.** Each milestone is a set of small PRs, each with tests. Interfaces at the seams (router, analyzer, executor, merger, migrator) are defined before their implementations.
- **Riskiest parts early.** The SQL analyzer (cgo) and the merge layer are the hardest. The analyzer is proven in M3, and merge gets its own milestone.

## 2. Milestones

```
M0 Scaffold -> M1 Registry+Router -> M2 Executor+shardtest (skeleton)
   -> M3 Analyzer -> M4 Writes -> M5 Transactions
   -> M6 Fan-out+Merge -> M7 Explain
   -> M8 Migrations -> M9 Observability -> M10 Hardening -> M11 Demo guide+Release
```

M8 (migrations) depends only on M2 and can run in parallel with M3 to M7 if there is more than one contributor.

### M0 Project scaffold (S)
- Go module `github.com/g8rswimmer/go-shard`, directory layout from architecture section 2.
- CI: build, `go vet`, `golangci-lint`, unit tests, race detector. Separate integration job (needs Docker).
- `Makefile` targets: `test`, `test-integration`, `lint`, `up`, `down`.
- `docker-compose.yml` with 3 Postgres shards.
- README skeleton, CONTRIBUTING (how to run tests), license.
- **Done when:** CI is green on an empty-but-compiling module and `make up` starts 3 shards.

### M1 Registry and router (M) - FR-2, FR-3
- `registry`: `Sharded`, `Colocated`, `Global`, `Validate()` (missing parent, cycles, key type mismatch). From M3, a sharded table must also declare its key type.
- `router`: key canonicalization (int, uuid, string), xxhash64, 1024 buckets, bucket-map validation, `ShardFor` / `ShardsFor` / `All`, and an `Even` helper that splits buckets across shards.
- Fixed hash test vectors so the hash can never change silently.
- **Done when:** unit tests cover all validation errors; vectors pass; bucket map with a gap or overlap is rejected.

### M2 Executor, config and test support - walking skeleton (L) - FR-1, FR-13
- `Config`, `shard.Open` (validate registry + bucket map, ping all shards), `Close`, `Health`.
- `exec`: one `*sql.DB` per shard, parallel `Query` (fail-fast) and `Exec` (best-effort) with per-shard timeout and a `MaxFanout` bound.
- Routing in M2 is explicit (`WithShardKey`, `WithShard`, `WithAllShards`); the analyzer in M3 makes it automatic.
- `Querier` interface (`Query`, `Exec`); `*shard.DB` implements it. `Begin` and `Explain` are added in M5 and M7.
- `shardtest.NewCluster` (testcontainers, plus `SHARDTEST_DSNS` mode), `Seed`, and helpers to assert where a row physically is.
- `examples/quickstart` (explicit `WithShardKey` routing until M3).
- **Done when:** an integration test inserts and reads a row by key across a 3-shard cluster, and the row is physically on the shard the router chose.

### M3 Analyzer (L) - FR-4
- `analyze`: the `Analysis` type (facts about a statement) and the `Analyzer` interface.
- `analyze/pgparse` with `pg_query_go`: tables, equality / `IN` / `ANY` conditions on the shard key, top-level `AND`, positional args, joins and `USING`, subqueries (each at its own query level), order, limit, aggregates.
- `plan.Route`: applies the routing rules (ARCHITECTURE 4.3), including colocation checks, global-table-only statements, key type coercion (the registry now requires a key type on every sharded table, so coercion always applies), and `ErrUnsupportedQuery` naming the construct.
- `query` builder (select, update, delete; where, order, limit) producing an `Analysis` directly; `QueryStatement` / `ExecStatement`.
- Overrides `WithShardKey`, `WithShard`, `WithAllShards` bypass analysis.
- Strict-mode errors (`ErrShardKeyRequired`, `ErrCrossShardJoin`, `ErrUnknownTable`).
- `docs/BUILDING.md`: cgo, cross-compilation and static builds (tested in Linux containers); CI job that builds and tests with `CGO_ENABLED=0`.
- `INSERT` routing is left to M4.
- **Done when:** a table-driven suite of 60+ SQL strings covers every routing rule in architecture 4.3 (including OR, ranges, function calls on the key, subqueries) and each yields the documented result.

### M4 Writes (M) - FR-7
- Insert routing from builder rows and raw `VALUES`; batch split per shard, parallel execution.
- Update / delete routing; reject updates to shard-key columns (`ErrShardKeyImmutable`).
- Global-table writes to all shards.
- `WriteResult` with per-shard outcome; optional idempotency key and its table.
- `examples/writes`.
- **Done when:** integration tests cover a batch where one shard is down (others commit, result lists the failure), and a retry with the same idempotency key applies only where it had not committed.

### M5 Transactions (S) - FR-8
- `Begin` with `ForKey` / `ForShard`, shard pinning, `ErrCrossShardTx`, global-table writes rejected inside a transaction.
- `examples/colocation` (profile + addresses in one transaction, colocated join).
- **Done when:** a rollback test shows profile and addresses roll back together, and a statement targeting another shard fails without side effects.

### M6 Fan-out and merge (L) - FR-5, FR-6
- `Merger` interface; steps: ordered k-way merge (with hidden order columns), limit/offset pushdown, `DISTINCT`, `COUNT`/`SUM`/`MIN`/`MAX`, `AVG` rewrite, `GROUP BY` re-aggregation, `HAVING` after merge.
- Streaming per-shard rows, `MaxMergeRows`, `ErrMergeLimitExceeded`.
- Failure policy: fail-fast and `AllowPartial` with `[]ShardError`.
- Reject unsupported constructs with clear errors (window functions, subqueries, cross-shard joins).
- Property test: the same generated queries on one combined database and on 3 shards must return the same results.
- `examples/fanout`.
- **Done when:** the property test passes for the supported subset, and memory use during a merge is flat with respect to result size up to `MaxMergeRows`.

### M7 Explain (S) - FR-9
- `Plan` rendering: strategy, targets, reason, merge steps, rewritten shard SQL.
- Optional per-shard Postgres `EXPLAIN`; `ANALYZE` opt-in.
- `shardtest.NewFake` and assertions (`AssertRoutes`, `AssertSingleShard`, `AssertFanout`, `LastPlan`).
- `examples/explain`, `examples/testing`.
- **Done when:** golden-file tests for Explain output of single, multi and all-shard queries, and the unit-test example runs without Docker.

### M8 Migrations (M) - FR-10
- `Migrator` interface; `golang-migrate` implementation, one instance per shard, shared source.
- Parallel apply with advisory lock per shard; `HaltOnFailure` / `ContinueOnFailure`.
- `Status` with drift detection; failed shard stays at its last good version and is re-runnable.
- `shardtest.WithMigrations`.
- `examples/migrations`.
- **Done when:** an integration test fails a migration on one shard, shows drift in `Status`, fixes it, re-runs, and ends with all shards at the same version.

### M9 Observability (S) - FR-11
- `observe.Hooks` wired into plan, per-shard execution and merge; `observe.Slog`.
- `observe/otel`: spans and metrics (query latency, fan-out width, errors per shard).
- `Health` returns per-shard status and pool stats.
- **Done when:** tests assert hook call order for single and fan-out queries, and an otel test checks span attributes (shards, strategy).

### M10 Hardening (M)
- Failure-injection pass: stop a shard mid-query, kill connections, slow shards hitting timeouts, context cancellation mid-merge; check for goroutine leaks (`goleak`).
- Benchmarks: routing overhead versus a direct `database/sql` call; merge throughput.
- Documentation: README quick start, supported/unsupported query reference, error reference, upgrade notes; godoc on all exported symbols.
- Confirm every example runs in CI; versioning policy and changelog.
- Resolve the remaining open decisions in section 5 below.
- **Done when:** failure-injection tests and benchmarks pass and the docs are complete.

### M11 Demo guide and v0.1.0 release (S) - FR-14
- Write `docs/DEMO.md`, a step-by-step guide for testing and demonstrating the library by hand against the local 3-shard cluster (details in REQUIREMENTS FR-14).
- Add a `make demo-setup` target that starts the shards, applies migrations and loads the demo dataset; `make demo-reset` returns to a clean state.
- A person who did not write the library runs the guide end to end on a clean machine; fix every step that is unclear or fails.
- Add a CI job that executes the guide's commands (extracted from the doc) so it cannot go stale.
- Tag `v0.1.0`.
- **Done when:** a reviewer completes the demo guide without help and the release checklist in section 6 is fully ticked.

## 3. Dependencies between milestones

| Milestone | Needs | Reason |
|---|---|---|
| M2 | M1 | Executor targets shards the router picks |
| M3 | M1 | Analyzer uses the registry |
| M4, M5, M6 | M2, M3 | Need real execution and query analysis |
| M6 | M4 not required | Reads and writes are independent |
| M7 | M3, M6 | Explain describes analysis and merge steps |
| M8 | M2 | Needs the shard pool and `shardtest` |
| M9 | M2 | Hooks are wired into the executor; fuller after M6 |

## 4. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| `pg_query_go` (cgo) complicates builds and cross-compilation | Users cannot build easily | Prove it in M3; isolate behind `Analyzer`; document build requirements; builder path works without parsing |
| Merge semantics differ subtly from single-database results (ordering ties, NULL ordering, collations, numeric types) | Wrong results | Property test against a combined database; document NULL ordering and collation requirements (all shards must share them); reject what cannot be reproduced |
| Hash or canonicalization changes after release | Data unreachable | Fixed test vectors in M1; hash version recorded in docs; any change is a breaking release |
| Fan-out hurts shard load | Slow or overloaded shards | `MaxFanout` bound, explicit `WithAllShards()`, Explain shows fan-out width |
| Docker-based tests are slow or flaky in CI | Slow feedback, ignored failures | Reuse one cluster per package; separate integration job; `SHARDTEST_DSNS` mode for CI-provided Postgres |
| Scope creep toward joins and distributed transactions | v1 never ships | Both are non-goals; changes go through REQUIREMENTS section 8 |
| Raw SQL support grows without bound | Maintenance burden | Supported subset is documented and enforced; unsupported constructs fail loudly |

## 5. Open decisions to settle before M11

Decided: minimum Go 1.26 and PostgreSQL 14+.

(Carried from architecture section 11.)

- Whether `observe/otel` is a separate Go module. Decide by M9.
- Builder API shape: fluent builder in v1, typed repositories later. Confirm in M3 review.
- Where the idempotency-key table is created. Decide in M4.

## 6. Quality gates

Every milestone:

- Unit tests for new logic; integration tests for new behavior touching Postgres.
- `go vet`, lint and `go test -race` pass.
- Godoc on exported symbols; the relevant example added or updated and building in CI.
- Docs updated if a requirement or design decision changed.

Release checklist for v0.1.0:

- [ ] All FR-1 to FR-14 have passing tests (traceability table in ARCHITECTURE section 10).
- [ ] Property test passes for the supported merge subset.
- [ ] Failure-injection tests pass; no goroutine leaks.
- [ ] Every example runs in CI against real Postgres.
- [ ] README quick start works on a clean machine.
- [ ] `docs/DEMO.md` completed end to end by someone other than its author, and its CI job passes.
- [ ] Supported/unsupported query reference and error reference published.
- [ ] Benchmarks recorded.
- [ ] Changelog and tag.

## 7. Suggested first steps

1. Start M0: module (`go 1.26`), CI with a Postgres 14 and latest matrix, `docker-compose.yml`.
3. Define the interfaces from ARCHITECTURE section 3 as empty packages, so M1 to M3 can proceed in parallel.
