# go-shard Requirements

Status: Draft v1.1

`go-shard` is a Go library for maintaining a sharded PostgreSQL database. This document is the reference for what the library must do. Architecture (`ARCHITECTURE.md`) and the project plan (`PROJECT_PLAN.md`) follow from it.

## 1. Goals and principles

- **Easy to understand and maintain:** small public API, explicit behavior, few concepts.
- **Metadata-driven routing:** the library decides the target shard(s) from declared metadata. The caller should rarely name a shard.
- **Safe by default:** ambiguous queries fail rather than silently hit every shard.
- **Postgres semantics preserved** wherever possible.

## 2. Decisions

| Area | Decision |
|---|---|
| Engine | PostgreSQL only for v1 (`database/sql` + pgx), isolated behind an interface |
| Shard routing | Hash of shard key over a fixed number of virtual buckets mapped to shards |
| Query API | Schema registry + query builder/repository as the primary path. Raw SQL also accepted and parsed with `pg_query_go`, with explicit overrides |
| v1 scope | Single-shard transactions, best-effort cross-shard writes, global tables, basic observability |

## 3. Non-goals (v1)

- **Resharding/rebalancing:** deferred; routing is designed so it can be added later.
- **Distributed transactions (2PC/sagas):** needs a coordinator, a durable transaction log and crash recovery. Use single-shard transactions and colocation instead.
- **Cross-shard joins:** needs a join execution engine and planner. Use colocation, global tables, or two queries joined in application code instead.
- Engines other than Postgres, and running as a proxy/server process.

See section 8 for what adding these later would involve.

## 4. Functional requirements

### FR-1 Shard topology and connections
- Configure N shard instances, each with its own DSN and pool settings.
- Shards have stable IDs. Topology is loaded from config at startup.
- Per-shard connection pooling, health check, and graceful close.

### FR-2 Schema registry (metadata)
- Each table is declared once as one of:
  - *sharded*: has a shard key and the key's type (int, uuid or string).
  - *colocated*: has a parent table and the same shard key; it inherits the key type.
  - *global*: replicated to every shard.
- The key type is required because routing hashes a key by its type: without it the number 42 and the text "42" would reach different shards, and a service passing IDs as strings (an HTTP path parameter, say) would silently read and write the wrong shard. With it, keys found in SQL or arguments are converted to the declared type before hashing.
- The registry validates itself at startup (colocated parent exists, key columns and key type present, no cycles).

```go
registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt))
registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id"))
registry.Global("countries")
```

### FR-3 Routing
- Single key: one shard.
- Key set (`IN`): only the shards those keys hash to.
- No shard-key predicate: fan out to all shards, only if explicitly allowed.
- The routing function is a pluggable interface; the default is hash over virtual buckets.
- The routing decision is deterministic and inspectable (see FR-9).

### FR-4 Query API
- Primary: query builder / typed repository API driven by the registry.
- Also accepts raw SQL. Table(s) and shard-key predicates are extracted with `pg_query_go`.
- Overrides: `WithShardKey(k)`, `WithShard(id)`, `WithAllShards()`.
- If routing cannot be determined, return a clear error (strict mode, the default).

### FR-5 Fan-out reads and result merging
- Execute on selected shards in parallel with context cancellation and timeouts.
- Merge results: ordered merge for `ORDER BY`, global `LIMIT`/`OFFSET`, `DISTINCT`, and aggregates (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG` via sum + count), with `GROUP BY` and `HAVING`.
- Rows are streamed: an ordered merge holds one row per shard however large the result. What must be remembered (groups, distinct rows) is bounded by a configurable limit and fails rather than exhausting memory.
- Supported and unsupported constructs are defined explicitly (ARCHITECTURE 5.5). Cross-shard joins, window functions and subqueries are rejected with a clear error.
- The error for a rejected cross-shard join names the alternatives: colocation, global tables, or two queries joined in application code.
- The merge layer takes a plain description of how to combine the shards' rows (a spec) rather than SQL, so a join-capable implementation can be added later without changing callers.
- Partial-failure policy for fan-out reads: fail-fast by default, with an optional allow-partial mode (`shard.AllowPartial(ctx)`) that merges the shards that answered and reports the others per shard (`shard.ShardErrors`).
- Known differences from a single database are documented: text is ordered bytewise (as the `C` collation), and a floating-point sum may differ in its last digits.

### FR-6 Colocation
- Rows of colocated tables share the parent's shard key and therefore its shard.
- Joins between colocated tables on the shard key run on a single shard in one SQL statement.
- Example: a `profile` and its `addresses` always live together.

### FR-7 Writes
- Single-row writes route by shard key. The key must be present as a literal or parameter, otherwise the write is refused before anything is written.
- Batch writes (an INSERT with many rows) are split per shard and run in parallel; each shard receives only its own rows.
- Cross-shard writes are best-effort with no atomicity guarantee. The result reports success or failure per shard, and per row group: for an INSERT, which rows each shard received, so the rows of a failed shard can be sent again.
- Writes to global tables go to all shards, with a per-shard result report.
- Shard-key columns are immutable. An update, or an upsert, that changes a shard key is rejected.
- Writes accept an optional idempotency key so retries (and future sagas) are safe: a retry applies the write only on shards where it had not been applied, and a key cannot be reused for a different statement. The key is recorded in the same transaction as the write.

### FR-8 Transactions
- Full ACID transaction scoped to one shard. All statements must route to the same shard, and this is enforced.
- A statement that belongs to a second shard fails with a clear error before anything is sent, and the transaction stays usable.
- A transaction can be started from a shard key (converted to the table's key type), a table and key, or a shard name; it can be read-only and take an isolation level.
- A transaction is a `Querier`, so code written against it runs unchanged inside one. A helper commits or rolls back automatically (also on a panic), so a transaction cannot be left open.
- Reads of global tables work inside a transaction; writes to global tables are refused because they touch every shard.

### FR-9 Query plan / explain
- `Explain()` returns the routing decision: target shards, why, strategy (single / multi / all), and merge steps.
- Optionally includes each shard's Postgres `EXPLAIN` output (`ShardPlans` option).
- Does not execute the query unless `ANALYZE` is requested (`Analyze` option); a write measured that way runs in a transaction that is rolled back.
- Reports the same refusals as running the statement would, so it can be used to check a query before shipping it.

### FR-10 Migrations
- Versioned migrations are applied to every shard and tracked per shard in a migrations table.
- Reports per-shard status and detects drift (shards at different versions).
- A failed shard is reported and re-runnable (idempotent). Policy is defined for halt-on-first-failure vs continue.
- Covers both sharded-table DDL and global-table data.
- Forward-only (no down migrations). Concurrent runs against a shard take turns. A migration is atomic, so a failed shard stays at its last good version; a shard a crash left dirty is reported and can be repaired explicitly.

### FR-11 Observability and health
- Per-shard health and pool stats, structured logging, and tracing/metrics hooks (OpenTelemetry-friendly).
- Includes the routing decision and per-shard latency.

### FR-12 Examples
- Runnable examples show each major capability, so a new user can copy one and have it work:
  - quick start (configure shards, register tables, insert, query by key)
  - colocation (profile and addresses in one shard, single-shard transaction)
  - fan-out (opt-in all-shards query with `ORDER BY`, `LIMIT` and aggregates)
  - batch writes and handling a per-shard partial failure
  - migrations across shards, including status and drift
  - `Explain()` output
  - writing tests against the library (see FR-13)
- A `docker-compose` file starts the local shard instances the examples need.
- Examples are compiled in CI and run against real Postgres instances, so they cannot go stale.
- Each example has a short README stating what it shows and how to run it.

### FR-13 Testing support for library users
- A public test-support package lets users test their own code against multiple shards:
  - starts N throwaway Postgres shards, applies the user's migrations, and cleans up
  - can use existing instances instead (via environment variables), for CI systems that provide their own
  - helpers to seed data per shard and to assert which shard(s) a query routes to
  - an in-memory fake that needs no database, for unit tests that only care about routing and call shape
- The public client is exposed as an interface so users can substitute a fake in their own tests.
- The library's own integration tests use the same package, so it is exercised continuously.

### FR-14 Demo guide
- A `docs/DEMO.md` guide lets a person test and demonstrate the library by hand, without reading the code.
- Setup is one command (`make demo-setup`): starts the local shards, applies migrations, loads a known dataset. `make demo-reset` returns to that state.
- Each scenario states the command to run, the expected output, and what it proves. Scenarios cover:
  - a query by shard key hits exactly one shard (shown with `Explain()` and by looking at the shard directly)
  - a profile and its addresses are on the same shard
  - an opt-in all-shards query returns correctly sorted, limited and aggregated results
  - a query with no shard key is refused with a helpful error
  - a batch write when one shard is stopped: the other shards commit and the failure is reported
  - a single-shard transaction rolls back cleanly, and a second shard is refused
  - a migration that fails on one shard, shows drift, and recovers when re-run
  - health and logs/traces showing per-shard activity
- Each scenario has a pass/fail checklist, so the guide doubles as a manual acceptance test before a release.
- The commands in the guide are run in CI so the guide stays accurate.

## 5. Non-functional requirements

- **Maintainability:** small packages with clear boundaries, interfaces at seams (router, executor, merger, migrator), idiomatic Go, `context.Context` everywhere.
- **Testability:** unit tests without a DB; integration tests against multiple real Postgres instances (docker/testcontainers).
- **Performance:** single-shard routing overhead is negligible next to query time. Fan-out is bounded by a configurable concurrency limit. Merge streams where possible to bound memory.
- **Safety:** no silent scatter-gather, no silent partial writes.
- **Compatibility:** Go 1.26 or newer; PostgreSQL 14 or newer. CI tests the oldest and newest supported Postgres versions.
- **Documentation:** README with a quick start. Each supported/unsupported query construct is documented. Examples, test support and the demo guide are covered by FR-12, FR-13 and FR-14.

## 6. Additional recommended requirements

- **Shard-key hygiene:** immutable keys; key type normalization so int/uuid/string hashing is stable across restarts and versions.
- **ID generation:** globally unique IDs that embed or hash to the shard (e.g. UUIDv7 or snowflake) so inserts can be routed.
- **Unique constraints:** uniqueness is only enforced per shard unless the unique column includes the shard key. Document this and validate in the registry.
- **Retry/timeout policy:** per-shard timeouts, and safe retries for idempotent reads only.
- **Read replicas:** deferred; leave a seam in the shard config.
- **Config:** loadable from struct/env/file; secrets are not logged.

## 7. Open questions (for the architecture phase)

- Virtual bucket count, and where the bucket-to-shard assignment is stored (static config vs table).
- Exact supported SQL subset for merge (aggregates with `GROUP BY`/`HAVING`).
- Migrations: build our own vs wrap an existing tool (golang-migrate/goose) per shard.

## 8. Future / v2 considerations

Out of scope for v1, recorded so the design leaves room for them.

### Cross-shard joins
- Strategies: broadcast (copy the small side), repartition (re-hash both sides by join key), coordinator-side (join in Go).
- A planner that picks a strategy from table statistics (e.g. `pg_class`, `ANALYZE`).
- Predicate and projection pushdown to shards.
- Memory bounds and spill-to-disk for hash joins.
- Join types (inner/left/right/full) and non-equality `ON` conditions.
- Wider SQL coverage: subqueries, `GROUP BY` / `ORDER BY` after a join.
- `Explain()` output showing strategy, data movement and estimated rows.

### Distributed transactions
- Two-phase commit via `PREPARE TRANSACTION` (requires `max_prepared_transactions > 0` on every shard).
- Durable coordinator log so commit decisions survive a crash.
- Startup recovery of orphaned prepared transactions (they hold locks and block vacuum).
- Documented weaker isolation: no global snapshot without a timestamp oracle.
- Cross-shard deadlock and timeout policy.
- Fault-injection tests for shard crash, coordinator crash and partition at each phase.
- Alternative: sagas with compensating actions (eventual consistency; relies on idempotent writes, see FR-7).

### Resharding / rebalancing
- Adding shards and moving virtual buckets with data, built on the fixed virtual-bucket design in FR-3.
