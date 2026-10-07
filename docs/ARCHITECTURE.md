# go-shard Architecture

Status: Draft v1

This document describes how `go-shard` meets the requirements in [REQUIREMENTS.md](REQUIREMENTS.md). FR-n references point to that document.

## 1. Design summary

Every operation goes through the same five-stage pipeline:

```
Analyze  ->  Route  ->  Plan  ->  Execute  ->  Merge
(what is     (which     (shard    (run in      (combine
 this query   shards?)   list +    parallel)    results)
 about?)                 merge
                         steps)
```

- **Analyze** turns a builder query or raw SQL into a small, engine-neutral description: tables, shard-key predicates, ordering, limit, aggregates.
- **Route** uses the schema registry and the router to pick target shards.
- **Plan** produces an inspectable `Plan`. `Explain()` returns this plan without executing it (FR-9).
- **Execute** runs the statement on each target shard with bounded concurrency.
- **Merge** combines per-shard results (FR-5). For a single shard, merge is a pass-through.

Because the plan is a plain value, routing and merging are unit-testable without a database.

### Fixed decisions

| Area | Decision |
|---|---|
| Engine | PostgreSQL only, via `database/sql` + pgx stdlib driver |
| Routing | xxhash64 of the canonicalized shard key, modulo 1024 virtual buckets; bucket-to-shard map comes from static config |
| SQL analysis | `pg_query_go` (cgo), isolated behind an `Analyzer` interface |
| Migrations | Wraps `golang-migrate` per shard; fan-out, status and drift detection are ours |
| Joins / transactions | Colocated joins and single-shard transactions only (see REQUIREMENTS section 8) |

## 2. Package layout

```
go-shard/
  shard.go            Public facade: Open, DB, Query/Exec/Begin/Explain
  config.go           Config structs and validation (FR-1)
  registry/           Table metadata: sharded, colocated, global (FR-2)
  router/             Key canonicalization, hashing, bucket map (FR-3)
  analyze/            Builder + raw SQL -> Analysis (FR-4)
    pgparse/          pg_query_go adapter (only package that imports cgo)
  plan/               Plan type, strategies, Explain rendering (FR-9)
  exec/               Shard pool, parallel executor, tx pinning (FR-1, FR-8)
  merge/              Ordered merge, limit/offset, distinct, aggregates (FR-5)
  write/              Row splitting, per-shard results, idempotency (FR-7)
  migrate/            Fan-out migration runner, status, drift (FR-10)
  observe/            Hooks interface, slog adapter; otel/ subpackage (FR-11)
  shardtest/          Public test support: containers, fake, assertions (FR-13)
  examples/           Runnable examples, one directory each (FR-12)
  docker-compose.yml  3 local Postgres shards for examples and the demo
  docs/
```

Dependency direction (arrows mean "imports"). Nothing imports the facade, and only `analyze/pgparse` imports cgo:

```
shard -> plan, exec, merge, write, migrate, observe
plan, write -> router, registry, analyze
exec, merge, migrate -> (types from plan/registry only)
analyze -> registry
analyze/pgparse -> cgo (pg_query_go)
```

## 3. Core types and interfaces

Small interfaces sit at the seams that are expected to change.

```go
// registry
type TableKind int // Sharded, Colocated, Global
type Table struct {
    Name     string
    Kind     TableKind
    KeyCol   string // empty for Global
    Parent   string // for Colocated
    Group    string // colocation group, derived from root parent
}
type Registry interface {
    Table(name string) (Table, bool)
    Validate() error
}

// router
type ShardID string
type Router interface {
    ShardFor(key any) (ShardID, error)       // canonicalize, hash, bucket, shard
    ShardsFor(keys []any) ([]ShardID, error)
    All() []ShardID
}

// analyze: engine-neutral description of a statement
type Analysis struct {
    Op         OpKind // Select, Insert, Update, Delete
    Tables     []TableRef
    KeyValues  map[string][]any // table -> shard-key values found (nil = none)
    Joins      []JoinInfo       // equality joins, used for colocation checks
    OrderBy    []OrderTerm
    Limit, Offset *int64
    Aggregates []AggregateTerm
    GroupBy    []string
    Distinct   bool
}
type Analyzer interface {
    FromSQL(sql string, args []any) (Analysis, error)
}
// builder queries produce Analysis directly, without parsing.

// plan
type Strategy int // Single, Multi, All
type Plan struct {
    SQL      string       // possibly rewritten (e.g. AVG -> SUM+COUNT)
    Args     []any
    Targets  []ShardID
    Strategy Strategy
    Reason   string       // why these shards (for Explain)
    Merge    []MergeStep  // empty for Single
}

// exec
type Executor interface {
    Run(ctx context.Context, p Plan) (ShardRows, error)
}

// merge: behind an interface so a join-capable version can be added (FR-5)
type Merger interface {
    Merge(ctx context.Context, steps []MergeStep, in ShardRows) (Rows, error)
}
```

## 4. Request flow

### 4.1 Read by shard key (single shard)

```
db.Query(ctx, sql, args...)
  -> Analyzer.FromSQL            tables=[profiles], key profile_id=$1
  -> Router.ShardFor(args[0])    shard-03
  -> Plan{Strategy: Single, Targets: [shard-03]}
  -> Executor.Run                one query, rows returned as-is
```

### 4.2 Fan-out read (all shards)

Only allowed when the caller opts in with `WithAllShards()`; otherwise the router returns `ErrShardKeyRequired` (FR-3, FR-4).

```
SELECT ... ORDER BY created_at DESC LIMIT 20   (WithAllShards)
  -> Plan{Strategy: All, Merge: [OrderedMerge(created_at DESC), Limit(20)]}
  -> shard SQL rewritten to LIMIT 20 (offset + limit pushed down)
  -> Executor runs on all shards in parallel
  -> Merger k-way merges sorted streams, stops at 20 rows
```

### 4.3 Routing rules (analysis of predicates)

| Predicate on shard key | Result |
|---|---|
| `key = $1` | Single shard |
| `key IN ($1, $2, ...)` | The distinct shards those keys hash to (Multi) |
| Top-level `AND` containing either of the above | Same as above |
| `OR`, function calls on the key, ranges, subqueries | Not determinable: error unless `WithAllShards()` / `WithShardKey()` |
| No predicate | Error unless `WithAllShards()` |
| Only global tables referenced | Any single shard (round-robin), since all shards hold the data |

Joins are allowed only if every non-global table is in the same colocation group and joined to its parent on the shard key. Otherwise: `ErrCrossShardJoin` with the alternatives named (FR-5, FR-6).

## 5. Components

### 5.1 Registry (FR-2)

- Built in code at startup (`registry.Sharded`, `Colocated`, `Global`).
- `Validate()` checks the parent exists, key columns are declared, there are no cycles, and a colocated table uses the same key type as its root. Group is the root ancestor's name.
- Immutable after `Open`, so it is safe for concurrent use without locks.

### 5.2 Router (FR-3)

- **Canonical key bytes:** integers as 8-byte big-endian `int64`, UUIDs as their 16 bytes, strings as UTF-8. Stable across restarts and versions, and unit-tested with fixed vectors.
- **Hash:** `xxhash64(canonical) % 1024` gives a bucket.
- **Bucket map:** config lists bucket ranges per shard. Startup validation requires every bucket in `[0,1024)` to be assigned exactly once.
- The bucket count is fixed so that adding resharding later moves buckets, not every key.
- `Router` is an interface; the default implementation is the only one in v1.

### 5.3 Analyzer (FR-4)

- **Builder path:** the builder already has table, predicates, order and limit, so it fills `Analysis` directly. This is the primary path.
- **Raw SQL path:** `pgparse` parses with `pg_query_go`, walks the tree and fills `Analysis`. Positional args (`$n`) are resolved against the supplied args.
- Anything outside the supported subset (see 5.5) yields an `ErrUnsupportedQuery` that names the construct.
- Only `analyze/pgparse` imports cgo. Building with a different parser means implementing `Analyzer`.
- Overrides `WithShardKey`, `WithShard`, `WithAllShards` bypass predicate analysis and are recorded in `Plan.Reason`.

### 5.4 Executor and shard pool (FR-1, FR-5)

- One `*sql.DB` per shard, with its own pool settings.
- `Run` fans out with an `errgroup` and a semaphore sized by `MaxFanout`, so a wide query cannot open unbounded work.
- Each shard call gets a derived context with the per-shard timeout; the caller's context cancels everything.
- **Failure policy:** fail-fast by default (first error cancels the rest). `AllowPartial()` returns rows plus a `[]ShardError`.
- Reads can be retried once on connection errors. Writes are never retried unless an idempotency key is supplied (FR-7).
- Rows are streamed per shard so the merger can start before all shards finish.

### 5.5 Merger (FR-5)

Supported merge steps:

| Step | How |
|---|---|
| `ORDER BY` | K-way merge using a heap over the per-shard sorted streams. Order columns missing from the select list are added as hidden columns, then dropped. |
| `LIMIT` / `OFFSET` | Each shard gets `LIMIT offset+limit`; the merger skips `offset` rows and stops after `limit`. |
| `DISTINCT` | Hash set over output rows (bounded by `MaxMergeRows`). |
| `COUNT`, `SUM`, `MIN`, `MAX` | Combined across shards. |
| `AVG` | Rewritten to `SUM` and `COUNT` per shard, divided at merge. |
| `GROUP BY` | Per-shard groups are re-aggregated by group key. `HAVING` is applied after the merge, not pushed to shards. |

Rejected with a clear error: window functions, subqueries, cross-shard joins, `FILTER`/`DISTINCT` inside aggregates, and `ORDER BY` expressions that cannot be reproduced at merge time.

Memory is bounded by `MaxMergeRows`; exceeding it fails the query rather than exhausting the process. Deep `OFFSET` is documented as expensive.

### 5.6 Writes (FR-7)

- **Insert:** the shard key is read from the row (builder) or the `VALUES` list (raw SQL). Missing key is `ErrShardKeyRequired`.
- **Batch insert:** rows are grouped by target shard, one statement per shard, executed in parallel.
- **Update / delete:** must carry a shard-key predicate (same rules as reads). An update that sets the shard-key column is rejected before execution.
- **Global tables:** the write goes to every shard.
- **Result:** `WriteResult{ PerShard map[ShardID]ShardOutcome }`, with rows affected or the error for each shard. A returned error means at least one shard failed; the result still shows which succeeded. There is no atomicity across shards.
- **Idempotency key:** optional. The key is stored in an `idempotency_keys` table on each affected shard and checked in the same single-shard transaction as the write, so a retry of a partially failed batch only re-applies on shards that did not commit.

### 5.7 Transactions (FR-8)

```go
tx, err := db.Begin(ctx, shard.ForKey(profileID)) // or ForShard(id)
```

- A transaction is pinned to one shard, chosen at `Begin` from a key or shard ID.
- Every statement inside runs through the normal Analyze/Route path, then a check: the target must equal the pinned shard, otherwise `ErrCrossShardTx`.
- Colocated tables make this practical: profile and addresses writes share one transaction and one SQL transaction block.
- Global-table writes are not allowed inside a transaction (they would touch every shard).

### 5.8 Explain (FR-9)

`db.Explain(ctx, sql, args...)` runs Analyze, Route and Plan and returns:

```
Explain{
  Strategy: All,
  Targets:  [shard-01 shard-02 shard-03],
  Reason:   "no shard-key predicate; WithAllShards()",
  Merge:    [OrderedMerge(created_at DESC), Limit(20)],
  ShardSQL: "SELECT ... LIMIT 20",
  ShardPlans: map[ShardID]string  // only with WithShardPlans(); runs EXPLAIN per shard
}
```

Per-shard plans use Postgres `EXPLAIN`; `ANALYZE` is opt-in because it executes the statement.

### 5.9 Migrations (FR-10)

- `migrate.Runner` opens one `golang-migrate` instance per shard, all pointed at the same migration source.
- Version is tracked per shard in `schema_migrations`. A Postgres advisory lock per shard prevents concurrent runs.
- **Apply:** runs shards in parallel (bounded). Policy `HaltOnFailure` (default; stops scheduling new shards after the first failure) or `ContinueOnFailure`.
- **Status:** returns `map[ShardID]Version` and flags drift when versions differ.
- A failed shard is left at its last good version and is re-runnable.
- Global tables are covered by running the same migration on every shard. The runner treats global-table data changes like any other migration.
- Implementation sits behind a `Migrator` interface so goose could replace golang-migrate.

### 5.10 Observability (FR-11)

- `observe.Hooks` is an interface with callbacks (`OnPlan`, `OnShardStart`, `OnShardDone`, `OnMerge`). Default is a no-op.
- `observe.Slog` is a ready-made structured logging adapter.
- `observe/otel` (separate Go module or subpackage) emits spans per query and per shard, and metrics for latency, fan-out width and errors.
- `db.Health(ctx)` pings every shard and returns per-shard status and `sql.DBStats`.

### 5.11 Client interface and test support (FR-13)

The facade's operations are exposed as an interface, so application code depends on the interface and tests can swap in a fake:

```go
type Querier interface {
    Query(ctx context.Context, sql string, args ...any) (Rows, error)
    Exec(ctx context.Context, sql string, args ...any) (WriteResult, error)
    Begin(ctx context.Context, target TxTarget) (Tx, error)
    Explain(ctx context.Context, sql string, args ...any) (Explain, error)
}
// *shard.DB implements Querier.
```

`shardtest` provides three tools:

| Tool | Use |
|---|---|
| `shardtest.NewCluster(t, n, opts...)` | Starts `n` Postgres containers (testcontainers), applies migrations from the given source, returns a ready `shard.Config`, and registers cleanup with `t.Cleanup`. If `SHARDTEST_DSNS` is set, it uses those instances instead and truncates tables between tests. |
| `shardtest.NewFake(reg, shards...)` | In-memory `Querier`. Runs the real Analyze, Route and Plan stages but records the plan instead of executing, and returns canned rows set with `fake.Returns(...)`. No database needed. |
| Assertions | `shardtest.AssertRoutes(t, q, sql, args, wantShards...)`, `AssertSingleShard`, `AssertFanout`, and `cluster.Seed(t, table, rows)`, which inserts each row on its owning shard. |

The fake reuses the real router and analyzer, so a routing assertion in a unit test means the same thing as in production. The fake does not emulate SQL results; the integration tests cover that.

Example of a user's unit test:

```go
func TestProfileLoadsFromOneShard(t *testing.T) {
    fake := shardtest.NewFake(registry(), "shard-01", "shard-02", "shard-03")
    svc := profiles.NewService(fake)          // depends on shard.Querier

    _, _ = svc.Get(ctx, "profile-42")

    shardtest.AssertSingleShard(t, fake.LastPlan())
}
```

Example of a user's integration test:

```go
func TestCreateProfileWithAddresses(t *testing.T) {
    cluster := shardtest.NewCluster(t, 3, shardtest.WithMigrations("file://./migrations"))
    db, _ := shard.Open(ctx, cluster.Config(registry()))
    svc := profiles.NewService(db)

    err := svc.Create(ctx, profile, addresses)
    require.NoError(t, err)

    // the profile and its addresses are on the same shard
    cluster.AssertColocated(t, "profile_id", profile.ID, "profiles", "addresses")
}
```

### 5.12 Examples (FR-12)

Each example is a small `main` package under `examples/` with its own README:

| Directory | Shows | Requirements |
|---|---|---|
| `examples/quickstart` | Config, registry, insert, query by key | FR-1 to FR-4 |
| `examples/colocation` | `profiles` + `addresses` on one shard, single-shard transaction, colocated join | FR-6, FR-8 |
| `examples/fanout` | `WithAllShards()` with `ORDER BY`, `LIMIT`, `COUNT`/`AVG`, `GROUP BY` | FR-5 |
| `examples/writes` | Batch insert split across shards, partial failure handling, idempotency key | FR-7 |
| `examples/migrations` | Apply to all shards, status, drift detection, failure policy | FR-10 |
| `examples/explain` | Reading `Explain()` output for single, multi and all-shard queries | FR-9 |
| `examples/testing` | Unit test with the fake and integration test with `NewCluster` | FR-13 |

Conventions:

- All examples use one shared domain (profiles and addresses, plus a global `countries` table), so a reader learns it once.
- Examples read shard DSNs from environment variables, defaulting to the `docker-compose.yml` instances (`make up` starts them).
- Run with `make example-<name>`.
- CI runs `go build ./examples/...` on every change, and an integration job runs each example against real Postgres and checks its exit code and key output lines, so examples cannot drift from the API.

### 5.13 Demo guide (FR-14)

`docs/DEMO.md` is a manual test script built on the same pieces as the examples:

- **Environment:** `docker-compose.yml` (3 shards) plus `make demo-setup` / `make demo-reset`. Setup applies the demo migrations and loads a fixed dataset (profiles, addresses, countries) generated from a seed, so results are the same on every run.
- **Driver:** a small `examples/demo` CLI with one subcommand per scenario (for example `demo route`, `demo fanout`, `demo partial-failure`). The guide runs these rather than asking the reader to write code.
- **Scenario format:** each scenario has *Command*, *Expected output*, *What it proves*, and a pass/fail checkbox.
- **Failure scenarios** use `docker compose stop shard-02` / `start` to take a shard down, so the reader sees the partial-failure and fail-fast behavior for real.
- **Staying accurate:** a CI job extracts the fenced commands and expected-output blocks from `DEMO.md`, runs them against the compose cluster, and fails on any difference.

## 6. Configuration (FR-1)

```go
cfg := shard.Config{
    Shards: []shard.ShardConfig{
        {ID: "shard-01", DSN: "...", MaxConns: 20, Buckets: shard.Range(0, 511)},
        {ID: "shard-02", DSN: "...", MaxConns: 20, Buckets: shard.Range(512, 1023)},
    },
    MaxFanout:     8,
    ShardTimeout:  5 * time.Second,
    MaxMergeRows:  100_000,
    Registry:      reg,
    Hooks:         observe.Slog(logger),
}
db, err := shard.Open(ctx, cfg)
```

- `Open` validates the registry and bucket map, then pings every shard. A bad configuration fails startup.
- DSNs are never logged. Config can be built from a struct, env or file by the caller; the library only requires the struct.

## 7. Error model

Sentinel errors, wrapped with context, usable with `errors.Is`:

`ErrShardKeyRequired`, `ErrCrossShardJoin`, `ErrCrossShardTx`, `ErrUnsupportedQuery`, `ErrShardKeyImmutable`, `ErrMergeLimitExceeded`, and `*ShardError` carrying a `ShardID` and cause. Messages say what to do (for example, "add a shard-key predicate or use WithAllShards()").

## 8. Testing strategy

| Level | What | How |
|---|---|---|
| Unit | Registry validation, key canonicalization vectors, bucket map validation, predicate analysis, merge steps, plan rendering | No DB; table-driven tests; fake `Executor` returns canned `ShardRows` |
| Integration | Routing and fan-out, ordered merge, writes with partial failure, single-shard tx, migrations and drift | 3 real Postgres containers via `shardtest.NewCluster` (the same public package users get); one container is stopped to test failures |
| Examples | Every example builds and runs | CI builds `./examples/...`; an integration job runs each against real Postgres (5.12) |
| Property | Merge equals running the same query on a single combined database | Load identical data into 1 DB and into 3 shards, compare results for generated queries |
| Compatibility | Postgres 14 and the newest supported release; Go 1.26 | CI matrix |

## 9. Dependencies

| Dependency | Why |
|---|---|
| `github.com/jackc/pgx/v5/stdlib` | Postgres driver for `database/sql` |
| `github.com/pganalyze/pg_query_go/v5` | SQL parsing (cgo) |
| `github.com/cespare/xxhash/v2` | Stable fast hash |
| `github.com/golang-migrate/migrate/v4` | Per-shard migrations |
| `golang.org/x/sync` | `errgroup` and semaphore |
| `github.com/testcontainers/testcontainers-go` | Integration tests only |
| `go.opentelemetry.io/otel` | Only in `observe/otel` |

## 10. Requirements traceability

| Requirement | Where |
|---|---|
| FR-1 Topology | 5.4, 6 |
| FR-2 Registry | 5.1 |
| FR-3 Routing | 4.3, 5.2 |
| FR-4 Query API | 5.3 |
| FR-5 Fan-out and merge | 4.2, 5.4, 5.5 |
| FR-6 Colocation | 4.3, 5.1 |
| FR-7 Writes | 5.6 |
| FR-8 Transactions | 5.7 |
| FR-9 Explain | 5.8 |
| FR-10 Migrations | 5.9 |
| FR-11 Observability | 5.10 |
| FR-12 Examples | 5.12 |
| FR-13 Test support | 5.11 |
| FR-14 Demo guide | 5.13 |

## 11. Resolved and remaining decisions

Resolved from REQUIREMENTS section 7:

- Bucket count and storage: 1024 virtual buckets, static config.
- Parser: `pg_query_go`, isolated behind `Analyzer`.
- Minimum versions: Go 1.26, PostgreSQL 14+.
- Migrations: wrap `golang-migrate` behind `Migrator`.

Still open (decide in the project plan or early implementation):

- Whether `observe/otel` is a separate Go module to keep the core dependency-free.
- Builder API surface: a fluent builder vs generated typed repositories. v1 proposal is a small fluent builder; typed repositories can be layered on top.
- Where the idempotency-key table is created (by the library's own migration vs the user's migrations).
