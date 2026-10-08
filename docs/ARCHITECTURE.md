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
  analyze/            Engine-neutral Analysis of a statement; Analyzer interface (FR-4)
    pgparse/          pg_query_go adapter (only package that needs cgo)
  query/              Builder: SELECT / UPDATE / DELETE statements that carry their Analysis (FR-4)
  plan/               Plan type, strategies, Explain rendering (FR-9)
  exec/               Shard pool, parallel executor, tx pinning (FR-1, FR-8)
  merge/              Ordered merge, limit/offset, distinct, aggregates (FR-5)
  migrate/            Fan-out migration runner, status, drift (FR-10)
  observe/            Hooks interface, slog adapter; otel/ subpackage (FR-11)
  shardtest/          Public test support: containers, fake, assertions (FR-13)
  examples/           Runnable examples, one directory each (FR-12)
  docker-compose.yml  3 local Postgres shards for examples and the demo
  docs/
```

Dependency direction (arrows mean "imports"). Nothing imports the facade, and only `analyze/pgparse` imports cgo:

```
shard -> plan, exec, merge, migrate, observe, query (optional), analyze/pgparse (cgo builds only)
plan -> router, registry, analyze
query -> analyze
exec, merge, migrate -> (types from plan/registry only)
analyze -> registry
analyze/pgparse -> cgo (pg_query_go)
```

## 3. Core types and interfaces

Small interfaces sit at the seams that are expected to change.

```go
// registry
type Kind int // KindSharded, KindColocated, KindGlobal
type Table struct {
    Name    string
    Kind    Kind
    KeyCol  string  // empty for Global
    KeyType KeyType // required on sharded tables; colocated tables inherit it
    Parent  string  // for Colocated
    Group   string  // colocation group: the root sharded table's name
}
// Immutable. New validates every declaration and returns all problems at once.
func New(decls ...Decl) (*Registry, error)
func (r *Registry) Table(name string) (Table, bool)
func (r *Registry) Tables() []Table // sorted by name

// router
type ShardID string
type BucketRange struct{ From, To int } // inclusive
type Assignment struct {
    Shard   ShardID
    Buckets []BucketRange
}
func New(assignments ...Assignment) (*HashRouter, error) // validates the bucket map
func Even(ids ...ShardID) []Assignment                   // near-equal contiguous split
type Router interface {
    ShardFor(key any) (ShardID, error)       // canonicalize, hash, bucket, shard
    ShardsFor(keys []any) ([]ShardID, error)
    All() []ShardID
}

// analyze: engine-neutral FACTS about a statement. Routing decides what they mean.
type Analysis struct {
    Op         OpKind // OpSelect, OpInsert, OpUpdate, OpDelete, OpOther (DDL...)
    Kind       string // names the statement when Op is OpOther
    Scopes     []int  // Scopes[i] = parent query level of level i; level 0 is the statement
    Tables     []TableRef   // every table use: ID, Scope, Schema, Name, Alias
    Target     int          // table written by INSERT / UPDATE / DELETE
    Insert     *Insert      // VALUES rows of an INSERT: Columns, Rows of Cell{Value, Known}, ConflictSet
    SetColumns []string     // columns an UPDATE assigns
    Equalities []Equality   // a.col = b.col (WHERE, and ON), AND-ed
    Bindings   []Binding    // col = 5, col IN (1,2), col = ANY($1): parameters already resolved
    Usings     []Using      // JOIN ... USING (cols)
    Notes      []string     // conditions that could not be used (OR, ranges...), shown when routing fails
    // For merging results (M6); routing ignores them:
    OrderBy []OrderTerm; Limit, Offset *int64; GroupBy []string
    Aggregates []AggregateTerm; Distinct, HasWindow, HasHaving bool
}
type Analyzer interface {
    FromSQL(sql string, args []any) (Analysis, error)
}
// Optional: an analyzer that can restrict an INSERT to some of its rows, needed
// to send a multi-row INSERT to several shards. pgparse implements it.
type RowSplitter interface {
    SplitRows(sql string, args []any, rows []int) (string, []any, error)
}
// A built query.Statement produces its Analysis directly, without parsing.

// plan: the Route stage. Pure: no database, no parser.
func Route(a analyze.Analysis, reg *registry.Registry, r router.Router, opts Options) (Plan, error)

// plan
type Strategy int // Single, Multi, All
type Plan struct {
    SQL      string       // possibly rewritten (e.g. AVG -> SUM+COUNT)
    Args     []any
    Targets  []ShardID
    Strategy Strategy
    Reason   string       // why these shards (for Explain)
}

// exec
type Executor interface {
    Run(ctx context.Context, p Plan) (ShardRows, error)
}

// merge: a fan-out SELECT is rewritten by a Planner into the statement every
// shard runs plus a Spec; Merge combines the shards' rows by the Spec. The Spec
// is the seam: a join-capable merge would replace the Planner and Merge (FR-5).
type Planner interface {
    PlanMerge(sql string, args []any) (Plan, error) // Plan = shard SQL + Args + Spec
}
func Merge(spec Spec, srcs []Source, opts Options) (*Rows, error)
```

## 4. Request flow

### 4.1 Read by shard key (single shard)

```
db.Query(ctx, "SELECT ... FROM profiles WHERE id = $1", 42)
  -> Analyzer.FromSQL            tables=[profiles], binding id = 42
  -> plan.Route                  coerce 42 to the key type, Router -> shard-03
  -> Plan{Strategy: Single, Targets: [shard-03]}
  -> Executor.Query              one query, rows returned as-is
```

### 4.2 Fan-out read (all shards)

Only allowed when the caller opts in with `WithAllShards()` (or when the key conditions name several shards); otherwise the router returns `ErrShardKeyRequired` (FR-3, FR-4).

```
SELECT ... ORDER BY created_at DESC LIMIT 20   (WithAllShards)
  -> Plan{Strategy: All, Targets: every shard}
  -> Planner.PlanMerge   shard SQL: LIMIT 20 (offset + limit pushed down), ORDER BY kept;
                         Spec: Order[created_at DESC], Limit 20
  -> Executor.Query      runs the shard SQL on all shards in parallel, rows streamed
  -> merge.Merge         k-way merge of the sorted streams; stops (and closes the shards) at 20 rows
```

### 4.3 Routing rules (analysis of predicates)

`plan.Route` applies these rules to an `Analysis`. Anything it cannot prove safe is refused, never guessed.

**Which conditions pick shards.** Only conditions that restrict the result and are AND-ed at the top level count: in `WHERE`, and in the `ON` of an inner join. In the `ON` of an outer join only the key *links* count, never constants (a constant there does not filter the preserved side).

| Condition on the shard key | Result |
|---|---|
| `key = $1`, `key = 42`, `42 = key`, `key = $1::bigint` | The one shard that key hashes to |
| `key IN (a, b, ...)`, `key = ANY($1)` (slice argument), `key = ANY(ARRAY[...])` | The distinct shards those keys hash to (Multi, or All if every shard) |
| Several of the above on the same key, AND-ed | The intersection. Conflicting conditions (`key = 1 AND key = 2`) match nothing: any one shard answers |
| `OR`, `NOT`, `<>`, `NOT IN`, ranges, `BETWEEN`, `LIKE`, `IS NULL`, `= NULL`, a function on the key, a comparison with another expression, `IN (subquery)` | Not usable: noted in the error. Error unless `WithAllShards()` / `WithShardKey()` |
| No usable condition | `ErrShardKeyRequired`, naming the table and key column |

**Key types.** The registry requires a key type on every sharded table (colocated tables inherit it), so every key found in SQL or arguments is converted to it before hashing: `'42'` and `42` reach the same shard for an int key, and a value that cannot be converted is an error. The requirement exists because keys are hashed by Go type; without it a string ID from an HTTP path would silently go to a different shard than the same number.

**Tables.**

| Statement touches | Result |
|---|---|
| Only global tables, or no table (`SELECT 1`), or system schemas | Any one shard, taking turns |
| `INSERT` / `UPDATE` / `DELETE` on a global table (and no sharded table) | Every shard |
| A write to a global table that also uses a sharded table | `ErrUnsupportedQuery` (split it) |
| A table not in the registry | `ErrUnknownTable` |
| Sharded or colocated tables from more than one colocation group | `ErrCrossShardJoin`, naming the alternatives |
| `INSERT ... VALUES` into a sharded or colocated table | Each row goes to the shard that owns its key (see 5.6) |
| `INSERT` whose rows are not known (`INSERT ... SELECT`, `DEFAULT VALUES`) | `ErrUnsupportedQuery` |
| `INSERT` that does not name its columns, omits the key column, or whose key is `DEFAULT`, NULL, a function or an expression | `ErrShardKeyRequired`, saying what to do |
| `UPDATE` that assigns the shard key, or `INSERT ... ON CONFLICT DO UPDATE` that does | `ErrShardKeyImmutable` |
| DDL and other non-DML statements | `ErrUnsupportedQuery`: use `WithAllShards()` or the migrations runner |

**Joins and subqueries.** Every use of a sharded/colocated table (in the FROM clause or in any subquery) is a node. Nodes are tied together by shard-key equality: `ON a.key = b.key`, `USING (key)`, or a correlated subquery condition. Each resulting group is limited to the shards its key conditions allow.

- One group: routed by its conditions, or `ErrShardKeyRequired` if it has none.
- Several groups: allowed only if every group is pinned to the same single shard (`p.id = $1 AND a.profile_id = $1`). Otherwise `ErrCrossShardJoin`: matching rows could be on different shards.
- Global tables never matter for routing.

**Naming.** A qualified column (`p.id`, `profiles.id`, `public.profiles.id`) is matched to a table by alias or name, searching the enclosing queries outward. An unqualified column is matched to the table in the nearest query level whose shard key has that name. If that level has tables but none is keyed by the name, the column may belong to one of them (we cannot see their other columns), so the search stops: in `WHERE EXISTS (SELECT 1 FROM countries WHERE id = 5)` the `id` is `countries.id` and must not route the outer `profiles`. After `USING (id)`, an unqualified `id` names the merged column.

**Not supported for routing** (`ErrUnsupportedQuery`, naming the construct): `WITH`, `UNION`/`INTERSECT`/`EXCEPT`, a subquery or function in `FROM`, `NATURAL JOIN`, `VALUES`, `SELECT INTO`, several statements in one call. They still run when the caller names the shard (`WithShardKey`, `WithShard`).

## 5. Components

### 5.1 Registry (FR-2)

- Built in code at startup: `registry.New(registry.Sharded(...), registry.Colocated(...), registry.Global(...))`.
- `New` validates and returns every problem at once: missing key, a sharded table with no key type, an invalid key type, missing or global parent, cycles, global tables with options, duplicate names, and a colocated table whose declared key type differs from its root's. Group is the root ancestor's name.
- A colocated table's key column may have a different name from its parent's (for example `profiles.id` and `addresses.profile_id`).
- Immutable once built, so it is safe for concurrent use without locks. There is no separate `Validate()` call.
- Names are matched exactly as declared; declare them in lower case, as PostgreSQL folds unquoted identifiers.

### 5.2 Router (FR-3)

- **Canonical key bytes:** a one-byte type tag, then the value. Integers (any width) as 8-byte big-endian `int64`, UUIDs as their 16 bytes, strings as UTF-8. Stable across restarts and versions, and pinned by test vectors computed with an independent xxhash64 implementation.
- **UUIDs:** a `[16]byte` and a string in canonical 8-4-4-4-12 form (any letter case) route to the same shard, so it does not matter whether the caller holds the UUID as bytes or text. Other string layouts are ordinary strings.
- **Rejected keys:** nil, unsigned values above `MaxInt64`, and unsupported types (floats, bools, byte slices, structs) return errors rather than guessing.
- **Hash:** `xxhash64(canonical) % 1024` gives a bucket.
- **Bucket map:** config lists bucket ranges per shard. Startup validation requires every bucket in `[0,1024)` to be assigned exactly once.
- The bucket count is fixed so that adding resharding later moves buckets, not every key.
- `Router` is an interface; the default implementation is the only one in v1.

### 5.3 Analyzer and query builder (FR-4)

Two producers of `Analysis`, one consumer (`plan.Route`):

- **Builder (`query`)**: `query.From("profiles").Where(query.Eq("id", 42)).Build()` returns a `Statement` with its SQL, arguments and `Analysis`, so no parsing happens and no cgo is needed. Supports `SELECT` (columns, `ORDER BY`, `LIMIT`, `OFFSET`), `UPDATE` and `DELETE`, with `Eq`, `In`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`, `Like`, `IsNull`, `NotNull`, AND-ed. Every identifier is validated and quoted, so a column name cannot change the statement. Anything else is written as SQL. Run with `db.QueryStatement` / `db.ExecStatement`. A test proves the builder's `Analysis` routes exactly like the parser's reading of the SQL the builder wrote.
- **Raw SQL (`analyze/pgparse`)**: parses with `pg_query_go` (PostgreSQL's own parser, v6 / PG 17 grammar) and walks the tree:
  - `$n` is replaced by the argument; integers too large for 32 bits (which the parser stores as text) are read back as integers.
  - Each subquery gets its own query level (found by walking the whole expression tree, so a subquery inside a `CASE`, a function argument or a select list is not missed).
  - Constants in the `ON` of an outer join are recorded as notes, not bindings.
  - Statements it cannot route safely return `ErrUnsupportedQuery` naming the construct.
- Parsing is per call and not cached. A cache keyed by SQL text is a straightforward later optimisation.
- Only `analyze/pgparse` needs cgo. Without cgo the package is empty, `Config.Analyzer` defaults to none, and raw SQL needs an explicit route (see docs/BUILDING.md). `Config.Analyzer` accepts any `analyze.Analyzer`.
- `WithShardKey`, `WithShard` and `WithAllShards` bypass analysis completely and are recorded in `Plan.Reason`. They are the escape hatch, so the caller is trusted.

### 5.4 Executor and shard pool (FR-1, FR-5)

- `exec.Pool` holds one `*sql.DB` (pgx stdlib driver) per shard with its own `MaxConns` (default 10). `exec.Open` connects and pings every shard; if any fails it closes the rest and returns a `*ShardError` naming the shard. DSNs never appear in errors.
- The pool depends on a small `Conn` interface (the subset of `*sql.DB` it uses), so the executor is unit tested with a fake `database/sql` driver and no database.
- `Executor.Query` and `Executor.Exec` run a `plan.Plan` on every target in parallel through an `errgroup` limited to `MaxFanout` (default 8), so a wide statement cannot open unbounded work. Plans with no targets, unknown shards or repeated shards are rejected before anything runs.
- Each shard call gets its own context with the per-shard timeout (if any), derived from the caller's context. The timeout covers streaming the rows too, so `exec.Rows` releases the context on `Close`: rows must always be closed.
- **Reads fail fast** (first error cancels the other shards' queries and closes any result sets already open). The cancellation is wired with `context.AfterFunc` and removed once each query returns, so cancelling on exit never cuts off rows the caller is still reading. A test pins this.
- **Writes do not fail fast.** Every target is attempted and each outcome is reported, because a write that succeeded on some shards must be visible. The facade turns the outcomes into a `WriteResult` plus an error if any shard failed.
- A plan may give each shard its own statement (`Plan.PerShard`, read through `Plan.StatementFor`): this is how a multi-row INSERT sends every shard only its rows. `ShardExec` also reports the VALUES rows the shard received and whether the write was a replay.
- `Executor.ExecIdempotent` runs the same fan-out with the idempotency protocol of 5.6.
- `Executor.Begin` returns an `exec.Tx`: one shard, one `*sql.Tx`, one connection, with `Query` / `Exec` / `Commit` / `Rollback` (5.7).
- `Executor.QueryPartial` is `Query` that does not fail fast: every target is attempted, the shards that could not start their query are returned as `[]*ShardError` next to the rows of those that could, and it fails only when no shard answered or the context ended. A failure while a shard streams is not tolerated; it stops the iteration (5.5).
- Reads can be retried once on connection errors; this is not implemented yet. Writes are never retried by the library: a retry is the caller's, made safe by an idempotency key (5.6).
- Rows are streamed per shard so the merger can start before all shards finish.

### 5.5 Merger (FR-5)

A SELECT that runs on more than one shard is rewritten and merged; a SELECT on one shard is not touched.

**Planner.** `merge.Planner` (implemented by `analyze/pgparse` with the parser and deparser, and by built `query` SELECTs without any parser) returns a `merge.Plan`: the SQL every shard runs, its arguments, and a `merge.Spec` that says how to combine the answers. The Spec is plain data (columns and how each is combined, hidden-column count, order, offset, limit, distinct, having), so `merge` knows nothing about SQL text.

| Construct | Shard statement | Merge |
|---|---|---|
| `ORDER BY` | kept, so each shard returns sorted rows. Order items not in the select list (a column, a function, an aggregate) are added as hidden trailing columns; with `SELECT *` they are named from the end (negative index) because the position of later columns is unknown. | k-way merge with a heap over the sorted streams, ties to the lower shard; one row per shard in memory |
| `LIMIT` / `OFFSET` | `OFFSET` removed, `LIMIT` becomes offset + limit; parameters used only by them are renumbered away | skip `offset`, stop after `limit`, then close the shard rows |
| `DISTINCT` | kept | hash set of the output rows, bounded by `MaxMergeRows` |
| `COUNT`, `SUM` | as written | added; NULL (a shard with no rows) is ignored. Numbers add as int64, float64 or exact decimals |
| `MIN`, `MAX` | as written | smallest / largest, NULL ignored |
| `AVG` | rewritten to `SUM` (column keeps the name `avg`) plus a hidden `COUNT` | sum / count; an average of integers shows 16 decimals as PostgreSQL does |
| `GROUP BY` | as written, plus a hidden copy of each group item that is not already a selected column, ordinal or alias | rows with equal keys are combined; groups are held in memory, bounded by `MaxMergeRows` |
| `HAVING` | removed; the aggregates it uses are added as hidden columns | evaluated on merged groups. `AND`/`OR`/`NOT` over comparisons of an aggregate (or group column) with a constant or parameter, with SQL's three-valued logic (`NOT (avg < 43)` is not true for a NULL average) |
| aggregate `ORDER BY`, `LIMIT`, `OFFSET` | removed | sorted in memory over the merged groups, then cut |

Aggregation reads every shard completely when the query runs (so a streaming error from a shard fails `Query`), then closes the connections. Everything else streams.

**Rejected** with `ErrUnsupportedQuery` naming the construct: window functions, subqueries (in any clause), `WITH`, `UNION` / `INTERSECT` / `EXCEPT`, `FOR UPDATE`, `DISTINCT ON`, `FETCH ... WITH TIES`, aggregates other than `count`/`sum`/`min`/`max`/`avg` (`string_agg`, `array_agg`, `stddev`, ...), `DISTINCT` / `FILTER` / `ORDER BY` inside an aggregate, an expression around an aggregate (`sum(x) / count(*)`), `GROUPING SETS` / `ROLLUP` / `CUBE`, `SELECT *` with aggregates or `GROUP BY`, `SELECT DISTINCT` with an order term that is not selected, `ORDER BY ... USING`, and a `LIMIT` that is not a number or parameter. Cross-shard joins are refused earlier by routing (`ErrCrossShardJoin`).

**Values.** Rows are read as `database/sql` driver values and compared by Go type: integers and floats numerically, decimals (`numeric` arrives as text; the column's type name tells the merge to compare it as a number) exactly with `big.Rat`, text byte by byte, times by instant. `Rows.Scan` converts as `database/sql` does for the common destinations and `sql.Scanner`.

**Known differences from one database.**
- Text is ordered as the `C` collation. If a database uses another collation (`en_US.UTF-8`), `ORDER BY` on a text column can order differently across shards than PostgreSQL would on one, and `MIN`/`MAX` of text can differ. Use `COLLATE "C"` in the query or a `C` database collation for text you order by.
- An average of decimals can show a different number of decimals than PostgreSQL (the value is the same).
- Floating-point sums can differ in their last digits, because shards add in a different order.
- A `GROUP BY` name that is both an input column and an output alias is read as the alias when it names a select item.

**Failure.** By default the first shard error cancels the others and fails the query. `shard.AllowPartial(ctx)` merges the shards that answered and reports the rest through `shard.ShardErrors(rows)`; the result then covers only those shards. Only a failure to start the query is tolerated: an error while a shard streams stops the iteration and appears in `Rows.Err()`, labelled with the shard.

**Memory.** Streaming merges (ordered or not, with or without `LIMIT`) hold one row per shard. Aggregation, `GROUP BY` and `DISTINCT` hold their groups or distinct rows and fail with `ErrMergeLimitExceeded` past `Config.MaxMergeRows` (default 100,000) instead of exhausting memory. Deep `OFFSET` is expensive because each shard returns `offset + limit` rows; prefer a keyset condition.

### 5.6 Writes (FR-7)

**INSERT.** The analyzer reads the `VALUES` rows (`analyze.Insert`: the column names, and for each row a `Cell` that is `Known` only if it is a literal or parameter). `plan.Route` finds the shard key column, converts each row's key to the declared type, and assigns the row to the shard that owns it (`Plan.Rows`: shard to row indexes). A row whose key is `DEFAULT`, NULL, a function call or an expression cannot be placed, and refuses the whole statement before anything is written; so does a missing key column or an `INSERT` with no column list.

**Splitting.** If the rows belong to more than one shard, each shard gets a statement holding only its rows:
- Raw SQL: `pgparse` parses the statement, keeps the chosen rows in the `VALUES` list, renumbers `$n` from 1 in the order the statement uses them, drops arguments no longer used, and deparses with PostgreSQL's own deparser. Field order is fixed, so the same input always gives the same text (the idempotency check relies on this). `ON CONFLICT` and `RETURNING` are kept.
- Built statements (`query.InsertInto`): the builder renders the chosen rows directly.
- An analyzer that cannot split (not a `RowSplitter`) gives `ErrUnsupportedQuery` for a multi-shard insert and says to insert in separate calls.

**Update / delete** follow the same rules as reads. An `UPDATE` that assigns the shard key column, or an upsert whose `DO UPDATE SET` does, is `ErrShardKeyImmutable`. Moving a row means deleting it and inserting it again.

**Global tables:** the write goes to every shard.

**Result.** `WriteResult{PerShard map[ShardID]ShardOutcome}`. Each outcome has `RowsAffected`, `Err`, `Rows` (the indexes of the VALUES rows that shard received) and `Replayed`. A returned error means at least one shard failed; the result still shows every shard. There is no atomicity across shards. Helpers: `RowsAffected()` (successful shards only), `Failed()` (error per failed shard) and `FailedRows()` (sorted indexes of the rows that went to failed shards, i.e. what to send again).

**Idempotency.** `shard.WithIdempotencyKey(ctx, key)` makes `Exec` / `ExecStatement` safe to retry. On each affected shard, in one transaction:

1. `INSERT INTO <idempotency table> (key, request_hash) ... ON CONFLICT (key) DO NOTHING`.
2. If that inserted nothing the key is already there: read the stored hash and `rows_affected`, roll back, and report a replay with the stored count. A different hash is `ErrIdempotencyKeyReused`.
3. Otherwise run the write, record `rows_affected`, and commit.

Because the key and the write commit or roll back together, a shard where the write failed has no key and a retry applies it; a shard that committed has the key and a retry skips it. Concurrent callers with the same key are serialized by the primary key: the second blocks on the first's transaction, then sees the key and replays (tested with 12 simultaneous callers: exactly one applies).

The hash covers the statement text and the arguments (times are normalised, so the instant is compared, not its representation). A retry therefore has to send exactly what the first attempt sent; for a split INSERT that means the same rows, which is why splitting is deterministic. The key is per call, so a context carrying a key should be used for one write; `Query` ignores it.

The table (default `go_shard_idempotency_keys`, set with `Config.IdempotencyTable`, may be `schema.name`) is never created implicitly: `DB.EnsureIdempotencyTable` creates it on every shard, or add `shard.IdempotencyDDL(table)` to your migrations. If it is missing the write is refused with `ErrIdempotencyTableMissing` and instructions. `DB.PruneIdempotencyKeys(ctx, olderThan)` removes old keys on every shard.

### 5.7 Transactions (FR-8)

```go
err := db.InTx(ctx, shard.ForTable("profiles", id), func(tx shard.Tx) error {
    _, err := tx.Exec(ctx, "INSERT INTO profiles ...")
    ...
})
// or, by hand:
tx, err := db.Begin(ctx, shard.ForTable("profiles", id))
defer tx.Rollback() // harmless after Commit (returns ErrTxDone)
```

- **One shard per transaction**, chosen at `Begin` from a target: `ForShard(id)`; `ForKey(key)` (hashed as given); or `ForTable(table, key)`, which converts the key to the table's declared type first, so `42` and `"42"` pick the same shard. `ForKey` exists for keys you already hold in the right type; prefer `ForTable`.
- **Every statement is routed as usual, then checked**: its plan must target exactly the transaction's shard, otherwise `ErrCrossShardTx`, returned before anything is sent so the transaction stays usable. The message names both shards and says how to proceed.
- **Global tables.** A statement that only reads global tables runs on the transaction's shard (`plan.Options.AnyShard` returns it). A write to a global table targets every shard, so it is refused, with a hint (`Plan.GlobalWrite`). With a single shard, "every shard" is the transaction's shard and the write is allowed.
- **`Tx` is a `Querier`**, so repository code written against `Querier` (`Query`, `Exec`, `QueryStatement`, `ExecStatement`) runs unchanged inside a transaction, with the same routing, builder statements and `RETURNING` support. `Begin` is deliberately **not** on `Querier`: it lives on a separate `TxBeginner` (implemented by `*DB`), so a `Tx` cannot start another and fakes of `Querier` stay small. This differs from the earlier sketch, which put `Begin` on `Querier`.
- **Colocated tables make this practical**: a profile and its addresses are on one shard, so one transaction writes both, and an ordinary join reads them back.
- **`InTx`** commits when the function returns nil and rolls back on an error or a panic (which continues), so a transaction cannot be left open.
- **`tx.Unchecked()`** returns a `Querier` that runs on the transaction's shard with no routing or checks, for SQL the router cannot place (`WITH`, a scan by a non-key column). The caller vouches that the statement belongs on this shard. Refusals that it can resolve say so.
- **Options:** `ReadOnly()` and `Isolation(level)` map onto `sql.TxOptions`.
- **Lifetime and timeouts.** `exec.Tx` holds one `*sql.Tx`, so all statements share a connection. The context given to `Begin` ends the transaction when cancelled (`database/sql` rolls it back). `ShardTimeout` applies to each statement, not to the transaction as a whole. A transaction has one connection, so the `Rows` of a query must be closed before the next statement.
- **Idempotency keys are refused inside a transaction** (`ErrUnsupportedQuery`): the transaction is already atomic on its shard, and one key per statement would collide with the key-reuse check. Put the key on a statement outside the transaction.
- **Not supported:** nested transactions and savepoints; a transaction that spans shards (see REQUIREMENTS section 8).

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
    // Added in a later milestone:
    Explain(ctx context.Context, sql string, args ...any) (Explain, error) // M7
}
// *shard.DB implements Querier.
```

Plain `db.Query` / `db.Exec` route from the SQL. The explicit overrides (FR-4), which skip analysis, are handles that are themselves `Querier`s:

```go
db.WithShardKey(id).Exec(ctx, "INSERT INTO profiles ...", id, name)  // shard that owns id
db.WithShard("shard-02").Query(ctx, "SELECT ...")                     // a named shard
db.WithAllShards().Exec(ctx, "CREATE TABLE ...")                      // every shard
```

- `Rows` is an interface (`Next`, `Scan`, `Columns`, `Err`, `Close`) that `*sql.Rows` satisfies and merged results will too.
- `WriteResult{PerShard map[ShardID]ShardOutcome}`: see 5.6. A write that fails on any shard returns an error and the result still lists every shard.
- `Query` on more than one shard returns `ErrUnsupportedQuery` until merging exists (M6).

`shardtest` provides these tools (the fake and the routing assertions arrive in M7):

| Tool | Use |
|---|---|
| `shardtest.NewCluster(t, n, opts...)` | Starts `n` Postgres containers in parallel (testcontainers; image from `WithPostgresVersion`, `SHARDTEST_POSTGRES_VERSION`, default 14) named `shard-01`..., and registers cleanup with `t.Cleanup`. Applying migrations (`WithMigrations`) arrives with M8. If `SHARDTEST_DSNS` is set, it uses those instances instead and **drops and recreates their `public` schema** at the start of each test; tests sharing them must run with `go test -p 1`. |
| Cluster helpers (M2) | `cluster.Open(t, reg)` opens a `*shard.DB` and closes it at cleanup; `Config(reg)`; `IDs()`; `DSN(id)`; `ExecAll(t, sql)` runs DDL on every shard; `Direct(t, id)` returns a plain `*sql.DB` that bypasses go-shard, so a test can assert where a row physically is; `Count(t, id, sql)`; `Stop(t, id)` shuts a container down to test a real outage (it skips the test when the cluster uses `SHARDTEST_DSNS`); `Seed(t, db, table, rows...)` inserts each row on the shard that owns it (global tables on every shard). |
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
        // Buckets are optional: leave them off every shard to split 0-1023 evenly
        // in the order listed. Otherwise list them on every shard.
        {ID: "shard-01", DSN: "...", MaxConns: 20, Buckets: []router.BucketRange{shard.Range(0, 511)}},
        {ID: "shard-02", DSN: "...", MaxConns: 20, Buckets: []router.BucketRange{shard.Range(512, 1023)}},
    },
    MaxFanout:     8,                // default 8
    ShardTimeout:  5 * time.Second,  // default none (the caller's context still applies)
    Registry:      reg,              // required
    MaxMergeRows:  100_000,          // default 100000: groups / distinct rows a merge may hold
    // Added in later milestones:
    Hooks:         observe.Slog(logger), // M9
}
db, err := shard.Open(ctx, cfg)
```

- `Open` validates the config (every problem is reported at once, wrapping `ErrInvalidConfig`), builds the router, then connects to and pings every shard. A bad configuration or an unreachable shard fails startup, and nothing is left open.
- `db.Health(ctx)` pings every shard in parallel and returns per-shard health, latency and pool stats.
- DSNs are never logged. Config can be built from a struct, env or file by the caller; the library only requires the struct.

## 7. Error model

Sentinel errors, wrapped with context, usable with `errors.Is`:

`ErrShardKeyRequired`, `ErrCrossShardJoin`, `ErrUnknownTable`, `ErrUnknownShard`, `ErrUnsupportedQuery`, `ErrMissingArgument`, `ErrInvalidConfig`, `ErrShardKeyImmutable`, `ErrIdempotencyKeyReused`, `ErrIdempotencyTableMissing`, `ErrCrossShardTx`, `ErrTxDone`, `ErrMergeLimitExceeded` (all in place); plus `*ShardError` carrying a `ShardID` and cause. The root package re-exports the values defined in `analyze` and `plan`, so `errors.Is` works whichever layer returned the error. Messages say what to do (for example, "add a shard-key predicate or use WithAllShards()").

## 8. Testing strategy

| Level | What | How |
|---|---|---|
| Unit | Registry validation, key canonicalization vectors, bucket map validation, predicate analysis, merge steps, plan rendering | No DB; table-driven tests; fake `Executor` returns canned `ShardRows` |
| Integration | Routing and fan-out, ordered merge, writes with partial failure, single-shard tx, migrations and drift | 3 real Postgres containers via `shardtest.NewCluster` (the same public package users get); one container is stopped to test failures |
| Examples | Every example builds and runs | CI builds `./examples/...`; an integration job runs each against real Postgres (5.12) |
| Property | Merge equals running the same query on a single combined database | `TestFanOutMatchesOneDatabase`: identical rows in 1 DB (a private schema on shard 1) and in 3 shards; 600 generated queries per run (rows, `DISTINCT`, aggregates, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, colocated joins); a total order is compared row for row, otherwise as a set plus an in-order check. `FANOUT_SEED` and `FANOUT_QUERIES` explore further |
| Compatibility | Postgres 14 and the newest supported release; Go 1.26 | CI matrix |

## 9. Dependencies

| Dependency | Why |
|---|---|
| `github.com/jackc/pgx/v5/stdlib` | Postgres driver for `database/sql` |
| `github.com/pganalyze/pg_query_go/v6` | SQL parsing (cgo, `analyze/pgparse` only) |
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
- Builder: a small fluent builder in its own package `query` (SELECT / INSERT / UPDATE / DELETE).
- Idempotency table: created explicitly with `DB.EnsureIdempotencyTable` or by the user's migrations (`shard.IdempotencyDDL`), never implicitly at first use.
- Merge design: the SQL is rewritten once into shard SQL plus a data `Spec`, and a separate `Merge` function combines rows by the Spec (no SQL knowledge), rather than a merger that interprets steps; `Plan` carries no merge steps.
- Without cgo the library still builds; only automatic routing of raw SQL is unavailable (docs/BUILDING.md).
- Migrations: wrap `golang-migrate` behind `Migrator`.

Still open (decide in the project plan or early implementation):

- Whether `observe/otel` is a separate Go module to keep the core dependency-free.
- Typed repositories on top of the builder (the fluent builder itself is done in M3).
