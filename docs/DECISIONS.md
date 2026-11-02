# Decisions

Closed decisions, so they are not reopened by accident. Each says what was
decided and where the reasoning is. To change one, change this file and the doc
it points to in the same change.

| Decision | Why, in a line | Details |
|---|---|---|
| Cross-shard joins are not supported; the error names the alternatives (colocate, global table, two queries) | A join engine and planner is a product of its own | REQUIREMENTS 3, 8 |
| No distributed transactions; a transaction covers one shard | Two-phase commit needs a coordinator, a durable log and recovery | REQUIREMENTS 3, 8 |
| No resharding in v1; 1024 fixed virtual buckets so it can be added later | Moving buckets, not rehashing keys | REQUIREMENTS 8, ARCHITECTURE 5.2 |
| A statement with no shard key is refused, never scattered | Silent scatter-gather hides load and wrong results | REQUIREMENTS FR-3, FR-4 |
| Fan-out is opt-in (`WithAllShards`) | The caller decides to pay for it | QUERIES.md |
| The shard key type is declared in the registry | `42` and `"42"` must reach the same shard | REQUIREMENTS FR-2 |
| The library does not retry; writes are retried safely with idempotency keys | A retry policy depends on the application | OPERATIONS.md |
| The idempotency table is created explicitly, never implicitly | No DDL as a side effect of a write | ARCHITECTURE 11 |
| Merging works on a data `Spec`; `merge` knows no SQL | The same merge serves raw SQL and the builder | ARCHITECTURE 11 |
| Writes to several shards are not atomic; the result reports each shard | There is no safe way to hide it | REQUIREMENTS FR-7 |
| `pg_query_go` (cgo) parses raw SQL, behind the `Analyzer` interface; everything else builds without cgo | Use PostgreSQL's own parser; keep the build easy | BUILDING.md |
| The fluent builder in `query` is the builder API; no typed repositories in v0.x | A repository layer can sit on `Querier` later | ARCHITECTURE 11 |
| `observe/otel` is a subpackage of the main module | One version to track | ARCHITECTURE 5.10 |
| Migrations wrap `golang-migrate`; forward-only; a failed shard returns to its last good version | One tool, one transaction per migration | ARCHITECTURE 5.9 |
| Hooks carry no statement arguments | Arguments may be secrets or personal data | observe/doc.go |
| Go 1.26+, PostgreSQL 14+ | | PROJECT_PLAN 5 |
