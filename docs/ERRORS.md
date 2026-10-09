# Error reference

Every error the library returns can be tested with `errors.Is` or `errors.As`.
Messages say what happened and what to do; this page says when each one
happens and how to react in code.

Errors fall into three groups: the statement was **refused before anything ran**
(nothing was sent to any shard, so it is always safe to fix and call again), a
**shard failed**, or the **setup is wrong**.

## Refused before anything ran

Nothing was sent to any shard. These are bugs in the statement or the calling
code, not conditions to retry.

| Error | When | What to do |
|---|---|---|
| `ErrShardKeyRequired` | The statement does not say which shard it belongs to: no usable condition on the shard key (an `id = $1`, or `id IN (...)`, AND-ed with the rest of the `WHERE`), and no override. Also returned for raw SQL in a build without cgo (no SQL analyzer). | Add the key condition, or say where it goes: `db.WithShardKey(k)`, `db.WithShard(id)`, `db.WithAllShards()`. Without cgo, use these or a built statement (package `query`). |
| `ErrCrossShardJoin` | A join between tables that are not guaranteed to be on one shard: different colocation groups, or not tied together by their shard key. | Colocate the tables, make the small one a global table, or run two queries and join in your code. |
| `ErrUnsupportedQuery` | SQL the library cannot route safely or run: see [QUERIES.md](QUERIES.md). The message names the construct. Also: an idempotency key inside a transaction; a multi-row `INSERT` that cannot be split. | Rewrite the statement as the message says, route it yourself (`WithShardKey` and friends), or use `tx.Unchecked()` inside a transaction. |
| `ErrMissingArgument` | The SQL uses `$3` and fewer arguments were given. | Pass every argument. |
| `ErrUnknownTable` | A table that is not in the registry. | Declare it in `registry.New`. |
| `ErrUnknownShard` | `WithShard` or `ForShard` names a shard that is not configured. The message lists the shards. | Use a configured shard ID. |
| `ErrShardKeyImmutable` | An `UPDATE` that assigns the shard key, or an upsert whose `DO UPDATE` does. | Delete the row and insert it again with the new key (not atomic across shards). |
| `ErrCrossShardTx` | Inside a transaction, a statement belongs to a different shard (or several). The transaction is still usable. | Keep rows that change together on one shard with colocation, or run that statement outside the transaction. |
| `ErrIdempotencyKeyReused` | A key used for one statement is used for a different statement or arguments. | Use one key per logical operation, and retry with exactly what you sent. |
| `ErrIdempotencyTableMissing` | A write with an idempotency key, and the key table does not exist on a shard. | `db.EnsureIdempotencyTable(ctx)`, or add `shard.IdempotencyDDL` to your migrations. |
| `router.ErrNilKey`, `router.ErrUnsupportedKey`, `router.ErrKeyOutOfRange` | The shard key value cannot be routed: nil, a type other than integer, string or UUID, or an unsigned value above `MaxInt64`. They arrive wrapped with the key. | Pass the key as its declared type. |

## A shard failed: `*ShardError`

```go
var se *shard.ShardError
if errors.As(err, &se) {
    log.Printf("shard %s: %v", se.Shard, se.Err)
}
```

`*ShardError` says which shard a failure came from and wraps the cause, which
is usually a PostgreSQL error (`*pgconn.PgError`, with its SQLSTATE), a
`context.DeadlineExceeded` from `Config.ShardTimeout`, or a connection error.
DSNs never appear in errors.

How a failure reaches you depends on the statement:

| Statement | Behaviour |
|---|---|
| Query on one shard | `Query` returns the `*ShardError`. |
| Query on several shards | **Fails fast** by default: the first shard error cancels the others and is returned. With `shard.AllowPartial(ctx)` the shards that answered are merged, and `shard.ShardErrors(rows)` lists the ones that did not (the result then covers only the shards that answered: check it before trusting a count or a sum). Only a failure to start the query is tolerated; an error while a shard is streaming stops the iteration and appears in `rows.Err()`, labelled with the shard. |
| Write | **Not fail fast.** Every target is attempted. `Exec` returns the `WriteResult` with one outcome per shard, and an error if any failed (one `*ShardError`, or several joined with `errors.Join`: `errors.As` finds them). There is no atomicity across shards. `res.Failed()` and `res.FailedRows()` say what to send again; with `shard.WithIdempotencyKey` the retry applies the write only where it did not happen. |
| Statement in a transaction | The `*ShardError` of the transaction's shard. |

Writes are never retried by the library, and reads are not retried either: a
retry that hides a shard failure is the caller's decision. Reads are safe to
retry yourself; retry writes with an idempotency key.

## Limits and results

| Error | When | What to do |
|---|---|---|
| `ErrMergeLimitExceeded` | Merging several shards' results would hold more than `Config.MaxMergeRows` groups or distinct rows (aggregates, `GROUP BY`, `DISTINCT`). Streaming merges (`ORDER BY`, `LIMIT`) are not counted. | Narrow the query, or raise `MaxMergeRows`. |
| `context.Canceled`, `context.DeadlineExceeded` | The caller's context ended, or `Config.ShardTimeout` did for a shard. A cancelled merge stops reading, closes every shard's rows and releases its connections. | Normal; no cleanup is needed beyond closing `Rows`. |
| `ErrTxDone` (`sql.ErrTxDone`) | A statement, `Commit` or `Rollback` on a transaction that has ended. A deferred `Rollback` after `Commit` returns it; that is harmless. | Ignore it for the deferred rollback. |

## Setup errors

| Error | When | What to do |
|---|---|---|
| `ErrInvalidConfig` | `Open` or `NewPlanner`: a bad `Config`. The message lists **every** problem found (no shard, duplicate IDs, a missing DSN, buckets that do not cover 0-1023 exactly once, a negative limit, ...). | Fix each line of the message. |
| `registry.ErrInvalid` | `registry.New`: a missing key, a missing or global parent, a cycle, a duplicate name, a key type that disagrees with the group's. All problems are listed. | Fix each line. |
| `*ShardError` from `Open` | A shard could not be reached or pinged. Nothing is left open. | Check the shard's address, credentials and `sslmode`. |

## Migrations (package `migrate`)

| Error | When |
|---|---|
| `migrate.ErrFailed` | `Up` / `UpTo`: at least one shard failed or was not started. Wraps one `*migrate.ShardError` per failed shard. The `Result` still lists every shard. A failed shard stays at its last good version; call `Up` again after fixing the cause. |
| `migrate.ErrNoShards` | `migrate.New` with no shards. |
| `*migrate.ShardError` | A migration or status read failed on a shard. |

A shard left **dirty** by a crash mid-migration is reported by
`Status.Dirty()`, and `Up` refuses it; repair it with `Runner.Force` after
checking by hand what the migration did.

## Matching errors from different layers

The root package re-exports the sentinel errors defined in `analyze`, `plan`,
`merge` and `exec`, so `errors.Is(err, shard.ErrShardKeyRequired)` works
whichever layer returned the error.
