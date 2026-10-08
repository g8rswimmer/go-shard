# Writes

Shows what happens when one write touches several shards:

1. **A batch is split.** One `INSERT` with many rows is sent to the shards that
   own the rows, each receiving only its own.
2. **One shard fails, the others commit.** There is no atomicity across shards,
   so the result reports every shard: what it wrote, and for a failure which
   rows were lost (`FailedRows`).
3. **A retry with an idempotency key is safe.** Shards that already applied the
   write do not apply it again; the one that failed does.
4. Without a key, repeating a half-applied batch fails on the rows that were
   already written.

```sh
make up                              # three local Postgres shards
go run ./examples/writes
```

Expected output:

```
== 1. a batch is split across shards
  shard-01: wrote 3 rows (rows [5 7 10])
  shard-02: wrote 3 rows (rows [1 4 8])
  shard-03: wrote 6 rows (rows [0 2 3 6 9 11])

== 2. one shard fails, the others commit
  shard-01: wrote 5 rows (rows [0 1 3 5 11])
  shard-02: FAILED (rows [2 4 6 9]): shard shard-02: ERROR: relation "items" does not exist (SQLSTATE 42P01)
  shard-03: wrote 3 rows (rows [7 8 10])
failed on shard-02: rows [2 4 6 9] were not written

== 3. retry with the same idempotency key
  shard-01: already applied, 5 rows (rows [0 1 3 5 11])
  shard-02: wrote 4 rows (rows [2 4 6 9])
  shard-03: already applied, 3 rows (rows [7 8 10])
total rows reported: 12 (all 12, none written twice)

== 4. the same retry without a key
error: write failed on 3 of 3 shards: shard shard-01: ERROR: duplicate key value violates unique constraint "items_pkey" (SQLSTATE 23505)
```

The example breaks `shard-02` by renaming its table. A real failure (an outage,
a failover, a full disk) looks the same to your code.

## Using idempotency keys

```go
ctx = shard.WithIdempotencyKey(ctx, "load-batch-2")
res, err := db.ExecStatement(ctx, st)
// on error, call again with the same ctx and the same statement
```

- Pick **one key per logical operation**. The same key with a different
  statement or arguments is refused (`ErrIdempotencyKeyReused`).
- Keys are recorded on each shard, in the same transaction as the write, in a
  table you create once with `db.EnsureIdempotencyTable(ctx)` (or add
  `shard.IdempotencyDDL("")` to your migrations).
- Old keys pile up: call `db.PruneIdempotencyKeys(ctx, 7*24*time.Hour)` from a
  scheduled job, keeping them as long as a retry can happen.
