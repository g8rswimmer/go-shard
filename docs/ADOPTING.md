# Adopting go-shard with an existing database

go-shard routes statements to shards. It does not split a database for you:
there is no tool in the library that takes an unsharded PostgreSQL instance and
spreads its rows over shards, and moving buckets between shards later
(resharding) is outside v1 ([REQUIREMENTS section 8](REQUIREMENTS.md)). This
guide is how to plan and do the move yourself, with what the library gives you
to make it safe. [examples/adopt](../examples/adopt) is a working loader.

Read [QUERIES.md](QUERIES.md) first for what can and cannot run across shards.

## 1. Decide before you load anything

Moving data is the easy part. These decisions are expensive to change, because
changing the key or the shard count later means moving rows again.

**Pick the shard key.** Choose the column almost every query already filters
on, whose value never changes (a shard key is immutable; updating it is
refused). Usually this is an account, tenant or user id. A key with a few
very large values (one tenant with half the data) makes one shard hot, and
hashing cannot fix that.

**Classify every table.** Each table is exactly one of:

| Kind | Use it for | Cost |
|---|---|---|
| Sharded | The tables you pick the key for | Queries need the key, or `WithAllShards()` |
| Colocated | Tables owned by a sharded row (addresses of a profile) | Must carry the parent's key as a column |
| Global | Small, rarely changed reference data (countries, plans) | Every write goes to every shard, and is not atomic |

Work down your foreign keys. A table that references a sharded table and is
read together with it should be colocated: it gets the parent's key as a
column (add and backfill the column in the old database first if it only has
the parent through another table). A table that two sharded tables both
reference, with different keys, cannot be colocated with both. Either make it
global (if it is small), or accept that joins to it need two queries joined in
your application.

**Look at what breaks**, in the old schema:

- **Foreign keys** hold only inside a shard. Between a colocated table and its
  parent they still work; between tables on different keys, drop them and check
  in your application.
- **Unique constraints** hold per shard. A primary key `id` of a table sharded
  by `id` is fine; a unique `email` is not checked across shards.
- **`serial` and identity columns** count separately on each shard and
  collide. Switch to ids generated before the insert (UUID v4/v7, or an
  application id generator), and keep the old ids for existing rows.
- **Queries that join across keys, or aggregate in ways the merge does not
  support**, are listed in [QUERIES.md](QUERIES.md). Run your real queries
  through `Explain` (below) before you decide.
- **Transactions** cover one shard. Anything that must change atomically must be
  keyed to the same shard.

**Pick the shard count and the bucket map.** There are 1024 virtual buckets,
split evenly in the order the shards are listed unless you give each shard
explicit `Buckets`. The map is static configuration: it must never change
while data exists under it. Choose more shards than you need now only if you
accept the cost of running them; adding a shard later means moving data
yourself. See [OPERATIONS.md](OPERATIONS.md) for capacity.

## 2. Try your queries without moving anything

`shard.NewPlanner` routes and explains statements with no database connection,
and `shardtest.NewFake` lets you assert routing in unit tests. Declare your
registry, then check every important statement:

```go
planner, _ := shard.NewPlanner(cfg)
e, err := planner.Explain(ctx, "SELECT ... FROM orders WHERE customer_id = $1", 42)
// err is shard.ErrShardKeyRequired etc. for statements you must change
fmt.Print(e) // strategy, targets, and the merge steps for a fan-out
```

Collect the statements the application runs (from logs or `pg_stat_statements`),
run them all through `Explain`, and count how many are `single`, `all`, or
refused. A large share of `all` means the key is wrong, or the code needs
to change first. This is the cheapest test of the design you will get.

## 3. Create the shards

Create the empty schema on every shard with the migration runner
([examples/migrations](../examples/migrations)): the same files for every
shard, so sharded, colocated and global tables exist everywhere. Create the
idempotency table (`DB.EnsureIdempotencyTable`) if you will use keys. Use the
same PostgreSQL version, collation and extensions on all shards; the merge
assumes they agree.

Load global tables from a migration, so every shard has the same rows.

## 4. Load the data

[examples/adopt](../examples/adopt) is the pattern; run it with
`make example-adopt` and read it first. The rules it follows:

1. **Write through the library**, never directly into a shard. Each row then goes
   to the shard the router chooses for its key, which is also where the
   application will look for it.
2. **Parents before children**, in key order, in batches. A batch is one
   `INSERT` with many rows; the library splits it by shard.
3. **Make the loader re-runnable**: `OnConflictDoNothing` (or an upsert), so a
   loader that stops can be started again. A batch that partly fails reports
   which shards and rows failed ([examples/writes](../examples/writes)); for a
   repeated batch you can also use an idempotency key.
4. **Load from a consistent snapshot** (a replica, or one repeatable-read
   transaction per table) or stop writes during the cutover. The loader does
   not see changes made after it read a row.
5. **Keep the old ids.** Do not let the shards assign new ones.

For a big table, run several loaders on disjoint key ranges. Watch the pool
limits (`Config` max connections) and the load on the old database.

## 5. Verify before you switch

Do not trust the loader's success message. The example checks, and so should
you:

- **Counts**: rows in the old table equal the sum over the shards, per table.
- **Placement**: each row is on the shard the router chooses for its key. Ask a
  shard for its keys with `WithShard(id)` and `Explain` each key (sample, for
  large tables). A non-zero result means rows were written some other way, or the
  bucket map or key type changed, and those rows will not be found.
- **Colocation**: no child row without its parent on the same shard. Run this
  per shard with `WithShard`; a subquery across shards is refused.
- **Content**: compare checksums of ranges of rows, or compare the results of
  your real queries on the old database and on the shards.

## 6. Cut over

Plan for one of these:

- **Downtime**: stop writes, load the last changes, verify, switch the
  application to go-shard.
- **Dual write, then switch**: write to both while the loader catches up, verify,
  then read from the shards. You need to design how the two stay equal; the
  library does not do it. An idempotency key makes repeating a write to the
  shards safe.

Keep the old database for as long as you want a way back, but write to only one
place once you have switched: nothing copies shard changes back.

## What this does not solve

- **Resharding.** If you outgrow the shard count, you move rows yourself. Moving
  whole buckets is the least painful, which is why the bucket count is fixed, but
  the library has no tool for it in v1.
- **Rebalancing a hot key.** Everything with one key lives on one shard.
- **Cross-shard joins and transactions.** Design them out of the data model now.
