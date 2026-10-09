# Supported and unsupported queries

What go-shard can route, what it can merge, and what it refuses. A refused
statement fails with `ErrUnsupportedQuery` (or `ErrShardKeyRequired` /
`ErrCrossShardJoin`) **before anything is sent**, and the message names the
construct and what to do. Nothing is guessed.

Two ways to write a statement:

- **Raw SQL**, `db.Query(ctx, "SELECT ...", args...)`: read by PostgreSQL's own
  parser to find the shard key. Needs cgo ([BUILDING.md](BUILDING.md)).
- **Built statements**, `package query`: `query.From("profiles").Where(query.Eq("id", 42)).Build()`
  carries its routing and needs no parser. It covers `SELECT`, `INSERT`,
  `UPDATE` and `DELETE` with AND-ed conditions; anything else is written as SQL.

And three explicit overrides that skip reading the statement (the caller is
trusted): `db.WithShardKey(k)`, `db.WithShard(id)` and `db.WithAllShards()`.
Use `Explain` to see where any statement would go.

## Where a statement runs

| The statement | Runs on |
|---|---|
| Has `shard_key = x` (or `IN (x, y)`) AND-ed into its `WHERE` | The shard that owns `x` (or those shards) |
| Inserts rows into a sharded table | Each row to the shard that owns its key; a multi-shard `INSERT` is split so each shard gets only its rows |
| Reads only global tables | Any one shard, taking turns |
| Writes a global table | Every shard |
| Joins tables of one colocation group on their shard key | The shard of the key |
| Has no usable shard key condition | **Refused** (`ErrShardKeyRequired`), unless you use `WithAllShards()` |
| Joins tables that are not colocated | **Refused** (`ErrCrossShardJoin`) |
| DDL and other statements that cannot be routed (`CREATE INDEX`, ...) | **Refused** unless you say `WithAllShards()` (or use the migration runner, which is the better way to change a schema) |

The key must be an AND-ed condition on the key column itself: an `OR`, a
function around the column, or a range (`id > 5`) does not name a shard. The key
may be a literal or a parameter, and is converted to the declared key type
before hashing, so `42` and `'42'` route alike.

## Reading from several shards

A `SELECT` that runs on more than one shard (`WithAllShards()`, or key
conditions that name several shards) is rewritten and its results merged to
what one database would return.

| Supported | Notes |
|---|---|
| `ORDER BY` | Any number of terms, `ASC`/`DESC`, `NULLS FIRST/LAST`, ordinals (`ORDER BY 2`) and aliases. Terms not selected are fetched as hidden columns. |
| `LIMIT`, `OFFSET` | Numbers or parameters. Each shard returns `offset + limit` rows, so a deep `OFFSET` is expensive: prefer a keyset condition. |
| `DISTINCT` | Held in memory, bounded by `MaxMergeRows`. |
| `count`, `sum`, `min`, `max`, `avg` | Including `count(*)`. `avg` is rewritten to a sum and a count. |
| `GROUP BY` | By columns, aliases or ordinals. Groups are held in memory, bounded by `MaxMergeRows`. |
| `HAVING` | `AND`/`OR`/`NOT` over comparisons of an aggregate (or group column) with a constant or parameter. |
| Colocated joins, `WHERE`, expressions in the select list | Run on each shard as written. |

| Refused on several shards | Why / what to do |
|---|---|
| Window functions (`OVER`) | A window needs all rows in one place. Filter to one shard key, or compute in your code. |
| Subqueries in any clause | Run the inner query first, or filter by the shard key so only one shard is used. |
| `WITH` (CTEs) | Same. |
| `UNION`, `INTERSECT`, `EXCEPT` | Run the parts and combine them. |
| `FOR UPDATE` / `FOR SHARE` | Lock rows in a single-shard transaction. |
| `DISTINCT ON`, `FETCH FIRST ... WITH TIES` | |
| Aggregates other than the five above (`string_agg`, `array_agg`, `stddev`, ...) | Their per-shard results cannot be combined. |
| `DISTINCT`, `FILTER` or `ORDER BY` inside an aggregate | A value can occur on more than one shard. |
| An expression around an aggregate (`sum(x) / count(*)`) | Select the aggregates and combine them in your code. |
| `GROUPING SETS`, `ROLLUP`, `CUBE` | |
| `SELECT *` with aggregates or `GROUP BY` | List the columns. |
| `SELECT DISTINCT` ordered or filtered by something not selected | List the columns you sort by. |
| `ORDER BY ... USING` | |
| A `LIMIT` or `OFFSET` that is not a number or parameter, or is negative | |

**Known differences from one database** (also in ARCHITECTURE 5.5):

- Text is ordered as the `C` collation. If your databases use another (`en_US.UTF-8`), `ORDER BY` on text, and `MIN`/`MAX` of text, can differ from one database; use `COLLATE "C"` or a `C` database collation for text you sort by.
- An average of decimals can show a different number of decimals (the value is the same).
- Floating-point sums can differ in their last digits, because shards add in a different order.
- All shards must have the same collation and schema, which the migration runner provides.

## Writes

| Statement | Supported |
|---|---|
| `INSERT ... VALUES` into a sharded table | Yes. Every row needs a literal or parameter for the shard key. A row whose key is `DEFAULT`, NULL, a function or an expression cannot be placed and refuses the whole statement. An `INSERT` with no column list is refused. `ON CONFLICT` and `RETURNING` are kept. |
| `INSERT ... SELECT` | Only with an explicit route (`WithShardKey`, `WithShard`, ...). |
| `UPDATE`, `DELETE` | With the shard key condition, like reads. Without one: `WithAllShards()`. `UPDATE` cannot assign the shard key (`ErrShardKeyImmutable`). |
| Any write to a global table | Every shard. |
| `RETURNING` | Run the write with `Query` on one shard (a key, `WithShardKey`, a transaction): it returns the rows. A write that runs on several shards cannot return rows (`Exec` reports rows affected per shard). |
| Several statements in one call (`a; b`) | Refused: send them one at a time. |
| `WITH` (CTEs) on writes, `SELECT INTO` | Refused: route explicitly. |

A write to several shards is **not atomic**. The `WriteResult` reports each
shard's outcome; use `shard.WithIdempotencyKey` to retry safely. Details:
[ERRORS.md](ERRORS.md).

## Transactions

A transaction covers **one shard** (`db.InTx(ctx, shard.ForTable("profiles", id), ...)`).
Every statement inside is routed as usual and then checked: one that belongs to
another shard returns `ErrCrossShardTx` before anything is sent. Writes to
global tables are refused inside a transaction (they go to every shard).
Nested transactions and savepoints are not supported. A transaction across
shards (two-phase commit) is a non-goal for v1 (REQUIREMENTS section 8).
`tx.Unchecked()` runs a statement on the transaction's shard without routing
it, for SQL the router cannot place (`WITH`, scans by a non-key column).

## Things to know about your schema

- **Uniqueness is per shard.** A `UNIQUE` constraint or primary key is enforced
  by one PostgreSQL database, so it holds across all shards only if it includes
  the shard key (the primary key `id` of a table sharded by `id` does).
  Uniqueness on another column (an email address) is not checked across shards.
- **Generate ids that can be routed.** An `INSERT` needs its shard key in the
  statement, so ids must be generated before the insert: a UUID (v4 or v7) or
  an application-side id generator, not a database `serial`/identity column
  (each shard would count separately and collide).
- **Global tables are copies.** A write goes to every shard and is not atomic;
  if one shard fails, the copies differ until the write is repeated. Use an
  idempotency key to repeat it safely, and keep global data in migrations where
  you can.
- **The shards must have the same schema and collation**; the migration runner
  keeps the schema the same ([examples/migrations](../examples/migrations)).

## Not supported at all

Cross-shard joins, distributed transactions, and resharding (moving buckets
between shards) are outside v1; see REQUIREMENTS section 8.
