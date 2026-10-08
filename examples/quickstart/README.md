# Quickstart

Shows the basics: describe your tables, open the shards, write a row to the
shard that owns its key, then read and update it with plain SQL. The library
finds the shard from the shard key in the SQL.

```sh
make up                              # three local Postgres shards
go run ./examples/quickstart
```

Expected output (the shard each profile lands on is fixed by its id):

```
wrote profile 1 (Ada) to shard-03
wrote profile 2 (Grace) to shard-02
wrote profile 3 (Edsger) to shard-03
wrote profile 4 (Barbara) to shard-03
wrote profile 5 (Alan) to shard-02
wrote profile 6 (Margaret) to shard-01
read profile 3: Edsger
renamed profile 3 on shard-03 (1 row)
refused: shard: no shard key: the query on "profiles" does not say which shard it belongs to; add `id = $n` or `id IN (...)` as an AND-ed condition, or use db.WithShardKey / db.WithAllShards
```

What to notice:

- **Reads and updates need no hints.** `SELECT ... WHERE id = $1` and
  `UPDATE ... WHERE id = $1` run on the shard that owns the id, because the
  library reads the shard key from the `WHERE` clause.
- **Inserts still name the key** with `db.WithShardKey(id)`. Routing an INSERT
  from its `VALUES` arrives in a later milestone.
- **A statement that does not say where it belongs is refused**, not guessed.
  The last line shows the error, which says how to fix it.
- **The key type is required** (`registry.Type(registry.KeyInt)`). Keys are
  converted to it before routing, so the number 42 and the text "42" go to the
  same shard.
- `WithAllShards()` runs a statement on every shard, used here to create the table.
