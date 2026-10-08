# Colocation and transactions

Shows why related tables are kept on one shard, and what that buys you:

1. **A profile and its addresses commit together.** `addresses` is colocated
   with `profiles`, so an address always lives on its profile's shard, and one
   transaction on that shard can write both.
2. **An ordinary join finds them,** with no scatter-gather: the shard key in the
   `WHERE` clause is all the library needs.
3. **A failure rolls everything back.** If any step fails, the profile does not
   exist without its addresses.
4. **A statement for another shard is refused,** before anything is sent. A
   transaction covers one shard; the refusal says so and how to proceed, and the
   transaction carries on.

```sh
make up                              # three local Postgres shards
go run ./examples/colocation
```

Expected output (which shard each id lives on is fixed by the id):

```
== 1. a profile and its addresses commit together
  transaction on shard-03

== 2. read them back with a join
  Ada lives in London
  Ada lives in Paris

== 3. a failure rolls everything back
  the transaction failed: shard shard-01: ERROR: duplicate key value violates unique constraint "addresses_pkey" (SQLSTATE 23505)
  profiles named Grace afterwards: 0

== 4. a statement for another shard is refused
  refused: shard: statement is outside the transaction's shard: the transaction is on shard-02 but this statement belongs to [shard-03] (INSERT of 1 row(s) by profiles.id -> [shard-03]); a transaction covers one shard, so keep rows that change together on one shard with colocation, or run this statement outside the transaction
  Barbara was saved: 1, the stray was not: 0
```

## The pieces

```go
reg, _ := registry.New(
    registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
    registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
)

// Commits if the function returns nil; rolls back on an error or a panic.
err := db.InTx(ctx, shard.ForTable("profiles", 7), func(tx shard.Tx) error {
    if _, err := tx.Exec(ctx, "INSERT INTO profiles ..."); err != nil {
        return err
    }
    _, err := tx.Exec(ctx, "INSERT INTO addresses ...")
    return err
})
```

- `shard.ForTable("profiles", id)` picks the shard that owns the id, converting
  it to the table's key type. `ForKey` uses the key as given and `ForShard`
  names a shard.
- `tx` is a `shard.Querier`, so repository code that takes a `Querier` runs
  unchanged inside a transaction.
- Every statement is routed as usual, then checked against the transaction's
  shard (`ErrCrossShardTx`). Reads of global tables run on that shard; writes to
  global tables are refused because they would touch every shard.
- Close the rows of one query before running the next statement: a transaction
  has a single connection.
- For SQL the router cannot place (a `WITH`, a scan by a non-key column), and
  which you know belongs on this shard, use `tx.Unchecked()`.
- Cancelling the context passed to `Begin` rolls the transaction back.
