# Quickstart

Shows the basics: describe your tables, open the shards, write a row to the
shard that owns its key, and read it back.

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
read profile 3 back: Edsger
```

What to notice:

- You never choose a shard by hand when you write or read a profile: the id
  decides, and the same id always goes to the same shard.
- Today you tell the library the key with `db.WithShardKey(id)`. Routing
  straight from the SQL arrives in a later milestone.
- `WithAllShards()` runs a statement on every shard, used here to create the table.
