# Migrations

Applies the same schema to every shard, and shows what happens when one shard
cannot take a migration.

A migration file is the same for every shard, so the sharded tables, the
colocated tables and the global tables (with their data) exist everywhere.
Migrations are numbered SQL files (`001_profiles.up.sql`, ...) and go forward
only. Each runs as one transaction: it happens completely or not at all.

1. **Before:** `Status` on shards nothing has been applied to. They agree, but
   they are all behind.
2. **`Up`:** applies the pending migrations. Here `shard-02` has a leftover
   table in the way of migration 3. The default policy, `HaltOnFailure`, stops
   starting new shards after the first failure, so `shard-03` is not started
   (with one shard at a time; shards already running when one fails finish).
   `ContinueOnFailure` migrates every shard it can instead. The failed shard
   stays at its last good version, with nothing from migration 3 left behind.
3. **`Status`:** shows each shard's version and reports the drift.
4. **Fix and re-run:** after removing the leftover table, `Up` finishes the
   shards that are behind and leaves the others alone. All shards end at the
   same version.
5. **A global table:** migration 2 creates and fills `ex_countries`, so every
   shard has the same rows.
6. **An unreachable shard:** `Status` names it and counts it as drift.

`db.Migrations(source, options...)` builds the `migrate.Runner` from the
shards in your `shard.Config`. Without a `shard.DB`, use
`migrate.New(shards, source, options...)`. The source is a directory
(`migrate.FromURL("file://./migrations")`) or an `fs.FS` such as an
`embed.FS`, as here. In tests, `shardtest.NewCluster(t, 3,
shardtest.WithMigrations(url))` applies them to a fresh cluster.

```sh
make up                              # three local Postgres shards
go run ./examples/migrations
```

It only touches tables whose names start with `ex_`, and its own version
table (`ex_migrations`).

Output:

```
== 1. before anything is applied
  latest: 3
  shard-01: none (behind)
  shard-02: none (behind)
  shard-03: none (behind)
  drift: no

== 2. Up, with shard-02 blocked by a leftover ex_badges table
  error: migrate: migration failed: 1 of 3 shards failed, 1 not started (HaltOnFailure): shard shard-02: migration 3: relation "ex_badges" already exists (line 1)
  shard-01: none -> 3
  shard-02: stayed at 2
  shard-03: not started (HaltOnFailure)

== 3. Status shows the drift
  latest: 3
  shard-01: 3
  shard-02: 2 (behind)
  shard-03: none (behind)
  drift: yes

== 4. fix the cause (drop the leftover table on shard-02) and run Up again
  shard-01: already at 3
  shard-02: 2 -> 3
  shard-03: none -> 3
  latest: 3
  shard-01: 3
  shard-02: 3
  shard-03: 3
  drift: no

== 5. the global table has its rows on every shard
  shard-01: 3 countries
  shard-02: 3 countries
  shard-03: 3 countries

== 6. a runner that cannot reach a shard says which
  unreadable: shard-99 (drift: true)
```
