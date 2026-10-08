# Fan-out and merge

Shows reading from every shard and getting one answer back.

A query that names a shard key goes to one shard. A query that does not is
refused, unless you say `WithAllShards()`: then it runs on every shard and the
results are merged as a single database would give them.

1. **Order, limit and offset:** the top five scores, and the next page.
2. **Aggregates with `GROUP BY`:** count, sum, average and max per team.
3. **`HAVING`:** it runs after the merge, on the combined groups.
4. **`DISTINCT`.**
5. **What is refused:** no shard key, a window function, `string_agg`: each
   says what to do instead.
6. **A shard that does not answer:** by default the query fails; with
   `shard.AllowPartial(ctx)` the other shards are merged and
   `shard.ShardErrors(rows)` says who did not answer.

```sh
make up                              # three local Postgres shards
go run ./examples/fanout
```

Expected output:

```
== 1. the five highest scores, from all shards
  name	score
  player-27	99
  player-08	96
  player-16	92
  player-24	88
  player-05	85

== 2. the next page
  name	score
  player-13	81
  player-21	77
  player-02	74
  player-29	73
  player-10	70

== 3. totals per team, busiest first
  team	players	total	average	best
  blue	10	505	50.5000000000000000	99
  green	10	465	46.5000000000000000	92
  red	10	535	53.5000000000000000	96

== 4. teams whose average is under 50 (HAVING runs after the merge)
  team	count
  green	10

== 5. distinct teams
  team
  blue
  green
  red

== 6. what is refused
  no shard key, no WithAllShards: shard: no shard key: the query on "players" does not say which shard it belongs to; add `id = $n` or `id IN (...)` as an AND-ed condition, or use db.WithShardKey / db.WithAllShards
  a window function:              shard: unsupported query: window functions on several shards: a window needs all rows in one place
  string_agg:                     shard: unsupported query: string_agg() on several shards: its per-shard results cannot be combined

== 7. a shard that does not answer
  by default the query fails: shard shard-02: ERROR: relation "players" does not exist (SQLSTATE 42P01)
  with AllowPartial: 24 players from the shards that answered
  did not answer: shard shard-02: ERROR: relation "players" does not exist (SQLSTATE 42P01)
```

## The pieces

```go
all := db.WithAllShards()

// The five highest scores on any shard.
rows, err := all.Query(ctx, "SELECT name, score FROM players ORDER BY score DESC, id LIMIT 5")

// Totals per team: each shard sums its own players, the library adds them up.
rows, err = all.Query(ctx, "SELECT team, count(*), avg(score) FROM players GROUP BY team HAVING avg(score) < 50")

// Tolerate a shard that is down, and find out which.
rows, err = all.Query(shard.AllowPartial(ctx), "SELECT count(*) FROM players")
failed := shard.ShardErrors(rows)
```

- `ORDER BY` merges the shards' sorted streams, one row per shard in memory,
  however large the result. Each shard is asked for `OFFSET + LIMIT` rows and
  the library skips the offset, so a deep page is expensive: use a keyset
  condition (`WHERE score < $1`) instead of a large `OFFSET`.
- An average is computed from each shard's sum and count, never by averaging
  averages.
- `GROUP BY` and `DISTINCT` hold their groups or distinct rows in memory, up to
  `Config.MaxMergeRows`; past that the query fails with
  `shard.ErrMergeLimitExceeded`.
- An `AllowPartial` result covers only the shards that answered: a count or sum
  is smaller by what the others hold. Check `ShardErrors` before trusting it.
- Built statements (`package query`) fan out too, and need no SQL parser or cgo.
