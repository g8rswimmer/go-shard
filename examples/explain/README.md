# Explain

Shows where a statement will run, and why, without running it.

`Explain` routes a statement with exactly the rules `Query` and `Exec` use. It
is the way to check a query before it ships, and the way to see what a
fan-out will do.

1. **One key:** one shard, and the reason (`profiles.id` -> `shard-02`).
2. **A colocated join:** a profile and its addresses are still one shard.
3. **Several keys:** only the shards that own them, with the merge steps.
4. **Every shard:** the rewritten SQL each shard receives (`avg` becomes a sum
   and a count; the `HAVING` is removed), and what the merge does with the
   answers.
5. **A batch insert:** which rows go to which shard, and the SQL each gets.
6. **A global table write:** every shard.
7. **A refusal:** no shard key. Explain returns the error running the
   statement would.
8. **Another refusal:** a window function on all shards.
9. **`db.Explainer(shard.ShardPlans())`:** PostgreSQL's own `EXPLAIN` from each shard,
   to check an index is used.
10. **`db.Explainer(shard.Analyze())`:** runs the statement to measure it. A write is
   run in a transaction that is rolled back, so it leaves no rows behind
   (a sequence advancing, or a trigger that calls out, are not undone).

`Explain` has fields (`Strategy`, `Targets`, `Reason`, `ShardSQL`, `Merge`,
`ShardPlans`) to inspect in code, and a `String()` for people. In unit tests,
`shardtest.NewFake` gives the same answers without a database; see
`examples/testing`.

```sh
make up                              # three local Postgres shards
go run ./examples/explain
```

Output (costs in the PostgreSQL plans depend on your version):

```
== 1. one key: one shard
  SELECT name FROM profiles WHERE id = $1
  op:        SELECT
  strategy:  single
  targets:   shard-02
  reason:    routed by key: profiles.id has 1 key value(s) -> [shard-02]
  shard sql: SELECT name FROM profiles WHERE id = $1

== 2. a profile and its addresses: still one shard
  SELECT p.name, a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1
  op:        SELECT
  strategy:  single
  targets:   shard-02
  reason:    routed by key: profiles.id has 1 key value(s) -> [shard-02]
  shard sql: SELECT p.name, a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1

== 3. several keys: those shards, merged
  SELECT id, name FROM profiles WHERE id IN (1, 2) ORDER BY name LIMIT 3
  op:        SELECT
  strategy:  multi
  targets:   shard-02, shard-03
  reason:    routed by key: profiles.id has 2 key value(s) -> [shard-02 shard-03]
  shard sql: SELECT id, name FROM profiles WHERE id IN (1, 2) ORDER BY name LIMIT 3
  merge:
    1. OrderedMerge(name)
    2. Limit(3)

== 4. every shard: the merge steps
  SELECT country, count(*) AS n, avg(age) FROM profiles GROUP BY country HAVING count(*) > 5 ORDER BY n DESC LIMIT 2
  op:        SELECT
  strategy:  all
  targets:   shard-01, shard-02, shard-03
  reason:    WithAllShards()
  shard sql: SELECT country, count(*) AS n, sum(age) AS avg, count(age), count(*) FROM profiles GROUP BY country
  merge:
    1. Aggregate(by country: count(*) = sum of counts, avg(age) = sum / count)
    2. Having(count(*) > 5)
    3. Sort(n DESC)
    4. Limit(2)
    5. DropHidden(2)

== 5. a batch insert is split by shard
  INSERT INTO profiles (id, name, country, age) VALUES (101, 'a', 'US', 30), (102, 'b', 'CA', 31), (103, 'c', 'MX', 32)
  op:        INSERT
  strategy:  multi
  targets:   shard-01, shard-02
  reason:    INSERT of 3 row(s) by profiles.id -> [shard-01 shard-02]
  shard sql:
    shard-01: INSERT INTO profiles (id, name, country, age) VALUES (101, 'a', 'US', 30), (102, 'b', 'CA', 31)
    shard-02: INSERT INTO profiles (id, name, country, age) VALUES (103, 'c', 'MX', 32)
  rows:
    shard-01: 0, 1
    shard-02: 2

== 6. a global table is written everywhere
  INSERT INTO countries (code) VALUES ('US')
  op:        INSERT
  strategy:  all
  targets:   shard-01, shard-02, shard-03
  reason:    INSERT global table "countries": every shard holds a copy
  shard sql: INSERT INTO countries (code) VALUES ('US')

== 7. refused: no shard key
  SELECT * FROM profiles WHERE name = 'x'
  refused: shard: no shard key: the query on "profiles" does not say which shard it belongs to; add `id = $n` or `id IN (...)` as an AND-ed condition, or use db.WithShardKey / db.WithAllShards

== 8. refused: a window function on all shards
  SELECT id, rank() OVER (ORDER BY age) FROM profiles
  refused: shard: unsupported query: window functions on several shards: a window needs all rows in one place

== 9. ShardPlans: PostgreSQL's plan on the shard that owns the key
  shard-02:
    Index Scan using profiles_pkey on profiles  (cost=0.15..8.17 rows=1 width=32)
      Index Cond: (id = '42'::bigint)

== 10. Analyze on a DELETE: measured on every shard, then rolled back
  shard-01: Delete on profiles, measured: true
  shard-02: Delete on profiles, measured: true
  shard-03: Delete on profiles, measured: true
  profiles after the ANALYZE: 30 (nothing was deleted)
```
