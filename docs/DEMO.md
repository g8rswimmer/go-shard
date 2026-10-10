# Demo guide

A hands-on tour of go-shard against three real PostgreSQL databases on your
machine. Each scenario gives a command, the output you should see, and what it
proves. It doubles as the manual acceptance test before a release (FR-14), and
CI runs every command in it and compares the output, so what you read here is
what the library does.

You do not need to read the code. Every command is
`go run ./examples/demo <command>`; the source is in
[examples/demo](../examples/demo/main.go) if you want to see how a scenario
works.

## Before you start

- Go 1.26+, a C compiler (the SQL parser uses cgo, see [BUILDING.md](BUILDING.md)),
  Docker with the `docker compose` plugin.
- Run every command from the root of the repository.
- The demo owns the tables `profiles`, `addresses`, `countries` and
  `demo_migrations` on the three `make up` databases (ports 5441 to 5443).
  `make demo-reset` drops them. Do not point it at databases you care about.
- Each scenario that changes data puts it back, except the migration scenario,
  which leaves the shards at version 3. Run `make demo-reset` to go back to the start.

## Setup

```sh
make demo-setup
```

This starts the three shards (`make up`), applies migrations 1 and 2 to each
and loads 30 profiles with two addresses each. Rows are written through the
library, so each lands on the shard that owns its id. Check it:

```sh
go run ./examples/demo status
```

```text
  latest: 3
  shard-01: 2 (behind)
  shard-02: 2 (behind)
  shard-03: 2 (behind)
  drift: no
rows:
  shard-01: 10 profiles, 20 addresses
  shard-02: 6 profiles, 12 addresses
  shard-03: 14 profiles, 28 addresses
```

Migration 3 exists but is not applied yet (`behind`); scenario 7 uses it.
The 30 profiles (ids 1 to 30) are named `profile-NN`, have a tier (`bronze`,
`silver`, `gold`) and a score, and are spread over the shards by the router.
`countries` is a global table: the same three rows on every shard.

- [ ] The command ends with the table above: three shards at version 2, 30 profiles in total.

## 1. A query by shard key hits exactly one shard

```sh
go run ./examples/demo where 7
```

```text
== Explain: SELECT name, tier, score FROM profiles WHERE id = $1  [id = 7]
  op:        SELECT
  strategy:  single
  targets:   shard-03
  reason:    routed by key: profiles.id has 1 key value(s) -> [shard-03]
  shard sql: SELECT name, tier, score FROM profiles WHERE id = $1
== the row
  name	tier	score
  profile-07	silver	59
== profiles with id 7, counted on every shard directly
  shard-01: 0
  shard-02: 0
  shard-03: 1
```

`Explain` says, without running anything, that the statement goes to one shard
and why. The library found the key in `WHERE id = $1`. The last three lines count the
row on each shard through a connection that bypasses routing. Look at the
database yourself:

```sh
docker compose exec -T shard-03 psql -U shard -d shard -c "SELECT id, name FROM profiles WHERE id = 7"
```

```text
 id |    name    
----+------------
  7 | profile-07
(1 row)
```

```sh
docker compose exec -T shard-01 psql -U shard -d shard -c "SELECT id, name FROM profiles WHERE id = 7"
```

```text
 id | name 
----+------
(0 rows)
```

Try another key: `go run ./examples/demo where 8`.

- [ ] `Explain` shows `strategy: single` and one target.
- [ ] The row is on that shard and on no other.
- [ ] `psql` on the other shards finds nothing.

## 2. A profile and its addresses are on the same shard

```sh
go run ./examples/demo colocated 7
```

```text
== profile 7 and its addresses, on every shard
  shard-01: 0 profile, 0 addresses
  shard-02: 0 profile, 0 addresses
  shard-03: 1 profile, 2 addresses
== a join of the two, with the shard key in WHERE
  op:        SELECT
  strategy:  single
  targets:   shard-03
  reason:    routed by key: profiles.id has 1 key value(s) -> [shard-03]
  shard sql: SELECT p.name, a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1 ORDER BY a.city
  name	city
  profile-07	Lisbon
  profile-07	Porto
```

`addresses` is declared *colocated* with `profiles`: its `profile_id` column is
the same shard key. So a profile's addresses live with it, one transaction can
write both, and an ordinary join with the key in `WHERE` is a single-shard
query, with no scatter-gather.

- [ ] The profile and both addresses are on the same shard, and nowhere else.
- [ ] The join is `strategy: single` and returns two rows.

## 3. An all-shards query returns sorted, limited, aggregated results

```sh
go run ./examples/demo fanout
```

```text
== the 5 highest scores
  strategy: all on 3 shards
  merge 1: OrderedMerge(score DESC, id)
  merge 2: Limit(5)
  id	name	score
  27	profile-27	99
  8	profile-08	96
  16	profile-16	92
  24	profile-24	88
  5	profile-05	85
== per tier: count, sum, min and max of the score
  strategy: all on 3 shards
  merge 1: Aggregate(by tier: count(*) = sum of counts, sum(score) = sum of sums, min(score) = smallest, max(score) = largest)
  merge 2: Sort(tier)
  tier	count	sum	min	max
  bronze	10	505	10	99
  gold	10	535	7	96
  silver	10	465	3	92
== everyone
  strategy: all on 3 shards
  merge 1: Aggregate(count(*) = sum of counts, sum(score) = sum of sums)
  count	sum
  30	1505
```

`db.WithAllShards()` is the explicit opt-in for a query on every shard. Each
shard runs the query and the library merges the answers as one database would.
The `merge` lines are the steps it takes. Check the numbers against the
dataset: ten profiles in each tier, 30 in all, and the highest score is 99
(profile 27).

- [ ] The top 5 are in descending order of score, whichever shard each is on.
- [ ] The three tiers have 10 profiles each; the totals are 30 and 1505.

## 4. A query with no shard key is refused

```sh
go run ./examples/demo nokey
```

```text
== SELECT id, name FROM profiles WHERE tier = 'gold'
  refused: shard: no shard key: the query on "profiles" does not say which shard it belongs to; add `id = $n` or `id IN (...)` as an AND-ed condition, or use db.WithShardKey / db.WithAllShards
== the same query, asking for every shard on purpose
  gold profiles: 10
```

Nothing was sent to any shard. The error says what to add. Asking for every
shard has to be said out loud.

- [ ] The first query fails with `no shard key` and says how to fix it.
- [ ] The all-shards version returns the count of gold profiles (10).

## 5. A transaction rolls back cleanly, and another shard is refused

```sh
go run ./examples/demo tx
```

```text
== a profile and its addresses in one transaction, then a failure
  transaction on shard-02
  failed: shard shard-02: ERROR: duplicate key value violates unique constraint "addresses_pkey" (SQLSTATE 23505)
  profiles with id 1001 afterwards: 0
== a statement for another shard is refused
  refused: shard: statement is outside the transaction's shard: the transaction is on shard-02 but this statement belongs to [shard-01] (INSERT of 1 row(s) by profiles.id -> [shard-01]); a transaction covers one shard, so keep rows that change together on one shard with colocation, or run this statement outside the transaction
  profiles with id 1002 afterwards: 0
```

The first transaction inserts a profile and then two addresses with the same
id. The second statement fails and PostgreSQL rolls back the whole transaction,
so the profile is gone too. The second transaction tries to insert a row that
belongs to another shard: it is refused before anything is sent, because a
transaction covers one shard. Nothing from either is kept.

- [ ] Profile 1001 does not exist afterwards.
- [ ] The stray insert is refused with an error naming both shards.

## 6. A batch write when a shard is stopped

This one stops `shard-02` for a few seconds with `docker compose stop` and
starts it again afterwards, so run it on the shards from `make up`.

```sh
go run ./examples/demo batch
```

```text
== stopping shard-02 (docker compose stop shard-02)
== one INSERT of 12 profiles, ids 201 to 212
  shard-01: wrote 2 rows (rows [3 11])
  shard-02: FAILED (rows [0 2 4 7 9])
  shard-03: wrote 5 rows (rows [1 5 6 8 10])
  failed on shard-02: rows [0 2 4 7 9] were not written
== starting shard-02 again
== the same statement and key again
  shard-01: already applied, 2 rows (rows [3 11])
  shard-02: wrote 5 rows (rows [0 2 4 7 9])
  shard-03: already applied, 5 rows (rows [1 5 6 8 10])
  profiles 201 to 212 on all shards: 12
```

One `INSERT` of 12 rows was split by shard. With `shard-02` down, the other
two shards committed their rows and the result says exactly which rows were
not written. There is no atomicity across shards, so the library reports it
rather than hiding it. After the shard came back, the same statement with the
same idempotency key was sent again: the shards that had applied it did not
apply it twice, and `shard-02` wrote its rows. The command deletes the 12 rows
when it finishes.

- [ ] The shards that were up report `wrote`; `shard-02` reports `FAILED` and its rows.
- [ ] The retry says `already applied` for the first and last shards and `wrote 5 rows` for `shard-02`.
- [ ] 12 profiles in all; `docker compose ps` shows all three shards running afterwards.

## 7. A migration that fails on one shard, drift, recovery

Migration 3 adds a constraint: no name longer than 20 characters. First put a
long name on one shard:

```sh
go run ./examples/demo migrate-break
```

```text
profile 9001, with a 40-character name, is on shard-01
```

Apply the migration. The policy here is `ContinueOnFailure`, so the other
shards are migrated too:

```sh
go run ./examples/demo migrate
```

```text
== Up
  shard-01: FAILED, stayed at 2
  shard-02: 2 -> 3
  shard-03: 2 -> 3
== status
  latest: 3
  shard-01: 2 (behind)
  shard-02: 3
  shard-03: 3
  drift: yes
  Up returned: migrate: migration failed: 1 of 3 shards failed: shard shard-01: migration 3: check constraint "profiles_name_length" of relation "profiles" is violated by some row (line 1)
```

The shard that holds the long name could not add the constraint. Its
migration ran in one transaction, so it stayed at version 2 and is not left
half done; the other two moved to 3. `Status` shows the drift. Fix the cause
and run it again:

```sh
go run ./examples/demo migrate-fix
```

```text
removed profile 9001
== Up
  shard-01: 2 -> 3
  shard-02: already at 3
  shard-03: already at 3
== status
  latest: 3
  shard-01: 3
  shard-02: 3
  shard-03: 3
  drift: no
```

- [ ] Exactly one shard fails, and the error names the shard and the constraint.
- [ ] That shard stayed at version 2, not dirty; the others are at 3 and `drift: yes`.
- [ ] After the fix, only the failed shard migrates, the others are `already at 3`, and `drift: no`.

## 8. Health and logs

```sh
go run ./examples/demo health
```

```text
shard-01: healthy=true, connection limit 10
shard-02: healthy=true, connection limit 10
shard-03: healthy=true, connection limit 10
```

`DB.Health` pings every shard in parallel; stop one (`docker compose stop shard-02`)
and run it again to see `healthy=false` (`shard.Open` itself refuses to start
when a shard is unreachable, so this is for a running program; start the shard
again afterwards).

```sh
go run ./examples/demo logs
```

```text
level=DEBUG msg="shard statement done" kind=query shard=shard-03
level=DEBUG msg="shard statement" kind=query strategy=single targets=shard-03
level=DEBUG msg="shard statement" kind=query strategy=all targets=shard-01,shard-02,shard-03
level=ERROR msg="shard statement failed" kind=query error="shard: no shard key: the query on \"profiles\" does not say which shard it belongs to; add `id = $n` or `id IN (...)` as an AND-ed condition, or use db.WithShardKey / db.WithAllShards"
```

The hooks (`observe.Slog`) log one line per statement: what kind, which
strategy, which shards. The first query also has a line per shard. The last
statement is the refused one, logged at a higher level. Times and durations are
left out here so the output is the same every run; real logs have them.

`observe/otel` turns the same events into spans and metrics. This example
prints what it recorded:

```sh
go run ./examples/observability
```

- [ ] One line per statement, with the right strategy and targets.
- [ ] The refused statement is logged as an `ERROR` and names no targets.

## Starting over, and cleaning up

```sh
make demo-reset
```

drops the demo tables on all shards and loads them again, so you can repeat
any scenario. `make down` stops the shards and deletes their data.

## Acceptance summary

For a release, run the guide from a clean state (`make down`, then
`make demo-setup`) and tick every box above. The scenarios cover:

| Scenario | Shows | Requirement |
|---|---|---|
| 1 | Routing by shard key, `Explain` | FR-3, FR-4, FR-9 |
| 2 | Colocation | FR-6 |
| 3 | Fan-out and merge | FR-5 |
| 4 | No shard key refused | FR-4 |
| 5 | Single-shard transactions | FR-8 |
| 6 | Partial failure, idempotent retry | FR-7 |
| 7 | Migrations and drift | FR-10 |
| 8 | Health, logs | FR-11 |

CI runs this file with `make demo-check`: every `sh` block is run from the
repository root, and when a `text` block follows it, the output must match.
A block marked `sh skip` is shown but not run.

## If something goes wrong

- `is make up running?` The shards are not reachable on ports 5441 to 5443.
  Run `make up`, and check that nothing else uses those ports.
- `relation "profiles" already exists` while setting up: a table from something
  else is in the way. `make demo-reset` replaces the demo tables.
- Scenario 7 shows no failure: the migration was already applied. Run
  `make demo-reset` first.
- A shard stays stopped after an interrupted scenario 6:
  `docker compose start shard-02`.
