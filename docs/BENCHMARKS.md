# Benchmarks

What the library costs on top of the database, and how fast results merge.
These numbers are a record of one machine at one commit, to spot regressions
and to answer "is it slow?"; they are not promises. Run them yourself:

```sh
make bench                 # no database needed (in-process fake shards)
make up && make bench-integration   # against the local PostgreSQL shards
make bench BENCHTIME=1x    # one iteration each: what CI does, to keep them building
```

Recorded on an Apple M2 (8 cores), macOS, Go 1.26.2, PostgreSQL 14 in Docker
Desktop on the same machine (so round trips are as short as they get: a real
network adds its own latency to both sides of every comparison).

## Routing overhead

`bench_test.go`: the shards are in-process fakes that answer instantly, so each
number is the library's own work (finding the shard, planning, running through
the executor) and nothing else. The baseline is the same call made straight on
`database/sql` against the same fake.

| Benchmark | Time | Allocations | What it measures |
|---|---:|---:|---|
| `DirectDatabaseSQL` | 0.33 µs | 8 | the baseline: one `database/sql` query, one row |
| `PlanOnly` | 0.9 µs | 22 | routing a built statement by its key: hash, bucket, shard, plan |
| `SingleShardWithShardKey` | 3.9 µs | 46 | `db.WithShardKey(k).Query(...)`: no analysis, the fixed cost of a call |
| `SingleShardWithHooks` | 3.9 µs | 46 | the same with `observe.Hooks` set (a no-op hook): hooks are free when they do nothing |
| `SingleShardBuilt` | 5.1 µs | 61 | a prepared `query` statement routed by the key it carries |
| `SingleShardBuildAndRun` | 6.5 µs | 77 | the same, building the statement on every call |
| `WriteSingleShard` | 4.8 µs | 40 | `UPDATE ... WHERE id = $1` through `ExecStatement` |
| `FanOutOrdered100RowsPerShard` | 44 µs | 414 | `ORDER BY` on 3 shards in parallel, 300 rows merged |
| **`SingleShardRawSQL`** | **106 µs** | 116 | **raw SQL: parsed by PostgreSQL's parser on every call** |
| `FanOutRawSQL` | 219 µs | 506 | the same on all shards, plus the rewrite for the merge |

**Reading this.** The library adds about **4-5 µs** to a call routed by a key
or a built statement. **Parsing raw SQL costs about 100 µs per call**, which is
twenty times that. Parsing is not cached (a cache keyed by SQL text is a
possible later optimisation, noted in ARCHITECTURE 5.3). On a hot path, prefer
a built statement or an explicit route (`WithShardKey`); raw SQL is for
convenience and for statements the builder does not cover.

## Against real PostgreSQL

`bench_integration_test.go` (build tag `integration`): three shards, 3,000
profiles, local round trips.

| Benchmark | Time | What it measures |
|---|---:|---|
| `RealDirectToTheShard` | 137 µs | `SELECT name FROM profiles WHERE id = $1` straight on the owning shard |
| `RealSingleShardBuilt` | 153 µs | the same through the library, built statement (**+12%**, about 15 µs) |
| `RealSingleShardRawSQL` | 289 µs | the same as raw SQL (**+110%**: the parser) |
| `RealFanOutOrderedLimit` | 544 µs | `ORDER BY id LIMIT 20` on all three shards, merged |
| `RealFanOutAggregate` | 577 µs | `SELECT name, count(*) ... GROUP BY name` on all three shards, merged |

The fan-out cost is mostly the slowest shard's round trip plus the connection
and goroutine work for three parallel calls; the merge itself is small (below).

## Merge throughput

`merge/bench_test.go`: the merge alone, over in-memory sources, so no database.
Each shard supplies 10,000 rows sorted by an integer id.

| Benchmark | Time | Rows/s | Allocations | Notes |
|---|---:|---:|---:|---|
| `OrderedMerge/shards=2` | 2.0 ms | 9.8 M | 59,767 | 20,000 rows |
| `OrderedMerge/shards=8` | 10.6 ms | 7.6 M | 239,801 | 80,000 rows |
| `OrderedMerge/shards=32` | 56.5 ms | 5.7 M | 959,926 | 320,000 rows; each row costs a few more heap steps |
| `OrderedMergeLimit` | 5.4 µs | | 113 | `ORDER BY ... LIMIT 20` over 8 shards of 10,000 rows: stops after 20 rows |
| `Concatenate` | 7.3 ms | 11.0 M | 239,795 | no `ORDER BY`: rows in shard order |
| `AggregateGroupBy/groups=10` | 16 µs | | 520 | 8 shards, one row per group each |
| `AggregateGroupBy/groups=1000` | 1.6 ms | | 53,026 | |
| `AggregateGroupBy/groups=20000` | 37 ms | | 1,098,193 | 160,000 input rows into 20,000 groups |
| `Distinct` | 11.9 ms | | 439,106 | 80,000 rows into 500 distinct values |

A merge handles millions of rows per second, so for any query a database
answers in milliseconds the merge is not the cost. Memory is bounded: streaming
merges hold one row per shard; aggregates, `GROUP BY` and `DISTINCT` hold their
groups and fail with `ErrMergeLimitExceeded` past `Config.MaxMergeRows`.

### What the benchmarks found

The first run of `OrderedMerge` showed throughput falling as shards were added
(3.8 M rows/s at 2 shards, 0.8 M at 32) and allocations growing with the number
of shards: 69 allocations per row at 32 shards. A profile put 62% of all
allocations in `compare`, which converted every integer to an exact `big.Rat`
for every heap comparison. Comparing two `int64` or two finite `float64`
values directly (same ordering, no allocation) and reusing the scan
destinations per source took it to the numbers above:

| `OrderedMerge` | Before | After |
|---|---:|---:|
| 2 shards | 5.2 ms, 239,760 allocs | 2.0 ms, 59,767 allocs |
| 8 shards | 55.3 ms, 3,123,569 allocs | 10.6 ms, 239,801 allocs |
| 32 shards | 379.7 ms, 22,028,413 allocs | 56.5 ms, 959,926 allocs |

Decimal (`numeric`) and mixed-type comparisons still use the exact path, because
they have to; a test pins that the fast paths order exactly as it does,
including at the extremes.
