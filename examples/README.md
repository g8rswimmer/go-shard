# Examples

Runnable examples, one directory each, with the output they print. All of them
use the local shards from `docker-compose.yml`:

```sh
make up
make example-quickstart     # or: go run ./examples/quickstart
make examples               # all of them
```

Set `SHARD_DSNS` (comma separated) to run against other databases. They only
touch the tables they create (and drop them when they finish), but run them
against databases you can afford to experiment on.

| Example | Shows |
|---|---|
| [quickstart](quickstart) | Configure shards, register tables, insert and query by key |
| [colocation](colocation) | A profile and its addresses on one shard: a transaction and a join |
| [fanout](fanout) | `WithAllShards()` with `ORDER BY`, `LIMIT`, aggregates, `GROUP BY`; what is refused; a shard that does not answer |
| [writes](writes) | A batch insert split across shards, partial failure, retrying with an idempotency key |
| [migrations](migrations) | Apply to every shard, status, drift, a migration that fails on one shard |
| [explain](explain) | Reading `Explain` output for single, multi and all-shard statements |
| [observability](observability) | Logs, OpenTelemetry spans and metrics, health and pool statistics |
| [adopt](adopt) | Move an existing database into shards: ordered, re-runnable load, then verify counts, placement and colocation ([guide](../docs/ADOPTING.md)) |
| [demo](demo) | The commands behind the [demo guide](../docs/DEMO.md): one per scenario, with `make demo-setup` |
| [testing](testing) | Unit testing with `shardtest.NewFake` (no Docker) and with a real cluster |

CI runs every example against real PostgreSQL and checks that it exits
successfully and prints its key lines (`TestExamplesRun`), so they cannot go
stale.
