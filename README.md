# go-shard

A Go library for a sharded PostgreSQL database. It routes each statement to the
shard that owns the data, fans out and merges results only when you ask for it,
and applies migrations to every shard.

- **Routing from the SQL.** Describe which tables are sharded, colocated or
  global; the library reads the shard key from `WHERE id = $1` or
  `VALUES (...)` and sends the statement to the right shard. A statement that
  does not say where it belongs is refused, not guessed.
- **Colocation.** A profile and its addresses live on one shard, so one
  transaction writes both and an ordinary join reads them.
- **Fan-out and merge.** `db.WithAllShards().Query(...)` runs everywhere and
  returns what one database would: `ORDER BY`, `LIMIT`/`OFFSET`, `DISTINCT`,
  `COUNT`/`SUM`/`MIN`/`MAX`/`AVG`, `GROUP BY`, `HAVING`.
- **Safe writes.** Batch inserts are split by shard; a partial failure is
  reported per shard; an idempotency key makes a retry safe.
- **Migrations.** One set of files applied to every shard in parallel, with
  status and drift detection.
- **See what it does.** `Explain` shows where a statement would run and why;
  hooks give logs, OpenTelemetry traces and metrics; a fake lets you unit test
  without a database.

> **Status:** pre-release (v0.1.0 is next). The API may still change before
> v1.0; see [versioning](docs/VERSIONING.md) and the [changelog](CHANGELOG.md).

## Install

```sh
go get github.com/g8rswimmer/go-shard
```

Requires Go 1.26+ and PostgreSQL 14+ on every shard. Routing raw SQL uses
PostgreSQL's own parser through cgo, so building needs a C compiler; without
cgo everything works except routing raw SQL automatically
([building](docs/BUILDING.md)).

## Quick start

```go
// 1. Which tables are sharded, and by what. The key type makes 42 and "42" route alike.
reg, _ := registry.New(
    registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
)

// 2. The shards. Their hash buckets are split evenly in the order listed.
db, err := shard.Open(ctx, shard.Config{
    Registry: reg,
    Shards: []shard.ShardConfig{
        {ID: "shard-01", DSN: "postgres://..."},
        {ID: "shard-02", DSN: "postgres://..."},
        {ID: "shard-03", DSN: "postgres://..."},
    },
})
defer db.Close()

// 3. Plain SQL. The id in the statement picks the shard.
db.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, $2)", 42, "Ada")
rows, _ := db.Query(ctx, "SELECT name FROM profiles WHERE id = $1", 42)

// 4. Ask for every shard explicitly; the results are merged.
top, _ := db.WithAllShards().Query(ctx,
    "SELECT name FROM profiles ORDER BY name LIMIT 10")

// 5. See where something would go, without running it.
plan, _ := db.Explain(ctx, "SELECT name FROM profiles WHERE id = $1", 42)
fmt.Println(plan)
```

To run a complete version against three local databases:

```sh
make up                          # three PostgreSQL shards in Docker
go run ./examples/quickstart
```

## Documentation

| | |
|---|---|
| [Examples](examples) | One runnable program per feature, each with its output |
| [Supported and unsupported queries](docs/QUERIES.md) | What is routed, merged, refused, and why |
| [Error reference](docs/ERRORS.md) | Every error, when it happens, what to do |
| [Operating it](docs/OPERATIONS.md) | Timeouts, shard failures, retries, capacity |
| [Benchmarks](docs/BENCHMARKS.md) | What the library costs; merge throughput |
| [Building](docs/BUILDING.md) | cgo, cross-compiling, Docker |
| [Versioning](docs/VERSIONING.md) · [Changelog](CHANGELOG.md) · [Upgrading](docs/UPGRADING.md) | |
| Design: [requirements](docs/REQUIREMENTS.md), [architecture](docs/ARCHITECTURE.md), [project plan](docs/PROJECT_PLAN.md) | |

API documentation is on [pkg.go.dev](https://pkg.go.dev/github.com/g8rswimmer/go-shard).

## What it is not

It does not run joins across shards or transactions that span shards, and it
does not move data between shards when you add one. These are deliberate
limits of v1 ([why, and what a v2 could do](docs/REQUIREMENTS.md)). Colocating
related tables, global tables and idempotent retries cover most of what those
would be used for.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md).

```sh
make up     # three local Postgres shards
make test   # unit tests (no Docker)
make bench  # benchmarks
```

## License

MIT, see [LICENSE](LICENSE).
