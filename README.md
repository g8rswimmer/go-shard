# go-shard

A Go library for maintaining a sharded PostgreSQL database.

- Routes each query to the right shard from table metadata, and fans out only when asked.
- Colocates related tables (for example a profile and its addresses) on one shard.
- Merges sorted, limited and aggregated results when more than one shard is hit.
- Applies migrations to every shard and reports drift.
- Explains where a query will run.

> **Status:** early development. Nothing is usable yet; see the
> [project plan](docs/PROJECT_PLAN.md) for progress.

## Design

- [Requirements](docs/REQUIREMENTS.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Project plan](docs/PROJECT_PLAN.md)

## Development

Requires Go 1.26+ and PostgreSQL 14+ (for the shards). See
[CONTRIBUTING.md](CONTRIBUTING.md).

```sh
make up     # three local Postgres shards
make test   # unit tests
```

## License

MIT, see [LICENSE](LICENSE).
