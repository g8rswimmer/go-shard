# Versioning policy

go-shard follows [semantic versioning](https://semver.org). Releases are Git
tags `vMAJOR.MINOR.PATCH` on `main`, made after CI passes, each with an entry in
[CHANGELOG.md](../CHANGELOG.md).

## Before v1.0.0

While the version is `v0.x`:

- A **minor** release (`v0.2.0`) may change the API in ways that break callers.
  Each such change is listed in the changelog under **Changed** or **Removed**
  and explained in [UPGRADING.md](UPGRADING.md) with what to do instead.
- A **patch** release (`v0.1.1`) only fixes bugs and never breaks callers.

## From v1.0.0

- **Major** releases may break the API; **minor** releases add features and
  never break it; **patch** releases fix bugs.
- A feature is deprecated before it is removed: the godoc says `Deprecated:`
  and names the replacement, and it keeps working for at least one minor release.

## What is the public API

| Package | Stability |
|---|---|
| `github.com/g8rswimmer/go-shard` | Public. Everything exported. |
| `registry`, `query`, `migrate`, `observe`, `observe/otel`, `shardtest` | Public. |
| `router` | Public for the types `Config` takes (`BucketRange`, `Assignment`) and the key errors; the rest may change in a minor release. |
| `analyze`, `merge` | Public for the interfaces you can implement (`analyze.Analyzer`, `merge.Planner`) and the types they use. |
| `plan`, `exec`, `analyze/pgparse` | **Implementation.** Exported because the root package needs them; their types reach you only through aliases in the root package (`ShardError`, the sentinel errors). They may change in any release. |

Output meant for people (`Explain.String()`, log lines, error messages) is
stable enough to read and diff but is not an interface: do not parse it. Use the
fields (`Explain.Targets`, `errors.Is`, `errors.As`).

## What never changes silently

These are persistent: changing them would make data unreachable or break
existing databases. A change is a **major** release (or, before v1.0, a minor
release with a migration note), never a patch.

- **Where a key lives.** The canonical key encoding, `xxhash64`, and the 1024
  buckets. They are pinned by fixed test vectors; a failing vector means the
  code is wrong, not the vector.
- **The idempotency table** (`go_shard_idempotency_keys`) and the migration
  version table (`schema_migrations`) schemas.
- **Which statements are routed where.** A statement that routed to one shard
  does not start routing to another. A statement that was refused may become
  supported in a minor release; a supported one is not refused without a major
  release (a statement that gave wrong results is fixed in a patch).

## Supported versions

- **Go:** the version in `go.mod` (currently 1.26). Raising the minimum is
  allowed in a minor release and is noted in the changelog.
- **PostgreSQL:** 14 and later, on every shard, tested in CI on 14 and 18.
  Dropping a version happens in a minor release after that PostgreSQL release
  reaches end of life, and is noted in the changelog.
- **Shards must agree:** the same PostgreSQL major version family, schema and
  collation. The migration runner is how they stay the same.
