# Security policy

## Reporting a problem

Please do not open a public issue for a security problem. Use GitHub's private
reporting: [Report a vulnerability](https://github.com/g8rswimmer/go-shard/security/advisories/new)
(on the repository's Security tab). Include what you ran, what you expected and
what happened; a minimal reproduction helps. This is a small project, so
there is no guaranteed response time, but a report will be read and credited in
the fix if you want that.

## Supported versions

Before v1.0, only the latest release is supported. Fixes are released as a new
patch version ([VERSIONING.md](docs/VERSIONING.md)).

## What counts

go-shard sits between an application and its databases, so these are security
problems, not just bugs:

- **A statement runs on the wrong shard**, or on all shards, when the caller
  asked for one: it can read or change another tenant's data. A routing rule
  that does not refuse what it should is in scope.
- **Credentials or data in output:** a DSN or password in an error message, a
  log line, a span or a metric; statement arguments in a hook event (they are
  never meant to be there).
- **SQL the library builds from identifiers or values** that can be turned
  into other SQL (the `query` builder, the `migrate` table name option, the
  idempotency table).

Not in scope: SQL the application passes in itself (it is run as written, on
the shard it names); the security of PostgreSQL, `pg_query_go` or other
dependencies (report those upstream, and tell us if we need to update).

## For users

- Keep DSNs in secrets, not in code. The library keeps them out of its errors
  and hook events.
- Give the application's database role only the rights it needs on each shard.
- The demo and examples create and drop tables: run them only on throwaway
  databases (`make up`).
