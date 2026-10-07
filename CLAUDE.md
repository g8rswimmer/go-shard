# CLAUDE.md

Guidance for working in this repository.

## What this is

`go-shard` is a Go library for maintaining a sharded PostgreSQL database: it routes queries to the right shard from table metadata, fans out and merges when needed, and applies migrations to every shard.

Read the design docs before changing behavior; they are the source of truth:

- [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md): what the library must do (FR-1 to FR-14)
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): how it is built, package layout, interfaces
- [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md): milestones M0 to M11 and their "done when" criteria

If a change alters a requirement or design decision, update the doc in the same change.

## Commands

```sh
make test              # unit tests with the race detector (no Docker)
make vet lint          # go vet and golangci-lint
make up                # 3 local Postgres shards on ports 5441-5443 (needs Docker)
make test-integration  # integration tests, build tag `integration`
make down              # stop the shards and delete their data
```

`make vet lint test` must pass before a change is done. Go 1.26+, PostgreSQL 14+.

## Go conventions

- **Prefer `switch` over `if` / `else if` / `else` chains.** A chain of two or more branches is a `switch` (use `switch { case cond: ... }` for conditions). A single `if` for an early return or guard clause is fine.
- **Every `switch` has a `default`**, even when there is nothing to do. Leave it empty with a short comment saying why, so it reads as deliberate:

  ```go
  switch {
  case n < 0:
      return errNegative
  case n == 0:
      return errZero
  default:
      // positive: nothing to reject
  }
  ```

  This applies to type switches and `select` as well.
- Match the surrounding code: comment density, naming, and idiom. Keep comments for the "why".
- Return errors with context, wrap with `%w`, and expose sentinel errors for conditions callers branch on (`errors.Is`).
- `context.Context` is the first parameter of anything that does I/O.
- Types that are built once and shared are immutable after construction (see `registry`, `router`).

## Testing

- Table-driven tests; unit tests must not need a database.
- Integration tests use the build tag `integration` and real Postgres (`make up`).
- Fixed test vectors (for example router bucket numbers) pin behavior that must never change. If one fails, fix the code, not the vector.

## Workflow

- Work on a branch per milestone; do not commit to `main` directly.
- Commit only when asked. End commit messages with the `Co-Authored-By` line the session specifies.
