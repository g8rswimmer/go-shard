# AGENTS.md

Guidance for working in this repository, for people and for coding agents. (`CLAUDE.md` imports this file.)

## What this is

`go-shard` is a Go library for maintaining a sharded PostgreSQL database: it routes queries to the right shard from table metadata, fans out and merges when needed, and applies migrations to every shard.

Read the design docs before changing behavior; they are the source of truth:

- [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md): what the library must do (FR-1 to FR-14)
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): how it is built, package layout, interfaces
- [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md): milestones M0 to M11 and their "done when" criteria

If a change alters a requirement or design decision, update the doc in the same change.

## Map of the code

Imports are within this module and checked with `go list`; keep the direction.

| Path | Owns | Imports |
|---|---|---|
| root: `shard.go`, `config.go`, `querier.go`, `fanout.go`, `tx.go`, `explain.go`, `observe.go`, `migrations.go`, `idempotency.go` | The public facade: `Open`, `DB`, `Querier`, `Tx`, `Explain`, `Planner`; wires the packages below | all of them (nothing imports it except `shardtest` and the examples) |
| `registry/` | Table metadata: sharded, colocated, global | nothing |
| `router/` | Key canonicalization, hashing, 1024 buckets | nothing |
| `analyze/` | Engine-neutral `Analysis` of a statement; the `Analyzer` interface | nothing |
| `analyze/pgparse/` | The only cgo code: parses SQL, builds merge plans | `analyze`, `merge` |
| `query/` | Builder whose statements carry their own `Analysis` (no parser needed) | `analyze`, `merge` |
| `plan/` | Routing rules: `Analysis` + registry + router -> shards and strategy | `analyze`, `registry`, `router` |
| `exec/` | Pools, parallel execution, timeouts, transactions, idempotency keys | `observe`, `plan`, `router` |
| `merge/` | Combines per-shard rows (order, limit, distinct, aggregates); knows no SQL | nothing |
| `migrate/` | Per-shard migration runner, status, drift | `router` |
| `observe/`, `observe/otel/` | Hooks interface, `Slog`, OpenTelemetry | `router` / `observe` |
| `shardtest/` | Public test support: containers, fake, assertions | root, `merge`, `migrate`, `query`, `registry` |
| `examples/` | One runnable program per feature; `examples/demo` backs `docs/DEMO.md` | the public packages |

How to make the common changes (route a new construct, add a merge function,
add an error or a hook event) is in [docs/CHANGING.md](docs/CHANGING.md). Decisions
that are closed are in [docs/DECISIONS.md](docs/DECISIONS.md).

## Commands

```sh
make check             # build, vet, lint, test and test-nocgo: the gate for any change (no Docker)
make test              # unit tests with the race detector (no Docker)
make test-nocgo        # unit tests without cgo; must also pass
make vet lint          # go vet and golangci-lint
make up                # 3 local Postgres shards on ports 5441-5443 (needs Docker)
make test-integration  # integration tests, build tag `integration`
make down              # stop the shards and delete their data
make bench             # benchmarks (BENCHTIME=1x for a quick check); bench-integration needs make up
make examples          # run every example against the make up shards
make demo-setup        # shards + migrations + demo dataset (docs/DEMO.md); demo-reset starts over
make demo-check        # run docs/DEMO.md and compare its output; needs Docker
```

`make check` must pass before a change is done. It needs no Docker, so it runs
in any sandbox; unit tests, `shardtest.NewFake` and `shard.NewPlanner` cover
routing and merging without a database. Go 1.26+, PostgreSQL 14+.

**These commands delete data**, so run them only against the throwaway shards
from `make up`: `make down`, `make demo-reset`, `make demo-check`,
`make test-integration-compose` and `make bench-integration` (they wipe the
`public` schema or the `demo_` tables), and `make examples` (creates and drops
its own tables). Never point `SHARD_DSNS` at a database that matters.

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

## cgo

Only `analyze/pgparse` needs cgo (the SQL parser). Keep everything else
buildable with `CGO_ENABLED=0`: files that need the parser carry
`//go:build cgo`, and `make test-nocgo` must pass. See
[docs/BUILDING.md](docs/BUILDING.md).

## Testing

- Table-driven tests; unit tests must not need a database.
- Integration tests use the build tag `integration` and real Postgres (`make up`).
- The routing rules are pinned by a table of real SQL statements in
  `plan/route_suite_test.go`. Add a case there for every new rule or bug. When
  changing a safety rule, mutate it and check a test fails.
- Unit-test packages run under `goleak` (`leak_test.go`): a test must not leave goroutines behind. Failure-injection tests are in `failure_integration_test.go`.
- Every exported symbol has a doc comment; the linter enforces it.
- **A bug fix starts with a failing test** that reproduces it, in the table the
  bug belongs to: a case in `plan/route_suite_test.go` for a wrong route, in
  `merge/merge_test.go` (or the property test) for a wrong merge result, in
  `failure_integration_test.go` for a failure behavior. Run it, see it fail,
  then fix.
- Fixed test vectors (for example router bucket numbers) pin behavior that must never change. If one fails, fix the code, not the vector.

## Scope: what this library will not do

Do not add these without changing [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md)
section 8 first, in the same change, with the reason:

- cross-shard joins, or distributed transactions (two-phase commit);
- resharding or rebalancing of data;
- automatic retries (the caller retries; writes use idempotency keys);
- guessing a shard when a statement does not say (it is refused, never scattered).

If a request seems to need one of these, say so and propose the smallest change
inside the limits instead.

## Workflow

- Work on a branch per change; do not commit to `main` directly.
- Commit only when asked. End commit messages with the `Co-Authored-By` line the session specifies.

## When you are done

- [ ] `make check` passes.
- [ ] The change has a test that fails without it (see Testing).
- [ ] Docs match the behavior: `docs/QUERIES.md` for what is supported, `docs/ERRORS.md` for errors, `docs/REQUIREMENTS.md` / `ARCHITECTURE.md` if a design decision changed.
- [ ] A user-visible change has a line in `CHANGELOG.md` under Unreleased.
- [ ] If you changed what `examples/demo` or `docs/DEMO.md` prints: `make demo-check` passes.
- [ ] If you touched routing, merging or the executor: `make bench BENCHTIME=1x` still runs; record real changes in `docs/BENCHMARKS.md`.
- [ ] You say what you did not run (integration tests need Docker).
