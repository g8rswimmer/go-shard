## What and why

<!-- What changes, and what problem it solves. Link the issue. -->

## Checklist

- [ ] `make check` passes (needs no Docker).
- [ ] A test fails without this change (a bug fix starts with the failing test; routing cases go in `plan/route_suite_test.go`).
- [ ] If a safety rule changed, I mutated it and a test failed.
- [ ] Docs match: `docs/QUERIES.md` (what is supported), `docs/ERRORS.md` (errors), `docs/REQUIREMENTS.md` / `ARCHITECTURE.md` (design decisions).
- [ ] User-visible change: line added to `CHANGELOG.md` under Unreleased.
- [ ] Changed `examples/demo` or `docs/DEMO.md`: `make demo-check` passes.
- [ ] Touched routing, merging or the executor: `make bench BENCHTIME=1x` runs; real changes recorded in `docs/BENCHMARKS.md`.
- [ ] Not run (say so): integration tests, PostgreSQL versions, other.

<!-- See AGENTS.md and docs/CHANGING.md. Out of scope without a requirements change: cross-shard joins, distributed transactions, resharding, retries. -->
