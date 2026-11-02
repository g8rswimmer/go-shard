# Making common changes

For each kind of change: where it goes, what else must change with it, and the
test that proves it. Read [ARCHITECTURE.md](ARCHITECTURE.md) for the why, and run
`make check` at the end. Closed decisions are in [DECISIONS.md](DECISIONS.md).

## Fix a statement that routes to the wrong shard (or is not refused)

1. Add a case to `plan/route_suite_test.go` with the real SQL and the expected
   result. Run `go test ./plan -run Suite` and watch it fail.
2. Fix the rule in `plan/route.go`. If the cause is how SQL is read (a construct
   the analyzer misses), fix `analyze/pgparse/parser.go` and add a case to
   `analyze/pgparse/parser_test.go`.
3. If you changed a safety rule (what is refused), mutate it back and check that a
   test fails.
4. If the statement is a builder statement, `plan/builder_equiv_test.go` checks
   the builder and the raw SQL route the same: extend it.
5. Update [QUERIES.md](QUERIES.md) if what is supported or refused changed.

## Support a new SQL construct in a fan-out query

1. Decide whether it can be reproduced exactly from per-shard results. If not,
   refuse it with `ErrUnsupportedQuery` naming the construct (do not approximate).
2. The rewrite is in `analyze/pgparse/merge.go` (it builds a `merge.Plan`: the
   SQL each shard runs, and a `merge.Spec`). The combining is in `merge/merge.go`.
   `merge` must not know SQL.
3. Tests: `analyze/pgparse/merge_test.go` (the rewrite), `merge/merge_test.go`
   (the combining), `merge/describe_test.go` (the `Explain` text), and the
   property test `fanout_property_integration_test.go` (same answer as one
   database; needs Docker).
4. [QUERIES.md](QUERIES.md) lists what is supported: add it.

## Add a merge function (a new aggregate)

Add it to `merge.Func` in `merge/spec.go` (and its `String`), implement it in
`merge/merge.go`, describe it in `merge/describe.go`, map the SQL name in
`analyze/pgparse/merge.go`, then add the tests above. Integer and float paths
are performance-sensitive: run `make bench BENCHTIME=1x`.

## Add or change an error

1. Define it where its package owns it. Sentinel errors that callers branch on
   are re-exported in `errors.go`. Wrap with `%w`; the message says what
   happened and what to do. Never put a DSN or statement arguments in it.
2. Add it to [ERRORS.md](ERRORS.md): when it happens, what to do.
3. Test it with `errors.Is` / `errors.As`.

## Add an observability event or field

`observe.Hooks` is an interface that users implement: adding a method breaks
them (embedding `observe.Nop` protects only those who did), so it is a breaking
change; prefer adding a field to an event struct. Then update the call sites
(`exec/hooks.go`, `observe.go`), `observe/slog.go`, `observe/otel/otel.go`, the
package doc in `observe/doc.go`, and the tests in `observe_test.go` and
`observe/`. Events never carry statement arguments.

## Change the public API

Check [VERSIONING.md](VERSIONING.md) for what counts as breaking. Update
godoc (every exported symbol needs it), `CHANGELOG.md`, and
[UPGRADING.md](UPGRADING.md) if users must change their code.

## Change what the demo prints

`docs/DEMO.md` is run by CI: the output blocks must match. Change
`examples/demo`, then run `make demo-check` and copy the new output into the
guide. Keep output the same on every run (no times, no map order, no goroutine
order).

## Add an example

A directory under `examples/` with `main.go` and a README showing its output;
add it to `EXAMPLES` in the `Makefile`, to `examples/README.md`, and to the
table in `examples/run_integration_test.go` with the key lines it must print.
Examples touch only tables with an `ex_` prefix and clean up after themselves.
