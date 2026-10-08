# Building with go-shard

go-shard reads raw SQL with PostgreSQL's own parser
([pg_query_go](https://github.com/pganalyze/pg_query_go)), which is C code
compiled through **cgo**. That is the only part of the library that needs it:
the package `analyze/pgparse`.

What that means for you:

| Your build | Works? | Notes |
|---|---|---|
| `CGO_ENABLED=1` with a C compiler (the default on most machines) | Yes | Everything, including routing raw SQL |
| `CGO_ENABLED=0` | Yes, with a limit | The library builds and runs, but raw SQL is not routed automatically: `db.Query(ctx, "SELECT ...")` returns `ErrShardKeyRequired`. Route explicitly (`WithShardKey`, `WithShard`, `WithAllShards`), use statements from package `query`, or set `Config.Analyzer` to your own `analyze.Analyzer` |

Statements built with package [`query`](../query) never need the parser, so a
program that only uses the builder and explicit routing can build without cgo.
That includes reading from every shard: a built SELECT is merged (ordering,
limit, offset) without the parser. Raw SQL that runs on several shards, and any
`GROUP BY` or aggregate, needs the parser (or an analyzer that also implements
`merge.Planner`, below).

## Requirements

- A C compiler on the build machine (`gcc` or `clang`). On macOS this comes with
  the Xcode command line tools; on Debian/Ubuntu install `build-essential`.
- The first build compiles PostgreSQL's parser and takes noticeably longer than
  a pure Go build (tens of seconds on a recent laptop). Go's build cache makes
  later builds fast, so keep it in CI (`actions/setup-go` does this by default).

## Linux builds

Build on Linux, either on a Linux machine or in a container. This was tested
with the official images:

```sh
# Dynamic, glibc
docker run --rm -v "$PWD":/src -w /src golang:1.26 go build -o app ./cmd/app

# Fully static, musl (for scratch or distroless images)
docker run --rm -v "$PWD":/src -w /src golang:1.26-alpine sh -c '
  apk add --no-cache gcc musl-dev &&
  CGO_ENABLED=1 go build -ldflags "-linkmode external -extldflags \"-static\"" -o app ./cmd/app'
```

The static build produced a working static executable (about 24 MB for the
quickstart example).

## Cross-compiling

`GOOS=linux go build` on a Mac does **not** work with the default compiler: cgo
needs a C compiler that targets Linux, and the macOS one cannot. Your options:

1. **Build in a Linux container or on a Linux CI runner** (above). This is the
   simplest and the approach tested here.
2. Use a cross C toolchain, for example `zig cc` as `CC`
   (`CC="zig cc -target x86_64-linux-musl" CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build`).
   This is commonly used with cgo projects but has not been tested with go-shard.
3. Build without cgo and supply your own analyzer, as above.

## Using your own analyzer

Anything that implements `analyze.Analyzer` can replace the parser:

```go
type Analyzer interface {
    FromSQL(sql string, args []any) (analyze.Analysis, error)
}
```

It reports the tables, the conditions on columns and the joins in a statement
(see package `analyze`); go-shard decides what that means for shard selection.
Set it with `Config.Analyzer`.

Merging the rows of a query that runs on several shards also needs the SQL
rewritten (extra columns for the sort, `AVG` split into a sum and a count).
The parser does that through `merge.Planner`; an analyzer that does not
implement it can route raw SQL, but a raw query that spans shards is refused
with a message saying so.
