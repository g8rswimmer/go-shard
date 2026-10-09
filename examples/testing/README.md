# Testing code that uses go-shard

Two ways to test a repository written against `shard.Querier`, side by side:

- **`profiles_test.go`, a unit test with `shardtest.NewFake`.** No Docker, no
  PostgreSQL; it runs in milliseconds with `go test ./examples/testing`.
  The fake routes every statement with the library's real rules (registry,
  hash, SQL parser), records where it would have gone, and answers with the
  rows you stub. You assert on routing, which is what breaks when a query
  changes:

  ```go
  fake := shardtest.NewFake(t, reg, "shard-01", "shard-02", "shard-03")
  fake.StubRows("FROM profiles", []string{"id", "name", "country"}, []any{int64(42), "Ada", "UK"})

  profile, err := repo.Get(ctx, 42)

  fake.AssertSingleShard("FROM profiles")           // one shard, not a fan-out
  fake.AssertFanout("GROUP BY country", shards...)  // every shard
  fake.AssertRoutes("UPDATE profiles", "shard-02")  // exactly these shards
  plan := fake.LastPlan()                           // the same Explain a real DB returns
  ```

  A statement the library would refuse (no shard key, a cross-shard join) is
  refused by the fake with the same error, so a mistake shows up in a unit
  test. `StubError` makes a statement fail after it has been routed, to test
  error handling.

- **`profiles_integration_test.go`, the same repository against real shards**
  with `shardtest.NewCluster` (build tag `integration`; needs Docker, or set
  `SHARDTEST_DSNS` to databases you already run).

The fake does not run SQL, keep data, or merge: a query returns the rows you
stubbed, whatever the shards. It does not fake transactions; test those against
a cluster. Parsing raw SQL needs cgo; without it, build statements with package
`query` (see `docs/BUILDING.md`).

```sh
go test -v ./examples/testing                      # the unit test, no Docker
go test -tags integration ./examples/testing       # needs Docker
```
