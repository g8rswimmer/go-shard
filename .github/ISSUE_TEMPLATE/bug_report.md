---
name: Bug report
about: A statement went to the wrong shard, a result is wrong, or something failed unexpectedly
labels: bug
---

<!-- Never paste a DSN, password or real data. Use the demo or made-up tables. -->

**What you ran**

The SQL (or the `query` builder code) and its arguments, and which route you used (`db.Query`, `WithShardKey`, `WithAllShards`, ...):

```sql
```

**The registry**

How the tables involved are declared (`registry.Sharded`, `Colocated`, `Global`), with key and key type:

```go
```

**What you expected**

Which shard(s) it should run on, or what the result should be.

**What happened**

The error (full text), or the wrong result. `db.Explain(ctx, sql, args...)` output helps a lot:

```text
```

**Setup**

- go-shard version:
- Go version, and `CGO_ENABLED` (raw SQL routing needs cgo):
- PostgreSQL version:
- Number of shards:

**Smallest reproduction**

If you can, a case for `plan/route_suite_test.go` (routing) or a test with `shardtest.NewFake` (no database needed). See [docs/CHANGING.md](../../docs/CHANGING.md).
