# Upgrading

Notes for moving between releases. Each release that breaks something has a
section here saying what changed and what to do. The rules for what may break,
and when, are in [VERSIONING.md](VERSIONING.md).

## v0.1.0

The first release; there is nothing to upgrade from.

If you were following `main` before the release, these changes during
development may affect you:

- **`shard.Querier` has three more methods** (`Explain`, `ExplainStatement`,
  `Explainer`). A fake or wrapper that implements `Querier` itself must add
  them; embedding `*shard.DB` or using `shardtest.NewFake` does it for you.
- **Explain options are not context values.** `shard.WithShardPlans(ctx)` and
  `shard.WithAnalyze(ctx)` do not exist; use
  `db.Explainer(shard.ShardPlans())` and `db.Explainer(shard.Analyze())`, then
  call `Explain` on the result.
- **`Begin` is not on `Querier`.** It is on `shard.TxBeginner` (which `*DB`
  implements), so a `Tx` cannot start another.
- **`Config.ShardTimeout` for queries** now counts from when the shard starts
  answering and again from when the rows are ready to read, rather than from
  the moment the query started. Queries that used to fail with
  `context.DeadlineExceeded` while rows were being read after a slow shard
  answered no longer do.
- **`migrate` is forward-only.** `.down.sql` files are ignored.

## Template for later releases

> ### vX.Y.Z
>
> **Breaking:** what changed, and the smallest edit that makes your code work
> again, with a before and after.
>
> **Behaviour:** changes that do not break the build but could surprise.
>
> **Persistent data:** anything that touches what is stored on the shards (this
> should be rare; see "What never changes silently" in VERSIONING.md).
