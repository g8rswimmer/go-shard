// Package merge combines per-shard results into one: ordering, LIMIT and
// OFFSET, DISTINCT, aggregates and GROUP BY.
//
// A query that runs on several shards is rewritten first (by a Planner, which
// needs to read the SQL) into the statement each shard runs and a Spec that
// says how to put the answers back together. Merge then does the putting
// together, without knowing anything about SQL text.
//
// Rows are streamed wherever the Spec allows it. An ordered merge, with or
// without LIMIT, holds one row per shard however large the result is.
// Aggregation, GROUP BY and DISTINCT must remember what they have seen, so they
// are bounded by Options.MaxRows and fail with ErrLimitExceeded instead of
// exhausting memory.
//
// Values are compared by their Go type: numbers numerically, text byte by byte,
// times by instant. Text is therefore ordered as in the "C" collation. If your
// database uses another collation, ORDER BY on a text column can order
// differently from PostgreSQL itself; see docs/ARCHITECTURE.md.
package merge
