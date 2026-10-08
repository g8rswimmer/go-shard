// Package pgparse analyzes raw SQL using PostgreSQL's own parser
// (github.com/pganalyze/pg_query_go).
//
// It is the only package in go-shard that needs cgo. Without cgo this package
// is empty and applications supply their own analyze.Analyzer, or route every
// statement explicitly with WithShardKey, WithShard or WithAllShards. See
// docs/BUILDING.md.
package pgparse
