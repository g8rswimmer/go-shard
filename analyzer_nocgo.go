//go:build !cgo

package shard

import "github.com/g8rswimmer/go-shard/analyze"

// defaultAnalyzer is nil without cgo, because the SQL parser needs it. Supply
// Config.Analyzer, build statements with package query, or route explicitly
// with WithShardKey, WithShard and WithAllShards. See docs/BUILDING.md.
func defaultAnalyzer() analyze.Analyzer { return nil }
