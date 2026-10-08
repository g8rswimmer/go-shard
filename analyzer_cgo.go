//go:build cgo

package shard

import (
	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/analyze/pgparse"
)

// defaultAnalyzer reads raw SQL with PostgreSQL's own parser.
func defaultAnalyzer() analyze.Analyzer { return pgparse.New() }
