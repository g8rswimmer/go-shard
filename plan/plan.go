// Package plan describes which shards a statement runs on and why.
//
// A Plan is a plain value: it is built without touching a database, so routing
// decisions can be unit tested and shown by Explain.
package plan

import (
	"fmt"

	"github.com/g8rswimmer/go-shard/router"
)

// Strategy says how many shards a plan touches.
type Strategy int

const (
	// Single targets exactly one shard.
	Single Strategy = iota + 1
	// Multi targets some, but not all, shards.
	Multi
	// All targets every shard.
	All
)

func (s Strategy) String() string {
	switch s {
	case Single:
		return "single"
	case Multi:
		return "multi"
	case All:
		return "all"
	default:
		return fmt.Sprintf("Strategy(%d)", int(s))
	}
}

// Plan is a routed statement.
type Plan struct {
	// SQL and Args are what each target shard runs.
	SQL  string
	Args []any
	// Targets are the shards to run on, in a stable order.
	Targets  []router.ShardID
	Strategy Strategy
	// Reason says why these targets were chosen, for Explain and logs.
	Reason string
}
