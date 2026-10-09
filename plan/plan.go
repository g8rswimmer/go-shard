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

// String returns "single", "multi" or "all".
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

// ShardStatement is the statement one shard runs when shards run different
// statements.
type ShardStatement struct {
	SQL  string
	Args []any
}

// Plan is a routed statement.
type Plan struct {
	// SQL and Args are what each target shard runs, unless PerShard says
	// otherwise.
	SQL  string
	Args []any
	// Targets are the shards to run on, in a stable order.
	Targets  []router.ShardID
	Strategy Strategy
	// Reason says why these targets were chosen, for Explain and logs.
	Reason string

	// GlobalWrite is true for a write to a global table, which goes to every
	// shard because every shard holds a copy.
	GlobalWrite bool

	// Rows is set for an INSERT ... VALUES. It lists, for each target shard,
	// the indexes of the VALUES rows that belong to it.
	Rows map[router.ShardID][]int
	// PerShard is set when shards must run different statements: a multi-row
	// INSERT whose rows belong to different shards. Shards not listed run SQL.
	PerShard map[router.ShardID]ShardStatement
}

// StatementFor returns the SQL and arguments a shard runs.
func (p Plan) StatementFor(id router.ShardID) (string, []any) {
	if s, ok := p.PerShard[id]; ok {
		return s.SQL, s.Args
	}
	return p.SQL, p.Args
}
