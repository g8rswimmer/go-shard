package merge

import "errors"

// ErrLimitExceeded is returned when merging would hold more than
// Options.MaxRows rows (or groups) in memory.
var ErrLimitExceeded = errors.New("shard: merge limit exceeded")

// Func says how a column is combined across shards.
type Func int

const (
	// Pass keeps the value as is: a plain column, or a group key. For a group,
	// the first shard's value is kept (all shards agree, by the rules of SQL).
	Pass Func = iota
	// Count adds the per-shard counts.
	Count
	// Sum adds the per-shard sums. A shard with no rows sums to NULL, which is
	// ignored.
	Sum
	// Min and Max pick the smallest or largest value, ignoring NULL.
	Min
	Max
	// Avg is a per-shard SUM that becomes sum / count at the end. Column.Count
	// is the column that holds the per-shard COUNT.
	Avg
)

func (f Func) String() string {
	switch f {
	case Pass:
		return "pass"
	case Count:
		return "count"
	case Sum:
		return "sum"
	case Min:
		return "min"
	case Max:
		return "max"
	case Avg:
		return "avg"
	default:
		return "Func(?)"
	}
}

// Column describes one column of the per-shard result.
type Column struct {
	Func Func
	// Key marks a GROUP BY column: rows with equal keys form one group.
	Key bool
	// Count is, for Avg, the index of the column holding the per-shard count.
	Count int
	// Label is the column as the statement wrote it, such as count(*). It is
	// only used to describe the merge (Spec.Steps) and may be empty.
	Label string
}

// Order is one ORDER BY key.
type Order struct {
	// Col is the index of the column to sort by. A negative index counts from
	// the end (-1 is the last column), which is how a hidden column is named
	// when the statement selects * and the position of the others is unknown.
	Col  int
	Desc bool
	// NullsFirst says where NULL sorts. PostgreSQL's default is last for
	// ascending order and first for descending, and the planner has already
	// applied it.
	NullsFirst bool
	// Label is the sort term as the statement wrote it. It is only used to
	// describe the merge (Spec.Steps) and may be empty.
	Label string
}

// Cond is a HAVING condition, evaluated on merged groups.
type Cond struct {
	// Op is "and", "or", "not", or a comparison: "=", "<>", "<", "<=", ">", ">=".
	Op string
	// Args are the operands of and / or / not.
	Args []Cond
	// Col and Value are the operands of a comparison: column Col of the merged
	// row against Value. Col is already combined (an Avg column is the average).
	Col   int
	Value any
}

// Spec says how to combine the results of one statement across shards.
type Spec struct {
	// Columns describe the per-shard result, hidden columns included. It may be
	// nil when the statement selects *, so the number of columns is not known
	// in advance; every column is then Pass (no Aggregate).
	Columns []Column
	// Hidden is how many trailing columns exist only to merge and are dropped
	// from the result.
	Hidden int
	// Aggregate combines rows with equal Key columns (all rows into one group
	// if no column is a Key) before anything else.
	Aggregate bool
	// Having filters merged groups.
	Having *Cond
	// Distinct drops repeated rows from the result.
	Distinct bool
	// Order sorts the result. In streaming mode every shard's rows must already
	// be sorted this way.
	Order []Order
	// Offset and Limit are applied to the merged result. Limit nil means no limit.
	Offset int64
	Limit  *int64
}

// Plan is a statement rewritten to run on each shard, and how to merge the
// results.
type Plan struct {
	SQL  string
	Args []any
	Spec Spec
}

// Planner rewrites a SELECT for a fan-out. It is implemented by the SQL
// analyzer in analyze/pgparse and by statements built with package query.
type Planner interface {
	// PlanMerge rewrites one SELECT. It returns an error wrapping
	// analyze.ErrUnsupportedQuery for constructs that cannot be merged.
	PlanMerge(sql string, args []any) (Plan, error)
}

// Options configure Merge.
type Options struct {
	// MaxRows bounds the rows, or groups, held in memory. Zero means no bound.
	MaxRows int
}
