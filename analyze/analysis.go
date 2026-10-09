// Package analyze defines the engine-neutral description of a SQL statement
// that routing works from.
//
// Two things produce an Analysis: the query builder (package query), which
// knows exactly what it built, and a parser (package analyze/pgparse), which
// reads raw SQL. Routing (package plan) only ever sees an Analysis, so it does
// not depend on how it was made.
//
// An Analysis records facts, not decisions. It says "the column id of table
// profiles is compared with 42", and the router decides what that means for
// shard selection using the registry.
package analyze

import "errors"

var (
	// ErrUnsupportedQuery is returned for SQL the library cannot route safely.
	// The message names the construct and what to do instead.
	ErrUnsupportedQuery = errors.New("shard: unsupported query")

	// ErrMissingArgument is returned when SQL refers to a parameter such as $3
	// that has no argument.
	ErrMissingArgument = errors.New("shard: missing argument")
)

// OpKind is the kind of statement.
type OpKind int

const (
	// OpSelect is a SELECT.
	OpSelect OpKind = iota + 1
	// OpInsert is an INSERT.
	OpInsert
	// OpUpdate is an UPDATE.
	OpUpdate
	// OpDelete is a DELETE.
	OpDelete
	// OpOther is any statement that cannot be routed from its predicates, such
	// as DDL. Analysis.Kind names it.
	OpOther
)

// String returns the SQL keyword: SELECT, INSERT, UPDATE, DELETE, or OTHER.
func (o OpKind) String() string {
	switch o {
	case OpSelect:
		return "SELECT"
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	case OpOther:
		return "other"
	default:
		return "OpKind(?)"
	}
}

// IsWrite reports whether the statement changes data.
func (o OpKind) IsWrite() bool { return o == OpInsert || o == OpUpdate || o == OpDelete }

// TableRef is one use of a table in a statement. A table that appears twice
// (a self join) is two TableRefs.
type TableRef struct {
	// ID is the index of this entry in Analysis.Tables.
	ID int
	// Scope is the query level the table appears in; see Analysis.Scopes.
	Scope int
	// Schema is the schema as written, or "" if none was given.
	Schema string
	// Name is the table name.
	Name string
	// Alias is the alias as written, or "" if none.
	Alias string
}

// ColumnRef is a column as written in the statement.
type ColumnRef struct {
	// Scope is the query level the reference appears in.
	Scope int
	// Qualifier is the table name or alias as written ("p" in p.id, or
	// "public.profiles" in public.profiles.id). Empty if unqualified.
	Qualifier string
	Name      string
}

// Equality is a condition left = right between two columns, found where it
// restricts the result: in WHERE or in a JOIN ... ON, combined with AND.
type Equality struct{ Left, Right ColumnRef }

// Binding is a condition that restricts a column to known values: col = 5,
// col IN (1, 2), col = ANY($1). Parameters are already replaced by the
// arguments they refer to.
type Binding struct {
	Column ColumnRef
	Values []any
}

// Using is a JOIN ... USING (columns) between two groups of tables.
type Using struct {
	// Left and Right are the IDs of the tables on each side of the join.
	Left, Right []int
	Columns     []string
}

// OrderTerm is one ORDER BY item.
type OrderTerm struct {
	// Column is the column as written, e.g. "name" or "p.name". Empty for an
	// ordinal or an expression.
	Column string
	// Ordinal is the 1-based select-list position for ORDER BY 2; 0 otherwise.
	Ordinal int
	// Complex is true when the item is an expression rather than a column or ordinal.
	Complex bool
	Desc    bool
	// Nulls is "first", "last", or "" for the default.
	Nulls string
}

// AggregateTerm is an aggregate function call in the select list.
type AggregateTerm struct {
	Func     string // lower case: count, sum, ...
	Distinct bool
	Star     bool // count(*)
}

// Cell is one value of an INSERT ... VALUES row.
type Cell struct {
	// Value is the literal, or the argument a parameter refers to.
	Value any
	// Known is false when the cell is not a literal or a parameter: a function
	// call, an expression, DEFAULT, a subquery. Such a value cannot be hashed
	// before the statement runs.
	Known bool
}

// Insert describes the rows of an INSERT ... VALUES.
type Insert struct {
	// Columns are the column names given in the statement. Empty when the
	// statement does not name them.
	Columns []string
	// Rows are the VALUES rows, in order.
	Rows [][]Cell
	// ConflictSet lists the columns an ON CONFLICT DO UPDATE assigns.
	ConflictSet []string
}

// Analysis describes one statement.
type Analysis struct {
	Op OpKind
	// Kind names the statement when Op is OpOther, e.g. "CreateStmt".
	Kind string

	// Scopes[i] is the parent of query level i, or -1 for the statement
	// itself. Level 0 is the statement; each subquery adds a level.
	Scopes []int

	Tables []TableRef
	// Target is the ID of the table an INSERT, UPDATE or DELETE writes to.
	// Meaningful only when Op.IsWrite().
	Target int

	// Insert describes the VALUES rows of an INSERT. It is nil for any other
	// statement, and for INSERT ... SELECT and INSERT ... DEFAULT VALUES, whose
	// rows are not known until they run.
	Insert *Insert
	// SetColumns lists the columns an UPDATE assigns.
	SetColumns []string

	Equalities []Equality
	Bindings   []Binding
	Usings     []Using

	// Notes explain predicates that could not be used for routing (an OR, a
	// range comparison, a function call). They are shown when routing fails.
	Notes []string

	// The fields below are for merging results across shards; routing does not
	// use them. They describe the outermost SELECT.
	OrderBy       []OrderTerm
	Limit, Offset *int64
	GroupBy       []string
	Aggregates    []AggregateTerm
	Distinct      bool
	HasWindow     bool
	HasHaving     bool
}

// RowSplitter is implemented by analyzers that can restrict an INSERT to some
// of its VALUES rows. It is needed to send a multi-row INSERT to several shards,
// each receiving only its own rows.
type RowSplitter interface {
	// SplitRows returns the statement with only the given rows (indexes into
	// Insert.Rows, in the order wanted), and the arguments it now needs. The
	// parameters are renumbered so that they start at $1 with no gaps.
	SplitRows(sql string, args []any, rows []int) (string, []any, error)
}

// Analyzer reads SQL and describes it. Implementations must be safe for
// concurrent use.
type Analyzer interface {
	// FromSQL analyzes one statement. Parameters ($1, $2, ...) are resolved
	// against args.
	FromSQL(sql string, args []any) (Analysis, error)
}
