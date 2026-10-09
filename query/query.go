// Package query builds SELECT, UPDATE and DELETE statements that already know
// how they route.
//
// A built Statement carries its SQL, its arguments and an analyze.Analysis, so
// running it needs no SQL parsing (and no cgo). The builder quotes every
// identifier, so user-supplied column names cannot change the statement.
//
//	st, err := query.From("profiles").
//	    Columns("id", "name").
//	    Where(query.Eq("id", 42)).
//	    Build()
//	rows, err := db.QueryStatement(ctx, st)
//
// Identifiers are quoted exactly as given. PostgreSQL stores unquoted names in
// lower case, so use lower-case names, as in the registry.
//
// Only AND is supported between conditions. For OR and anything else, write
// SQL and pass it to Query or Exec.
package query

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/merge"
)

// Dir is a sort direction.
type Dir int

const (
	Asc Dir = iota
	Desc
)

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// Statement is a built statement.
type Statement struct {
	sql      string
	args     []any
	analysis analyze.Analysis
	split    func(rows []int) (string, []any)
	merge    *merge.Plan // set for a SELECT
}

// SQL returns the statement text, with $1, $2, ... placeholders.
func (s Statement) SQL() string { return s.sql }

// Args returns the values for the placeholders.
func (s Statement) Args() []any { return s.args }

// MergePlan returns the statement each shard runs when this SELECT runs on
// several shards, and how to merge the results. It is how a built SELECT
// fans out without parsing SQL.
func (s Statement) MergePlan() (merge.Plan, error) {
	if s.merge == nil {
		return merge.Plan{}, fmt.Errorf("%w: only a SELECT can return rows from several shards", analyze.ErrUnsupportedQuery)
	}
	return *s.merge, nil
}

// Analysis describes the statement for routing.
func (s Statement) Analysis() analyze.Analysis { return s.analysis }

// SplitRows returns an INSERT with only the given rows (indexes into the rows
// added with Row, in the order wanted) and the arguments it needs. It is how a
// multi-row INSERT is sent to several shards, each receiving its own rows. It
// fails for any statement that is not an INSERT.
func (s Statement) SplitRows(rows []int) (string, []any, error) {
	if s.split == nil {
		return "", nil, errors.New("query: only an INSERT can be split into rows")
	}
	for _, r := range rows {
		if r < 0 || r >= len(s.analysis.Insert.Rows) {
			return "", nil, fmt.Errorf("query: row %d is out of range (the INSERT has %d rows)", r, len(s.analysis.Insert.Rows))
		}
	}
	if len(rows) == 0 {
		return "", nil, errors.New("query: no rows to keep")
	}
	sql, args := s.split(rows)
	return sql, args, nil
}

// Cond is a condition in a WHERE clause. Build one with Eq, In and the others.
type Cond struct {
	col  string
	op   string // "=", "IN", "<>", "<", "<=", ">", ">=", "LIKE", "IS NULL", "IS NOT NULL"
	vals []any
}

// Eq is col = value. On a shard key column it routes to one shard.
func Eq(col string, value any) Cond { return Cond{col, "=", []any{value}} }

// In is col IN (values...). On a shard key column it routes to the shards the
// values hash to.
func In(col string, values ...any) Cond { return Cond{col, "IN", values} }

// Ne is col <> value.
func Ne(col string, value any) Cond { return Cond{col, "<>", []any{value}} }

// Lt is col < value.
func Lt(col string, value any) Cond { return Cond{col, "<", []any{value}} }

// Le is col <= value.
func Le(col string, value any) Cond { return Cond{col, "<=", []any{value}} }

// Gt is col > value.
func Gt(col string, value any) Cond { return Cond{col, ">", []any{value}} }

// Ge is col >= value.
func Ge(col string, value any) Cond { return Cond{col, ">=", []any{value}} }

// Like is col LIKE pattern.
func Like(col string, pattern string) Cond { return Cond{col, "LIKE", []any{pattern}} }

// IsNull is col IS NULL.
func IsNull(col string) Cond { return Cond{col, "IS NULL", nil} }

// NotNull is col IS NOT NULL.
func NotNull(col string) Cond { return Cond{col, "IS NOT NULL", nil} }

// stmt accumulates SQL, arguments and analysis for one statement.
type stmt struct {
	errs     []error
	table    string
	schema   string
	name     string
	where    []Cond
	args     []any
	analysis analyze.Analysis
}

// clone returns a copy to render into, so Build can be called more than once.
func (s *stmt) clone() *stmt {
	c := *s
	c.errs = slices.Clone(s.errs)
	return &c
}

func (s *stmt) fail(format string, args ...any) { s.errs = append(s.errs, fmt.Errorf(format, args...)) }

func (s *stmt) setTable(op analyze.OpKind, table string) {
	s.analysis = analyze.Analysis{Op: op, Scopes: []int{-1}}
	parts := strings.Split(table, ".")
	switch len(parts) {
	case 1:
		s.name = parts[0]
	case 2:
		s.schema, s.name = parts[0], parts[1]
	default:
		s.fail("table %q: use name or schema.name", table)
		return
	}
	for _, p := range parts {
		if !identRE.MatchString(p) {
			s.fail("table %q is not a valid identifier", table)
			return
		}
	}
	s.table = quote(s.name)
	if s.schema != "" {
		s.table = quote(s.schema) + "." + s.table
	}
	s.analysis.Tables = []analyze.TableRef{{ID: 0, Scope: 0, Schema: s.schema, Name: s.name}}
}

func quote(ident string) string { return `"` + ident + `"` }

func (s *stmt) ident(col string) string {
	if !identRE.MatchString(col) {
		s.fail("column %q is not a valid identifier", col)
	}
	return quote(col)
}

// param appends an argument and returns its placeholder.
func (s *stmt) param(v any) string {
	s.args = append(s.args, v)
	return fmt.Sprintf("$%d", len(s.args))
}

// whereSQL renders the conditions and records what they mean for routing.
func (s *stmt) whereSQL() string {
	if len(s.where) == 0 {
		return ""
	}
	parts := make([]string, len(s.where))
	for i, c := range s.where {
		col := s.ident(c.col)
		ref := analyze.ColumnRef{Scope: 0, Name: c.col}
		switch c.op {
		case "=":
			parts[i] = col + " = " + s.param(c.vals[0])
			s.analysis.Bindings = append(s.analysis.Bindings, analyze.Binding{Column: ref, Values: []any{c.vals[0]}})
		case "IN":
			if len(c.vals) == 0 {
				s.fail("In(%q) needs at least one value", c.col)
				continue
			}
			ph := make([]string, len(c.vals))
			for j, v := range c.vals {
				ph[j] = s.param(v)
			}
			parts[i] = col + " IN (" + strings.Join(ph, ", ") + ")"
			s.analysis.Bindings = append(s.analysis.Bindings, analyze.Binding{Column: ref, Values: append([]any(nil), c.vals...)})
		case "IS NULL", "IS NOT NULL":
			parts[i] = col + " " + c.op
			s.analysis.Notes = append(s.analysis.Notes, "an IS NULL / IS NOT NULL test")
		default:
			parts[i] = col + " " + c.op + " " + s.param(c.vals[0])
			s.analysis.Notes = append(s.analysis.Notes, fmt.Sprintf("a %q comparison", c.op))
		}
	}
	return " WHERE " + strings.Join(parts, " AND ")
}

func (s *stmt) finish(sql string) (Statement, error) {
	if err := errors.Join(s.errs...); err != nil {
		return Statement{}, fmt.Errorf("query: %w", err)
	}
	return Statement{sql: sql, args: s.args, analysis: s.analysis}, nil
}

// ---- SELECT ----------------------------------------------------------------

// SelectBuilder builds a SELECT.
type SelectBuilder struct {
	stmt
	cols          []string
	order         []orderItem
	limit, offset *int64
}

type orderItem struct {
	col string
	dir Dir
}

// From starts a SELECT from a table, given as name or schema.name.
func From(table string) *SelectBuilder {
	b := &SelectBuilder{}
	b.setTable(analyze.OpSelect, table)
	return b
}

// Columns sets the columns to return. The default is all columns (*).
func (b *SelectBuilder) Columns(cols ...string) *SelectBuilder { b.cols = cols; return b }

// Where adds conditions, combined with AND.
func (b *SelectBuilder) Where(conds ...Cond) *SelectBuilder {
	b.where = append(b.where, conds...)
	return b
}

// OrderBy adds a sort key. Call it again for more keys.
func (b *SelectBuilder) OrderBy(col string, dir Dir) *SelectBuilder {
	b.order = append(b.order, orderItem{col, dir})
	return b
}

// Limit caps the number of rows.
func (b *SelectBuilder) Limit(n int64) *SelectBuilder { b.limit = &n; return b }

// Offset skips rows.
func (b *SelectBuilder) Offset(n int64) *SelectBuilder { b.offset = &n; return b }

// Build returns the statement, or an error naming every invalid input.
func (b *SelectBuilder) Build() (Statement, error) {
	s := b.clone()
	cols := "*"
	if len(b.cols) > 0 {
		q := make([]string, len(b.cols))
		for i, c := range b.cols {
			q[i] = s.ident(c)
		}
		cols = strings.Join(q, ", ")
	}
	from := " FROM " + s.table + s.whereSQL()
	sql := "SELECT " + cols + from

	// The statement each shard runs when this one fans out: order columns that
	// are not selected are added as hidden columns, and OFFSET is left to the
	// merge (the shards' LIMIT covers the rows it skips).
	var (
		hidden  []string
		order   []merge.Order
		pending []int // star: positions of hidden order terms, named from the end
	)
	if len(b.order) > 0 {
		items := make([]string, len(b.order))
		for i, o := range b.order {
			items[i] = s.ident(o.col)
			t := analyze.OrderTerm{Column: o.col}
			if o.dir == Desc {
				items[i] += " DESC"
				t.Desc = true
			}
			s.analysis.OrderBy = append(s.analysis.OrderBy, t)

			mo := merge.Order{Desc: o.dir == Desc, NullsFirst: o.dir == Desc, Label: o.col}
			switch at := slices.Index(b.cols, o.col); {
			case at >= 0:
				mo.Col = at
			case len(b.cols) > 0:
				mo.Col = len(b.cols) + len(hidden)
				hidden = append(hidden, s.ident(o.col))
			default:
				pending = append(pending, i)
				hidden = append(hidden, s.ident(o.col))
			}
			order = append(order, mo)
		}
		sql += " ORDER BY " + strings.Join(items, ", ")
		from += " ORDER BY " + strings.Join(items, ", ")
	}
	for k, i := range pending {
		order[i].Col = k - len(pending) // -1 is the last hidden column
	}

	var offset int64
	if b.limit != nil {
		if *b.limit < 0 {
			s.fail("Limit cannot be negative")
		}
		sql += fmt.Sprintf(" LIMIT %d", *b.limit)
		s.analysis.Limit = b.limit
	}
	if b.offset != nil {
		if *b.offset < 0 {
			s.fail("Offset cannot be negative")
		}
		sql += fmt.Sprintf(" OFFSET %d", *b.offset)
		s.analysis.Offset = b.offset
		offset = *b.offset
	}
	if b.limit != nil {
		from += fmt.Sprintf(" LIMIT %d", offset+*b.limit)
	}

	shardCols := cols
	if len(hidden) > 0 {
		shardCols += ", " + strings.Join(hidden, ", ")
	}
	st, err := s.finish(sql)
	if err != nil {
		return st, err
	}
	st.merge = &merge.Plan{
		SQL:  "SELECT " + shardCols + from,
		Args: st.args,
		Spec: merge.Spec{Hidden: len(hidden), Order: order, Offset: offset, Limit: b.limit},
	}
	return st, nil
}

// ---- UPDATE ----------------------------------------------------------------

// UpdateBuilder builds an UPDATE.
type UpdateBuilder struct {
	stmt
	sets []assignment
}

type assignment struct {
	col string
	val any
}

// Update starts an UPDATE of a table, given as name or schema.name.
func Update(table string) *UpdateBuilder {
	b := &UpdateBuilder{}
	b.setTable(analyze.OpUpdate, table)
	return b
}

// Set assigns a value to a column.
func (b *UpdateBuilder) Set(col string, value any) *UpdateBuilder {
	b.sets = append(b.sets, assignment{col, value})
	return b
}

// Where adds conditions, combined with AND.
func (b *UpdateBuilder) Where(conds ...Cond) *UpdateBuilder {
	b.where = append(b.where, conds...)
	return b
}

// Build returns the statement, or an error naming every invalid input.
func (b *UpdateBuilder) Build() (Statement, error) {
	s := b.clone()
	if len(b.sets) == 0 {
		s.fail("Update needs at least one Set")
	}
	assigns := make([]string, len(b.sets))
	for i, a := range b.sets {
		assigns[i] = s.ident(a.col) + " = " + s.param(a.val)
		s.analysis.SetColumns = append(s.analysis.SetColumns, a.col)
	}
	return s.finish("UPDATE " + s.table + " SET " + strings.Join(assigns, ", ") + s.whereSQL())
}

// ---- DELETE ----------------------------------------------------------------

// DeleteBuilder builds a DELETE.
type DeleteBuilder struct{ stmt }

// DeleteFrom starts a DELETE from a table, given as name or schema.name.
func DeleteFrom(table string) *DeleteBuilder {
	b := &DeleteBuilder{}
	b.setTable(analyze.OpDelete, table)
	return b
}

// Where adds conditions, combined with AND.
func (b *DeleteBuilder) Where(conds ...Cond) *DeleteBuilder {
	b.where = append(b.where, conds...)
	return b
}

// Build returns the statement, or an error naming every invalid input.
func (b *DeleteBuilder) Build() (Statement, error) {
	s := b.clone()
	return s.finish("DELETE FROM " + s.table + s.whereSQL())
}

// ---- INSERT ----------------------------------------------------------------

// InsertBuilder builds an INSERT ... VALUES.
type InsertBuilder struct {
	stmt
	cols      []string
	rows      [][]any
	onConf    conflictKind
	confTo    []string
	confSet   []string
	returning []string
}

type conflictKind int

const (
	noConflict conflictKind = iota
	conflictDoNothing
	conflictDoUpdate
)

// InsertInto starts an INSERT into a table, given as name or schema.name.
func InsertInto(table string) *InsertBuilder {
	b := &InsertBuilder{}
	b.setTable(analyze.OpInsert, table)
	return b
}

// Columns names the columns the rows give values for. It is required: the shard
// key column must be among them, so the rows can be routed.
func (b *InsertBuilder) Columns(cols ...string) *InsertBuilder { b.cols = cols; return b }

// Row adds one row of values, in the order of Columns. Call it once per row; rows
// whose keys belong to different shards are sent to their own shards.
func (b *InsertBuilder) Row(values ...any) *InsertBuilder {
	b.rows = append(b.rows, values)
	return b
}

// OnConflictDoNothing adds ON CONFLICT DO NOTHING.
func (b *InsertBuilder) OnConflictDoNothing() *InsertBuilder {
	b.onConf = conflictDoNothing
	return b
}

// OnConflictUpdate adds ON CONFLICT (target...) DO UPDATE SET col = EXCLUDED.col
// for each column in set. Setting the shard key column is refused when the
// statement is routed.
func (b *InsertBuilder) OnConflictUpdate(target []string, set ...string) *InsertBuilder {
	b.onConf, b.confTo, b.confSet = conflictDoUpdate, target, set
	return b
}

// Returning adds RETURNING columns. Run such a statement with QueryStatement.
func (b *InsertBuilder) Returning(cols ...string) *InsertBuilder { b.returning = cols; return b }

// Build returns the statement, or an error naming every invalid input.
func (b *InsertBuilder) Build() (Statement, error) {
	s := b.clone()
	if len(b.cols) == 0 {
		s.fail("Insert needs Columns, so the shard key column can be found")
	}
	if len(b.rows) == 0 {
		s.fail("Insert needs at least one Row")
	}

	cols := make([]string, len(b.cols))
	for i, c := range b.cols {
		cols[i] = s.ident(c)
	}
	for i, r := range b.rows {
		if len(r) != len(b.cols) {
			s.fail("row %d has %d values for %d columns", i, len(r), len(b.cols))
		}
	}

	var tail string
	switch b.onConf {
	case conflictDoNothing:
		tail = " ON CONFLICT DO NOTHING"
	case conflictDoUpdate:
		if len(b.confTo) == 0 || len(b.confSet) == 0 {
			s.fail("OnConflictUpdate needs a conflict target and at least one column to set")
		}
		target := make([]string, len(b.confTo))
		for i, c := range b.confTo {
			target[i] = s.ident(c)
		}
		set := make([]string, len(b.confSet))
		for i, c := range b.confSet {
			set[i] = s.ident(c) + " = EXCLUDED." + s.ident(c)
		}
		tail = " ON CONFLICT (" + strings.Join(target, ", ") + ") DO UPDATE SET " + strings.Join(set, ", ")
		s.analysis.Insert = &analyze.Insert{ConflictSet: slices.Clone(b.confSet)}
	default:
		// plain INSERT
	}
	if len(b.returning) > 0 {
		r := make([]string, len(b.returning))
		for i, c := range b.returning {
			r[i] = s.ident(c)
		}
		tail += " RETURNING " + strings.Join(r, ", ")
	}
	if err := errors.Join(s.errs...); err != nil {
		return Statement{}, fmt.Errorf("query: %w", err)
	}

	// Copy what the Statement keeps, so changing the builder later cannot change it.
	rows := make([][]any, len(b.rows))
	for i, r := range b.rows {
		rows[i] = slices.Clone(r)
	}
	head := "INSERT INTO " + s.table + " (" + strings.Join(cols, ", ") + ") VALUES "
	render := func(idx []int) (string, []any) {
		var args []any
		groups := make([]string, len(idx))
		for g, i := range idx {
			ph := make([]string, len(rows[i]))
			for j, v := range rows[i] {
				args = append(args, v)
				ph[j] = fmt.Sprintf("$%d", len(args))
			}
			groups[g] = "(" + strings.Join(ph, ", ") + ")"
		}
		return head + strings.Join(groups, ", ") + tail, args
	}
	all := make([]int, len(rows))
	for i := range all {
		all[i] = i
	}
	sql, args := render(all)

	in := &analyze.Insert{Columns: slices.Clone(b.cols)}
	if s.analysis.Insert != nil {
		in.ConflictSet = s.analysis.Insert.ConflictSet
	}
	for _, r := range rows {
		cells := make([]analyze.Cell, len(r))
		for j, v := range r {
			cells[j] = analyze.Cell{Value: v, Known: true}
		}
		in.Rows = append(in.Rows, cells)
	}
	s.analysis.Insert = in

	return Statement{sql: sql, args: args, analysis: s.analysis, split: render}, nil
}
