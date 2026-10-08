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
}

// SQL returns the statement text, with $1, $2, ... placeholders.
func (s Statement) SQL() string { return s.sql }

// Args returns the values for the placeholders.
func (s Statement) Args() []any { return s.args }

// Analysis describes the statement for routing.
func (s Statement) Analysis() analyze.Analysis { return s.analysis }

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
	sql := "SELECT " + cols + " FROM " + s.table + s.whereSQL()

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
		}
		sql += " ORDER BY " + strings.Join(items, ", ")
	}
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
	}
	return s.finish(sql)
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
