//go:build cgo

package pgparse

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/g8rswimmer/go-shard/analyze"
)

// Parser implements analyze.Analyzer with PostgreSQL's parser. It is stateless
// and safe for concurrent use.
type Parser struct{}

var _ analyze.Analyzer = Parser{}

// New returns a Parser.
func New() Parser { return Parser{} }

// FromSQL parses one statement and describes it. Syntax errors are returned
// as is; constructs that parse but cannot be routed safely return an error
// wrapping analyze.ErrUnsupportedQuery that names the construct.
func (Parser) FromSQL(sql string, args []any) (analyze.Analysis, error) {
	res, err := pg.Parse(sql)
	if err != nil {
		return analyze.Analysis{}, fmt.Errorf("shard: cannot parse SQL: %w", err)
	}
	switch len(res.Stmts) {
	case 0:
		return analyze.Analysis{}, unsupported("empty statement")
	case 1:
		// exactly what we want
	default:
		return analyze.Analysis{}, unsupported("%d statements in one call; send them one at a time", len(res.Stmts))
	}

	w := &walker{args: args}
	if err := w.statement(res.Stmts[0].Stmt); err != nil {
		return analyze.Analysis{}, err
	}
	return w.a, nil
}

func unsupported(format string, args ...any) error {
	return fmt.Errorf("%w: %s", analyze.ErrUnsupportedQuery, fmt.Sprintf(format, args...))
}

// walker collects facts from one statement.
type walker struct {
	a    analyze.Analysis
	args []any
	err  error // first error found while scanning expressions
}

func (w *walker) newScope(parent int) int {
	w.a.Scopes = append(w.a.Scopes, parent)
	return len(w.a.Scopes) - 1
}

func (w *walker) note(format string, args ...any) {
	n := fmt.Sprintf(format, args...)
	for _, have := range w.a.Notes {
		if have == n {
			return
		}
	}
	w.a.Notes = append(w.a.Notes, n)
}

func (w *walker) statement(n *pg.Node) error {
	switch s := n.Node.(type) {
	case *pg.Node_SelectStmt:
		w.a.Op = analyze.OpSelect
		scope := w.newScope(-1)
		if err := w.selectStmt(s.SelectStmt, scope); err != nil {
			return err
		}
		w.selectExtras(s.SelectStmt)
		return w.err

	case *pg.Node_UpdateStmt:
		u := s.UpdateStmt
		w.a.Op = analyze.OpUpdate
		if u.WithClause != nil {
			return unsupported("WITH (common table expressions)")
		}
		scope := w.newScope(-1)
		w.a.Target = w.addTable(u.Relation, scope)
		for _, f := range u.FromClause {
			if _, err := w.fromItem(f, scope); err != nil {
				return err
			}
		}
		w.where(u.WhereClause, scope)
		for _, t := range u.TargetList {
			w.a.SetColumns = append(w.a.SetColumns, t.GetResTarget().GetName())
		}
		w.scanAll(scope, append(append([]*pg.Node{u.WhereClause}, u.TargetList...), u.ReturningList...)...)
		return w.err

	case *pg.Node_DeleteStmt:
		d := s.DeleteStmt
		w.a.Op = analyze.OpDelete
		if d.WithClause != nil {
			return unsupported("WITH (common table expressions)")
		}
		scope := w.newScope(-1)
		w.a.Target = w.addTable(d.Relation, scope)
		for _, f := range d.UsingClause {
			if _, err := w.fromItem(f, scope); err != nil {
				return err
			}
		}
		w.where(d.WhereClause, scope)
		w.scanAll(scope, append([]*pg.Node{d.WhereClause}, d.ReturningList...)...)
		return w.err

	case *pg.Node_InsertStmt:
		ins := s.InsertStmt
		w.a.Op = analyze.OpInsert
		if ins.WithClause != nil {
			return unsupported("WITH (common table expressions)")
		}
		scope := w.newScope(-1)
		w.a.Target = w.addTable(ins.Relation, scope)
		w.insert(ins)
		return w.err

	default:
		w.a.Op = analyze.OpOther
		w.a.Kind = strings.TrimPrefix(fmt.Sprintf("%T", n.Node), "*pg_query.Node_")
		w.newScope(-1)
		return nil
	}
}

// insert records the VALUES rows of an INSERT. For INSERT ... SELECT and
// INSERT ... DEFAULT VALUES there are no rows to read, and Insert stays nil.
func (w *walker) insert(ins *pg.InsertStmt) {
	sel := ins.SelectStmt.GetSelectStmt()
	if sel == nil || len(sel.ValuesLists) == 0 {
		return
	}

	hasSubquery := false
	walkNodes(ins.SelectStmt.ProtoReflect(), func(n *pg.Node) bool {
		if n.GetSubLink() != nil {
			hasSubquery = true
		}
		return !hasSubquery
	})
	if hasSubquery {
		w.fail(unsupported("a subquery inside VALUES: compute the value first, or route with WithShardKey"))
		return
	}

	info := &analyze.Insert{}
	for _, c := range ins.Cols {
		info.Columns = append(info.Columns, c.GetResTarget().GetName())
	}
	for _, vl := range sel.ValuesLists {
		var row []analyze.Cell
		for _, item := range vl.GetList().GetItems() {
			v, ok, err := w.constant(unwrapCast(item))
			if err != nil {
				w.fail(err)
				return
			}
			row = append(row, analyze.Cell{Value: v, Known: ok})
		}
		info.Rows = append(info.Rows, row)
	}
	if oc := ins.OnConflictClause; oc != nil && oc.Action == pg.OnConflictAction_ONCONFLICT_UPDATE {
		for _, t := range oc.TargetList {
			info.ConflictSet = append(info.ConflictSet, t.GetResTarget().GetName())
		}
	}
	w.a.Insert = info
}

// SplitRows returns the INSERT with only the given VALUES rows, in the order
// given, and the arguments it still uses. Parameters are renumbered from $1
// with no gaps, in the order the statement uses them, so the result is the
// same every time it is asked for.
func (Parser) SplitRows(sql string, args []any, rows []int) (string, []any, error) {
	res, err := pg.Parse(sql)
	if err != nil {
		return "", nil, fmt.Errorf("shard: cannot parse SQL: %w", err)
	}
	if len(res.Stmts) != 1 {
		return "", nil, unsupported("cannot split %d statements", len(res.Stmts))
	}
	sel := res.Stmts[0].Stmt.GetInsertStmt().GetSelectStmt().GetSelectStmt()
	if sel == nil || len(sel.ValuesLists) == 0 {
		return "", nil, unsupported("only INSERT ... VALUES can be split across shards")
	}
	if len(rows) == 0 {
		return "", nil, unsupported("no rows to keep")
	}

	kept := make([]*pg.Node, len(rows))
	for i, r := range rows {
		if r < 0 || r >= len(sel.ValuesLists) {
			return "", nil, fmt.Errorf("shard: row %d is out of range (the statement has %d rows)", r, len(sel.ValuesLists))
		}
		kept[i] = sel.ValuesLists[r]
	}
	sel.ValuesLists = kept

	renumber := map[int32]int32{}
	var newArgs []any
	var walkErr error
	walkNodes(res.Stmts[0].Stmt.ProtoReflect(), func(n *pg.Node) bool {
		p := n.GetParamRef()
		if p == nil || walkErr != nil {
			return walkErr == nil
		}
		to, seen := renumber[p.Number]
		if !seen {
			if p.Number < 1 || int(p.Number) > len(args) {
				walkErr = fmt.Errorf("%w: the SQL uses $%d but %d argument(s) were given", analyze.ErrMissingArgument, p.Number, len(args))
				return false
			}
			newArgs = append(newArgs, args[p.Number-1])
			to = int32(len(newArgs))
			renumber[p.Number] = to
		}
		p.Number = to
		return true
	})
	if walkErr != nil {
		return "", nil, walkErr
	}

	out, err := pg.Deparse(res)
	if err != nil {
		return "", nil, fmt.Errorf("shard: cannot rebuild the INSERT: %w", err)
	}
	return out, newArgs, nil
}

var _ analyze.RowSplitter = Parser{}

// selectStmt records the tables and predicates of a SELECT (the statement
// itself or a subquery) at the given scope.
func (w *walker) selectStmt(s *pg.SelectStmt, scope int) error {
	switch {
	case s.WithClause != nil:
		return unsupported("WITH (common table expressions)")
	case s.Op != pg.SetOperation_SETOP_NONE:
		return unsupported("UNION, INTERSECT and EXCEPT")
	case len(s.ValuesLists) > 0:
		return unsupported("VALUES list")
	case s.IntoClause != nil:
		return unsupported("SELECT INTO")
	default:
		// a plain SELECT
	}

	for _, f := range s.FromClause {
		if _, err := w.fromItem(f, scope); err != nil {
			return err
		}
	}
	w.where(s.WhereClause, scope)

	nodes := []*pg.Node{s.WhereClause, s.HavingClause, s.LimitCount, s.LimitOffset}
	nodes = append(nodes, s.TargetList...)
	nodes = append(nodes, s.GroupClause...)
	nodes = append(nodes, s.SortClause...)
	nodes = append(nodes, s.WindowClause...)
	w.scanAll(scope, nodes...)
	return w.err
}

func (w *walker) where(n *pg.Node, scope int) {
	if n != nil {
		w.conjuncts(n, scope, true)
	}
}

// addTable records a table use.
func (w *walker) addTable(rv *pg.RangeVar, scope int) int {
	id := len(w.a.Tables)
	w.a.Tables = append(w.a.Tables, analyze.TableRef{
		ID:     id,
		Scope:  scope,
		Schema: rv.Schemaname,
		Name:   rv.Relname,
		Alias:  rv.GetAlias().GetAliasname(),
	})
	return id
}

// fromItem records the tables in one FROM item and returns their IDs.
func (w *walker) fromItem(n *pg.Node, scope int) ([]int, error) {
	switch x := n.Node.(type) {
	case *pg.Node_RangeVar:
		return []int{w.addTable(x.RangeVar, scope)}, nil

	case *pg.Node_JoinExpr:
		j := x.JoinExpr
		if j.IsNatural {
			return nil, unsupported("NATURAL JOIN: name the join columns with ON or USING")
		}
		left, err := w.fromItem(j.Larg, scope)
		if err != nil {
			return nil, err
		}
		right, err := w.fromItem(j.Rarg, scope)
		if err != nil {
			return nil, err
		}
		if j.Quals != nil {
			// Constants in the ON clause of an outer join do not filter the
			// preserved side, so they must not be used to pick a shard.
			w.conjuncts(j.Quals, scope, j.Jointype == pg.JoinType_JOIN_INNER)
			w.scanAll(scope, j.Quals)
		}
		if len(j.UsingClause) > 0 {
			u := analyze.Using{Left: left, Right: right}
			for _, c := range j.UsingClause {
				u.Columns = append(u.Columns, c.GetString_().GetSval())
			}
			w.a.Usings = append(w.a.Usings, u)
		}
		return append(left, right...), nil

	case *pg.Node_RangeSubselect:
		return nil, unsupported("subquery in FROM: move it into WHERE, or route with WithShardKey")
	case *pg.Node_RangeFunction:
		return nil, unsupported("function call in FROM: route with WithShardKey")
	default:
		return nil, unsupported("%T in FROM", n.Node)
	}
}

// conjuncts walks the AND-ed conditions of a predicate. Conditions inside OR or
// NOT cannot be relied on to restrict the result, so they are only noted.
func (w *walker) conjuncts(n *pg.Node, scope int, allowConsts bool) {
	n = unwrapCast(n)
	switch x := n.Node.(type) {
	case *pg.Node_BoolExpr:
		switch x.BoolExpr.Boolop {
		case pg.BoolExprType_AND_EXPR:
			for _, arg := range x.BoolExpr.Args {
				w.conjuncts(arg, scope, allowConsts)
			}
		case pg.BoolExprType_OR_EXPR:
			w.note("an OR expression")
		case pg.BoolExprType_NOT_EXPR:
			w.note("a NOT expression")
		default:
			w.note("a boolean expression")
		}
	case *pg.Node_AExpr:
		w.predicate(x.AExpr, scope, allowConsts)
	case *pg.Node_SubLink:
		w.note("a subquery condition")
	case *pg.Node_NullTest:
		w.note("an IS NULL / IS NOT NULL test")
	default:
		w.note("a condition the router cannot read (%s)", nodeName(n))
	}
}

func (w *walker) predicate(e *pg.A_Expr, scope int, allowConsts bool) {
	op := opName(e)
	switch e.Kind {
	case pg.A_Expr_Kind_AEXPR_OP:
		if op == "=" {
			w.equals(e.Lexpr, e.Rexpr, scope, allowConsts)
			return
		}
		w.note("a %q comparison", op)

	case pg.A_Expr_Kind_AEXPR_IN:
		if op != "=" {
			w.note("a NOT IN list")
			return
		}
		w.inList(e, scope, allowConsts)

	case pg.A_Expr_Kind_AEXPR_OP_ANY:
		if op != "=" {
			w.note("a %q ANY comparison", op)
			return
		}
		w.anyEq(e, scope, allowConsts)

	default:
		w.note("a %s condition", strings.ToLower(strings.TrimPrefix(e.Kind.String(), "AEXPR_")))
	}
}

func (w *walker) equals(l, r *pg.Node, scope int, allowConsts bool) {
	l, r = unwrapCast(l), unwrapCast(r)
	lc, lok := column(l, scope)
	rc, rok := column(r, scope)
	switch {
	case lok && rok:
		w.a.Equalities = append(w.a.Equalities, analyze.Equality{Left: lc, Right: rc})
	case lok:
		w.bindConst(lc, r, allowConsts)
	case rok:
		w.bindConst(rc, l, allowConsts)
	default:
		w.note("a comparison between two expressions")
	}
}

func (w *walker) bindConst(col analyze.ColumnRef, value *pg.Node, allowConsts bool) {
	v, ok, err := w.constant(value)
	switch {
	case err != nil:
		w.fail(err)
	case !ok:
		w.note("a comparison of %s with an expression or NULL", col.Name)
	case allowConsts:
		w.a.Bindings = append(w.a.Bindings, analyze.Binding{Column: col, Values: []any{v}})
	default:
		w.note("a value in the ON clause of an outer join")
	}
}

func (w *walker) inList(e *pg.A_Expr, scope int, allowConsts bool) {
	col, ok := column(unwrapCast(e.Lexpr), scope)
	list := e.Rexpr.GetList()
	if !ok || list == nil {
		w.note("an IN list that is not on a plain column")
		return
	}
	var values []any
	for _, item := range list.Items {
		v, ok, err := w.constant(unwrapCast(item))
		switch {
		case err != nil:
			w.fail(err)
			return
		case !ok:
			w.note("an IN list with non-constant values")
			return
		default:
			values = append(values, v)
		}
	}
	if !allowConsts {
		w.note("a value in the ON clause of an outer join")
		return
	}
	w.a.Bindings = append(w.a.Bindings, analyze.Binding{Column: col, Values: values})
}

// anyEq handles col = ANY($1) where $1 is a slice, and col = ANY(ARRAY[...]).
func (w *walker) anyEq(e *pg.A_Expr, scope int, allowConsts bool) {
	col, ok := column(unwrapCast(e.Lexpr), scope)
	if !ok {
		w.note("an ANY comparison that is not on a plain column")
		return
	}
	rhs := unwrapCast(e.Rexpr)

	var values []any
	switch x := rhs.Node.(type) {
	case *pg.Node_ParamRef:
		arg, err := w.param(x.ParamRef)
		if err != nil {
			w.fail(err)
			return
		}
		rv := reflect.ValueOf(arg)
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			w.note("= ANY(%s) where the argument is not a slice", "$"+strconv.Itoa(int(x.ParamRef.Number)))
			return
		}
		values = make([]any, rv.Len())
		for i := range values {
			values[i] = rv.Index(i).Interface()
		}
	case *pg.Node_AArrayExpr:
		for _, el := range x.AArrayExpr.Elements {
			v, ok, err := w.constant(unwrapCast(el))
			switch {
			case err != nil:
				w.fail(err)
				return
			case !ok:
				w.note("an ANY(ARRAY[...]) with non-constant values")
				return
			default:
				values = append(values, v)
			}
		}
	default:
		w.note("an ANY comparison the router cannot read")
		return
	}
	if !allowConsts {
		w.note("a value in the ON clause of an outer join")
		return
	}
	w.a.Bindings = append(w.a.Bindings, analyze.Binding{Column: col, Values: values})
}

// constant resolves an expression to a value when it is a literal or a
// parameter. ok is false for anything else, including NULL.
func (w *walker) constant(n *pg.Node) (v any, ok bool, err error) {
	switch x := n.Node.(type) {
	case *pg.Node_AConst:
		return aconst(x.AConst)
	case *pg.Node_ParamRef:
		arg, err := w.param(x.ParamRef)
		return arg, err == nil, err
	case *pg.Node_AExpr:
		// A negative number that the parser did not fold into the literal.
		e := x.AExpr
		if e.Kind == pg.A_Expr_Kind_AEXPR_OP && e.Lexpr == nil && opName(e) == "-" {
			inner, ok, err := w.constant(unwrapCast(e.Rexpr))
			if err != nil || !ok {
				return nil, false, err
			}
			if n, isInt := toInt64(inner); isInt {
				return -n, true, nil
			}
		}
		return nil, false, nil
	default:
		return nil, false, nil
	}
}

func (w *walker) param(p *pg.ParamRef) (any, error) {
	i := int(p.Number)
	if i < 1 || i > len(w.args) {
		return nil, fmt.Errorf("%w: the SQL uses $%d but %d argument(s) were given", analyze.ErrMissingArgument, i, len(w.args))
	}
	return w.args[i-1], nil
}

func aconst(c *pg.A_Const) (any, bool, error) {
	if c.Isnull {
		return nil, false, nil
	}
	switch v := c.Val.(type) {
	case *pg.A_Const_Ival:
		return int64(v.Ival.Ival), true, nil
	case *pg.A_Const_Fval:
		// The parser stores integers that do not fit in 32 bits as text.
		if n, err := strconv.ParseInt(v.Fval.Fval, 10, 64); err == nil {
			return n, true, nil
		}
		f, err := strconv.ParseFloat(v.Fval.Fval, 64)
		return f, err == nil, nil
	case *pg.A_Const_Sval:
		return v.Sval.Sval, true, nil
	case *pg.A_Const_Boolval:
		return v.Boolval.Boolval, true, nil
	default:
		return nil, false, nil
	}
}

func (w *walker) fail(err error) {
	if w.err == nil {
		w.err = err
	}
}

// scanAll finds subqueries anywhere inside the given expressions and analyzes
// each at its own scope below the current one.
func (w *walker) scanAll(scope int, nodes ...*pg.Node) {
	for _, n := range nodes {
		if n == nil || w.err != nil {
			continue
		}
		walkNodes(n.ProtoReflect(), func(x *pg.Node) bool {
			sl := x.GetSubLink()
			if sl == nil {
				return true
			}
			ss := sl.GetSubselect().GetSelectStmt()
			if ss == nil {
				w.fail(unsupported("a subquery the router cannot read"))
				return false
			}
			child := w.newScope(scope)
			if err := w.selectStmt(ss, child); err != nil {
				w.fail(err)
			}
			if sl.Testexpr != nil {
				w.scanAll(scope, sl.Testexpr)
			}
			return false
		})
	}
}

// walkNodes calls fn for every pg.Node inside m, depth first, fields in
// declaration order. If fn returns false the node's children are skipped. The
// order is fixed, not left to protobuf's Range, because callers number things
// in visiting order and that must be the same on every run.
func walkNodes(m protoreflect.Message, fn func(*pg.Node) bool) {
	if node, ok := m.Interface().(*pg.Node); ok && !fn(node) {
		return
	}
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Message() == nil || fd.IsMap() || !m.Has(fd) {
			continue
		}
		v := m.Get(fd)
		switch {
		case fd.IsList():
			l := v.List()
			for j := 0; j < l.Len(); j++ {
				walkNodes(l.Get(j).Message(), fn)
			}
		default:
			walkNodes(v.Message(), fn)
		}
	}
}

// selectExtras records what merging results across shards will need.
func (w *walker) selectExtras(s *pg.SelectStmt) {
	a := &w.a
	a.Distinct = len(s.DistinctClause) > 0
	a.HasHaving = s.HavingClause != nil

	for _, item := range s.SortClause {
		sb := item.GetSortBy()
		if sb == nil {
			continue
		}
		t := analyze.OrderTerm{Desc: sb.SortbyDir == pg.SortByDir_SORTBY_DESC}
		switch sb.SortbyNulls {
		case pg.SortByNulls_SORTBY_NULLS_FIRST:
			t.Nulls = "first"
		case pg.SortByNulls_SORTBY_NULLS_LAST:
			t.Nulls = "last"
		default:
			// database default
		}
		switch {
		case sb.Node.GetColumnRef() != nil:
			t.Column = columnText(sb.Node.GetColumnRef())
		case sb.Node.GetAConst().GetIval() != nil:
			t.Ordinal = int(sb.Node.GetAConst().GetIval().Ival)
		default:
			t.Complex = true
		}
		if sb.SortbyDir == pg.SortByDir_SORTBY_USING {
			t.Complex = true
		}
		a.OrderBy = append(a.OrderBy, t)
	}

	a.Limit = w.intValue(s.LimitCount)
	a.Offset = w.intValue(s.LimitOffset)

	for _, g := range s.GroupClause {
		switch {
		case g.GetColumnRef() != nil:
			a.GroupBy = append(a.GroupBy, columnText(g.GetColumnRef()))
		case g.GetAConst().GetIval() != nil:
			a.GroupBy = append(a.GroupBy, "#"+strconv.Itoa(int(g.GetAConst().GetIval().Ival)))
		default:
			a.GroupBy = append(a.GroupBy, "(expression)")
		}
	}

	for _, t := range s.TargetList {
		val := t.GetResTarget().GetVal()
		if fc := val.GetFuncCall(); fc != nil && fc.Over == nil && aggregateFuncs[funcName(fc)] {
			a.Aggregates = append(a.Aggregates, analyze.AggregateTerm{Func: funcName(fc), Distinct: fc.AggDistinct, Star: fc.AggStar})
		}
	}
	for _, n := range append(append([]*pg.Node{}, s.TargetList...), s.SortClause...) {
		walkNodes(n.ProtoReflect(), func(x *pg.Node) bool {
			if fc := x.GetFuncCall(); fc != nil && fc.Over != nil {
				a.HasWindow = true
			}
			return true
		})
	}
}

// intValue resolves LIMIT / OFFSET to a number when it is a literal or a
// parameter holding an integer.
func (w *walker) intValue(n *pg.Node) *int64 {
	if n == nil {
		return nil
	}
	v, ok, err := w.constant(unwrapCast(n))
	if err != nil || !ok {
		return nil
	}
	if i, ok := toInt64(v); ok {
		return &i
	}
	return nil
}

var aggregateFuncs = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
	"array_agg": true, "string_agg": true, "bool_and": true, "bool_or": true, "every": true,
	"json_agg": true, "jsonb_agg": true, "json_object_agg": true, "jsonb_object_agg": true,
	"stddev": true, "stddev_pop": true, "stddev_samp": true,
	"variance": true, "var_pop": true, "var_samp": true,
	"bit_and": true, "bit_or": true,
}

func funcName(fc *pg.FuncCall) string {
	if len(fc.Funcname) == 0 {
		return ""
	}
	return strings.ToLower(fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval())
}

// unwrapCast strips casts such as $1::bigint or '5'::int.
func unwrapCast(n *pg.Node) *pg.Node {
	for n != nil {
		tc := n.GetTypeCast()
		if tc == nil {
			return n
		}
		n = tc.Arg
	}
	return n
}

// column reads a plain column reference. Anything with a star or a subscript
// is not a plain column.
func column(n *pg.Node, scope int) (analyze.ColumnRef, bool) {
	cr := n.GetColumnRef()
	if cr == nil || len(cr.Fields) == 0 {
		return analyze.ColumnRef{}, false
	}
	parts := make([]string, len(cr.Fields))
	for i, f := range cr.Fields {
		s := f.GetString_()
		if s == nil {
			return analyze.ColumnRef{}, false
		}
		parts[i] = s.Sval
	}
	return analyze.ColumnRef{
		Scope:     scope,
		Qualifier: strings.Join(parts[:len(parts)-1], "."),
		Name:      parts[len(parts)-1],
	}, true
}

func columnText(cr *pg.ColumnRef) string {
	parts := make([]string, 0, len(cr.Fields))
	for _, f := range cr.Fields {
		switch {
		case f.GetString_() != nil:
			parts = append(parts, f.GetString_().Sval)
		case f.GetAStar() != nil:
			parts = append(parts, "*")
		default:
			parts = append(parts, "?")
		}
	}
	return strings.Join(parts, ".")
}

func opName(e *pg.A_Expr) string {
	if len(e.Name) == 0 {
		return ""
	}
	return e.Name[len(e.Name)-1].GetString_().GetSval()
}

func nodeName(n *pg.Node) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", n.Node), "*pg_query.Node_")
}

func toInt64(v any) (int64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if u := rv.Uint(); u <= 1<<63-1 {
			return int64(u), true
		}
		return 0, false
	default:
		return 0, false
	}
}
