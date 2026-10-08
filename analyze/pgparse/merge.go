//go:build cgo

package pgparse

import (
	"fmt"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/g8rswimmer/go-shard/merge"
)

var _ merge.Planner = Parser{}

// mergeFuncs are the aggregates whose per-shard results can be combined.
var mergeFuncs = map[string]merge.Func{
	"count": merge.Count, "sum": merge.Sum, "min": merge.Min, "max": merge.Max, "avg": merge.Avg,
}

// PlanMerge rewrites a SELECT that runs on several shards, and says how to put
// the answers together:
//
//   - ORDER BY keeps its place in the shard statement, so each shard returns
//     sorted rows; order terms that are not in the select list are added as
//     hidden columns.
//   - OFFSET is removed and LIMIT becomes offset+limit on each shard; the
//     merge applies the real ones.
//   - With aggregates or GROUP BY, the shard statement groups as written, AVG
//     becomes SUM plus a hidden COUNT, the group keys are selected, and
//     HAVING, ORDER BY, OFFSET and LIMIT are applied after the merge.
//
// Constructs whose results cannot be combined return an error wrapping
// analyze.ErrUnsupportedQuery that names the construct.
func (Parser) PlanMerge(sql string, args []any) (merge.Plan, error) {
	res, err := pg.Parse(sql)
	if err != nil {
		return merge.Plan{}, fmt.Errorf("shard: cannot parse SQL: %w", err)
	}
	if len(res.Stmts) != 1 {
		return merge.Plan{}, unsupported("%d statements in one call; send them one at a time", len(res.Stmts))
	}
	sel := res.Stmts[0].Stmt.GetSelectStmt()
	if sel == nil {
		return merge.Plan{}, unsupported("only a SELECT can return rows from several shards")
	}

	b := &mergeBuilder{sel: sel, w: &walker{args: args}}
	spec, err := b.build()
	if err != nil {
		return merge.Plan{}, err
	}

	newArgs, err := renumberParams(res.Stmts[0].Stmt, args)
	if err != nil {
		return merge.Plan{}, err
	}
	out, err := pg.Deparse(res)
	if err != nil {
		return merge.Plan{}, fmt.Errorf("shard: cannot rebuild the SELECT: %w", err)
	}
	return merge.Plan{SQL: out, Args: newArgs, Spec: spec}, nil
}

// mergeBuilder rewrites one SELECT in place.
type mergeBuilder struct {
	sel *pg.SelectStmt
	w   *walker // resolves LIMIT and HAVING constants against the arguments

	aggregate bool
	star      bool           // the select list has * or t.*
	explicit  int            // targets written in the statement
	cols      []merge.Column // aggregate mode: one per target, hidden ones too
	hidden    int
}

// pendingOrder is an order term waiting for the final hidden count.
type pendingOrder struct {
	index  int
	hidden int // the k-th hidden column, or -1 when index is final
}

func (b *mergeBuilder) build() (merge.Spec, error) {
	sel := b.sel
	switch {
	case sel.WithClause != nil:
		return merge.Spec{}, unsupported("WITH (common table expressions) on several shards")
	case sel.Op != pg.SetOperation_SETOP_NONE:
		return merge.Spec{}, unsupported("UNION, INTERSECT and EXCEPT on several shards")
	case len(sel.LockingClause) > 0:
		return merge.Spec{}, unsupported("FOR UPDATE / FOR SHARE on several shards; lock rows through a single-shard transaction")
	case sel.LimitOption == pg.LimitOption_LIMIT_OPTION_WITH_TIES:
		return merge.Spec{}, unsupported("FETCH FIRST ... WITH TIES on several shards")
	case len(sel.WindowClause) > 0:
		return merge.Spec{}, unsupported("window functions on several shards")
	default:
		// a plain SELECT
	}
	for _, d := range sel.DistinctClause {
		if d.Node != nil {
			return merge.Spec{}, unsupported("DISTINCT ON on several shards")
		}
	}
	if err := rejectNested(sel); err != nil {
		return merge.Spec{}, err
	}

	limit, err := b.count(sel.LimitCount, "LIMIT")
	if err != nil {
		return merge.Spec{}, err
	}
	offset, err := b.count(sel.LimitOffset, "OFFSET")
	if err != nil {
		return merge.Spec{}, err
	}

	b.explicit = len(sel.TargetList)
	b.aggregate = len(sel.GroupClause) > 0 || sel.HavingClause != nil
	for _, t := range sel.TargetList {
		val := t.GetResTarget().GetVal()
		switch {
		case isStar(val):
			b.star = true
		case containsAggregate(val):
			b.aggregate = true
		default:
			// a plain column or expression
		}
	}
	for _, s := range sel.SortClause {
		if containsAggregate(s.GetSortBy().GetNode()) {
			b.aggregate = true
		}
	}

	spec := merge.Spec{Distinct: len(sel.DistinctClause) > 0}
	switch {
	case b.aggregate:
		err = b.planAggregate(&spec)
	default:
		err = b.planRows(&spec)
	}
	if err != nil {
		return merge.Spec{}, err
	}

	spec.Hidden = b.hidden
	if spec.Distinct && b.hidden > 0 {
		return merge.Spec{}, unsupported("SELECT DISTINCT with an ORDER BY or HAVING term that is not in the select list; list the columns you sort by")
	}

	// What the shards run: ordering as written (rows mode), no OFFSET, and the
	// LIMIT widened to cover the rows the merge will skip.
	sel.LimitOffset = nil
	sel.LimitCount = nil
	sel.LimitOption = pg.LimitOption_LIMIT_OPTION_DEFAULT
	spec.Offset = *offset
	spec.Limit = limit
	if !b.aggregate && limit != nil {
		sel.LimitCount = pg.MakeAConstIntNode(*offset+*limit, -1)
		sel.LimitOption = pg.LimitOption_LIMIT_OPTION_COUNT
	}
	if b.aggregate {
		sel.SortClause = nil
		sel.HavingClause = nil
	}
	return spec, nil
}

// planRows handles a SELECT without aggregates: shards sort, the merge merges.
func (b *mergeBuilder) planRows(spec *merge.Spec) error {
	order, err := b.orderTerms()
	if err != nil {
		return err
	}
	spec.Order = b.finishOrder(order)
	return nil
}

// planAggregate handles aggregates and GROUP BY: every shard returns its
// groups, and the merge combines them.
func (b *mergeBuilder) planAggregate(spec *merge.Spec) error {
	sel := b.sel
	if b.star {
		return unsupported("SELECT * with aggregates or GROUP BY on several shards; list the columns")
	}
	spec.Aggregate = true

	// Columns the user selected.
	var avgs []int
	for i, t := range sel.TargetList {
		rt := t.GetResTarget()
		var name string
		if fc := rt.Val.GetFuncCall(); fc != nil {
			name = funcName(fc)
		}
		col, isAvg, err := b.classify(rt.Val)
		if err != nil {
			return err
		}
		b.cols = append(b.cols, col)
		if isAvg {
			avgs = append(avgs, i)
			if rt.Name == "" {
				rt.Name = name // the column is still called avg, though the shard computes a sum
			}
		}
	}
	for _, i := range avgs {
		b.cols[i].Count = b.hiddenCount(sel.TargetList[i].GetResTarget().Val)
	}

	// Group keys.
	for _, g := range sel.GroupClause {
		idx, err := b.groupKey(g)
		if err != nil {
			return err
		}
		b.cols[idx].Key = true
	}

	// HAVING, evaluated after the merge.
	if sel.HavingClause != nil {
		c, err := b.having(sel.HavingClause)
		if err != nil {
			return err
		}
		spec.Having = &c
	}

	order, err := b.orderTerms()
	if err != nil {
		return err
	}
	spec.Order = b.finishOrder(order)
	spec.Columns = b.cols
	return nil
}

// classify says how a select-list expression is combined. For AVG it rewrites
// the call to SUM in place and reports isAvg so the caller can add the COUNT.
func (b *mergeBuilder) classify(val *pg.Node) (col merge.Column, isAvg bool, err error) {
	fc := val.GetFuncCall()
	if fc != nil && fc.Over == nil && isAggregateName(funcName(fc)) {
		f, ok := mergeFuncs[funcName(fc)]
		switch {
		case !ok:
			return col, false, unsupported("%s() on several shards: its per-shard results cannot be combined", funcName(fc))
		case fc.AggDistinct:
			return col, false, unsupported("%s(DISTINCT ...) on several shards: a value can occur on more than one shard", funcName(fc))
		case fc.AggFilter != nil:
			return col, false, unsupported("%s(...) FILTER on several shards", funcName(fc))
		case len(fc.AggOrder) > 0:
			return col, false, unsupported("ORDER BY inside %s() on several shards", funcName(fc))
		default:
			// a combinable aggregate
		}
		if f == merge.Avg {
			fc.Funcname = []*pg.Node{pg.MakeStrNode("sum")}
		}
		return merge.Column{Func: f}, f == merge.Avg, nil
	}
	if containsAggregate(val) {
		return col, false, unsupported("an expression around an aggregate (such as sum(x) / count(*)) on several shards; select the aggregates and combine them in your code")
	}
	return merge.Column{Func: merge.Pass}, false, nil
}

// hiddenCount appends the COUNT that pairs with a rewritten AVG, and returns
// its column index. avg is the (already rewritten) call.
func (b *mergeBuilder) hiddenCount(avg *pg.Node) int {
	c := proto.Clone(avg).(*pg.Node)
	c.GetFuncCall().Funcname = []*pg.Node{pg.MakeStrNode("count")}
	b.sel.TargetList = append(b.sel.TargetList, pg.MakeResTargetNodeWithVal(c, -1))
	b.cols = append(b.cols, merge.Column{Func: merge.Count})
	b.hidden++
	return len(b.cols) - 1
}

// addHidden appends a column that exists only to merge, and returns its index.
// In aggregate mode it is classified like a select-list item.
func (b *mergeBuilder) addHidden(expr *pg.Node) (int, error) {
	expr = proto.Clone(expr).(*pg.Node)
	b.sel.TargetList = append(b.sel.TargetList, pg.MakeResTargetNodeWithVal(expr, -1))
	b.hidden++
	if !b.aggregate {
		return b.explicit + b.hidden - 1, nil
	}
	col, isAvg, err := b.classify(expr)
	if err != nil {
		return 0, err
	}
	b.cols = append(b.cols, col)
	idx := len(b.cols) - 1
	if isAvg {
		b.cols[idx].Count = b.hiddenCount(expr)
	}
	return idx, nil
}

// groupKey finds the column holding a GROUP BY item, adding one if needed.
func (b *mergeBuilder) groupKey(g *pg.Node) (int, error) {
	switch {
	case g.GetGroupingSet() != nil:
		return 0, unsupported("GROUPING SETS, ROLLUP and CUBE on several shards")
	case g.GetAConst().GetIval() != nil:
		n := int(g.GetAConst().GetIval().Ival)
		if n < 1 || n > b.explicit {
			return 0, unsupported("GROUP BY %d: there is no such select-list position", n)
		}
		return n - 1, nil
	default:
		// a column or expression: select a copy of it
	}
	if g.GetColumnRef() != nil {
		// Already selected as a plain column or an output alias: reuse it.
		if idx, found, _ := b.findColumn(g); found && b.cols[idx].Func == merge.Pass {
			return idx, nil
		}
	}
	return b.addHidden(g)
}

// orderTerm is an ORDER BY item resolved to a merge order.
type orderTerm struct {
	col   pendingOrder
	desc  bool
	nulls bool
}

// orderTerms resolves each ORDER BY item to a column of the shard result.
func (b *mergeBuilder) orderTerms() ([]orderTerm, error) {
	var out []orderTerm
	for _, item := range b.sel.SortClause {
		sb := item.GetSortBy()
		switch sb.SortbyDir {
		case pg.SortByDir_SORTBY_USING:
			return nil, unsupported("ORDER BY ... USING on several shards")
		default:
			// ASC, DESC or default
		}
		desc := sb.SortbyDir == pg.SortByDir_SORTBY_DESC
		nullsFirst := desc // PostgreSQL's default: NULL sorts as the largest value
		switch sb.SortbyNulls {
		case pg.SortByNulls_SORTBY_NULLS_FIRST:
			nullsFirst = true
		case pg.SortByNulls_SORTBY_NULLS_LAST:
			nullsFirst = false
		default:
			// the default chosen above
		}

		t := orderTerm{desc: desc, nulls: nullsFirst}
		idx, found, err := b.findColumn(sb.Node)
		switch {
		case err != nil:
			return nil, err
		case found:
			t.col = pendingOrder{index: idx, hidden: -1}
		default:
			// Not in the select list: select a copy of the expression. This
			// covers a column, a function call, or an aggregate.
			k := b.hidden
			idx, err := b.addHidden(sb.Node)
			if err != nil {
				return nil, err
			}
			switch {
			case b.star && !b.aggregate:
				t.col = pendingOrder{index: idx, hidden: k}
			default:
				t.col = pendingOrder{index: idx, hidden: -1}
			}
		}
		out = append(out, t)
	}
	return out, nil
}

// findColumn locates an ORDER BY item in the select list: an ordinal, an output
// alias, or a column that is selected.
func (b *mergeBuilder) findColumn(n *pg.Node) (int, bool, error) {
	if ival := n.GetAConst().GetIval(); ival != nil {
		pos := int(ival.Ival)
		if pos < 1 || (!b.star && pos > b.explicit) {
			return 0, false, unsupported("ORDER BY %d: there is no such select-list position", pos)
		}
		return pos - 1, true, nil
	}
	if b.star {
		// Positions after a * are not known, so look nothing up by name.
		return 0, false, nil
	}
	cr := n.GetColumnRef()
	if cr == nil {
		return 0, false, nil
	}
	text := columnText(cr)
	qualified := strings.Contains(text, ".")
	last := text[strings.LastIndex(text, ".")+1:]
	for i, t := range b.sel.TargetList[:b.explicit] {
		rt := t.GetResTarget()
		switch {
		case !qualified && rt.Name == text:
			return i, true, nil // an output alias
		case !isSameColumn(rt.Val, last):
			// not this column
		case !qualified, !strings.Contains(columnText(rt.Val.GetColumnRef()), "."):
			return i, true, nil // same column name, qualified on at most one side
		case columnText(rt.Val.GetColumnRef()) == text:
			return i, true, nil
		default:
			// a different table's column of the same name
		}
	}
	return 0, false, nil
}

// finishOrder turns resolved terms into merge.Order, naming hidden columns from
// the end when the select list has a *.
func (b *mergeBuilder) finishOrder(terms []orderTerm) []merge.Order {
	if len(terms) == 0 {
		return nil
	}
	out := make([]merge.Order, len(terms))
	for i, t := range terms {
		col := t.col.index
		if t.col.hidden >= 0 {
			col = t.col.hidden - b.hidden // -1 is the last hidden column
		}
		out[i] = merge.Order{Col: col, Desc: t.desc, NullsFirst: t.nulls}
	}
	return out
}

// having converts a HAVING condition to one the merge can evaluate on merged
// groups: and / or / not over comparisons of an aggregate (or group column)
// with a constant.
func (b *mergeBuilder) having(n *pg.Node) (merge.Cond, error) {
	switch {
	case n.GetBoolExpr() != nil:
		be := n.GetBoolExpr()
		var op string
		switch be.Boolop {
		case pg.BoolExprType_AND_EXPR:
			op = "and"
		case pg.BoolExprType_OR_EXPR:
			op = "or"
		case pg.BoolExprType_NOT_EXPR:
			op = "not"
		default:
			return merge.Cond{}, unsupported("this HAVING condition on several shards")
		}
		c := merge.Cond{Op: op}
		for _, a := range be.Args {
			sub, err := b.having(a)
			if err != nil {
				return merge.Cond{}, err
			}
			c.Args = append(c.Args, sub)
		}
		return c, nil

	case n.GetAExpr() != nil && n.GetAExpr().Kind == pg.A_Expr_Kind_AEXPR_OP:
		e := n.GetAExpr()
		op := opName(e)
		switch op {
		case "=", "<>", "<", "<=", ">", ">=":
			// supported comparison
		case "!=":
			op = "<>"
		default:
			return merge.Cond{}, unsupported("HAVING with the operator %q on several shards", op)
		}
		left, right := unwrapCast(e.Lexpr), unwrapCast(e.Rexpr)
		value, isConst, err := b.w.constant(right)
		if err != nil {
			return merge.Cond{}, err
		}
		operand := left
		if !isConst {
			// constant on the left: 5 < count(*)
			value, isConst, err = b.w.constant(left)
			if err != nil {
				return merge.Cond{}, err
			}
			operand = right
			op = flip(op)
		}
		if !isConst {
			return merge.Cond{}, unsupported("HAVING must compare an aggregate with a constant or a parameter on several shards")
		}
		idx, err := b.operand(operand)
		if err != nil {
			return merge.Cond{}, err
		}
		return merge.Cond{Op: op, Col: idx, Value: value}, nil

	default:
		return merge.Cond{}, unsupported("HAVING %s on several shards; use and / or / not over comparisons with a constant", nodeName(n))
	}
}

// operand finds or adds the merged column a HAVING comparison reads.
func (b *mergeBuilder) operand(n *pg.Node) (int, error) {
	if !containsAggregate(n) && n.GetColumnRef() != nil {
		if idx, found, err := b.findColumn(n); err != nil || found {
			return idx, err
		}
	}
	return b.addHidden(n)
}

func flip(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return op
	}
}

// count resolves a LIMIT or OFFSET to a number.
func (b *mergeBuilder) count(n *pg.Node, what string) (*int64, error) {
	if n == nil {
		if what == "OFFSET" {
			zero := int64(0)
			return &zero, nil
		}
		return nil, nil
	}
	n = unwrapCast(n)
	if c := n.GetAConst(); c != nil && c.Isnull {
		if what == "OFFSET" {
			zero := int64(0)
			return &zero, nil
		}
		return nil, nil // LIMIT NULL and LIMIT ALL mean no limit
	}
	v, ok, err := b.w.constant(n)
	if err != nil {
		return nil, err
	}
	i, isInt := toInt64(v)
	switch {
	case !ok || !isInt:
		return nil, unsupported("%s must be a whole number or a parameter holding one on several shards", what)
	case i < 0:
		return nil, unsupported("%s cannot be negative", what)
	default:
		return &i, nil
	}
}

// rejectNested refuses constructs that cannot run independently on each shard.
func rejectNested(sel *pg.SelectStmt) error {
	var err error
	walkNodes(sel.ProtoReflect(), func(n *pg.Node) bool {
		switch {
		case err != nil:
			return false
		case n.GetSubLink() != nil || n.GetRangeSubselect() != nil:
			err = unsupported("subqueries on several shards; run the inner query first, or filter by the shard key so only one shard is used")
		case n.GetFuncCall() != nil && n.GetFuncCall().Over != nil:
			err = unsupported("window functions on several shards: a window needs all rows in one place")
		default:
			// keep looking
		}
		return err == nil
	})
	return err
}

func isStar(n *pg.Node) bool {
	cr := n.GetColumnRef()
	if cr == nil || len(cr.Fields) == 0 {
		return false
	}
	return cr.Fields[len(cr.Fields)-1].GetAStar() != nil
}

func isAggregateName(name string) bool { return aggregateFuncs[name] }

// containsAggregate reports whether an aggregate call appears anywhere in n.
func containsAggregate(n *pg.Node) bool {
	if n == nil {
		return false
	}
	found := false
	walkNodes(n.ProtoReflect(), func(x *pg.Node) bool {
		if fc := x.GetFuncCall(); fc != nil && fc.Over == nil && isAggregateName(funcName(fc)) {
			found = true
		}
		return !found
	})
	return found
}

// isSameColumn reports whether n is a column reference whose column is name.
func isSameColumn(n *pg.Node, name string) bool {
	cr := n.GetColumnRef()
	if cr == nil || len(cr.Fields) == 0 {
		return false
	}
	last := cr.Fields[len(cr.Fields)-1].GetString_()
	return last != nil && last.Sval == name
}
