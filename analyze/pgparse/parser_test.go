//go:build cgo

package pgparse

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/analyze"
)

func analyzeSQL(t *testing.T, sql string, args ...any) analyze.Analysis {
	t.Helper()
	a, err := New().FromSQL(sql, args)
	if err != nil {
		t.Fatalf("FromSQL(%q): %v", sql, err)
	}
	return a
}

func TestStatementKinds(t *testing.T) {
	tests := []struct {
		sql    string
		op     analyze.OpKind
		kind   string
		target string // table name written to
	}{
		{"SELECT 1", analyze.OpSelect, "", ""},
		{"SELECT * FROM t", analyze.OpSelect, "", ""},
		{"UPDATE t SET a = 1", analyze.OpUpdate, "", "t"},
		{"DELETE FROM t", analyze.OpDelete, "", "t"},
		{"INSERT INTO t (a) VALUES (1)", analyze.OpInsert, "", "t"},
		{"CREATE TABLE t (a int)", analyze.OpOther, "CreateStmt", ""},
		{"DROP TABLE t", analyze.OpOther, "DropStmt", ""},
		{"TRUNCATE t", analyze.OpOther, "TruncateStmt", ""},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			a := analyzeSQL(t, tc.sql)
			if a.Op != tc.op || a.Kind != tc.kind {
				t.Errorf("op/kind = %v/%q, want %v/%q", a.Op, a.Kind, tc.op, tc.kind)
			}
			if tc.target != "" && a.Tables[a.Target].Name != tc.target {
				t.Errorf("target = %+v, want table %s", a.Tables[a.Target], tc.target)
			}
		})
	}
}

func TestTables(t *testing.T) {
	a := analyzeSQL(t, `SELECT * FROM profiles p, public.addresses, sales."Orders" o JOIN countries ON true`)
	want := []analyze.TableRef{
		{ID: 0, Name: "profiles", Alias: "p"},
		{ID: 1, Schema: "public", Name: "addresses"},
		{ID: 2, Schema: "sales", Name: "Orders", Alias: "o"},
		{ID: 3, Name: "countries"},
	}
	if !reflect.DeepEqual(a.Tables, want) {
		t.Errorf("tables = %+v\nwant     %+v", a.Tables, want)
	}
}

func TestScopes(t *testing.T) {
	a := analyzeSQL(t, `SELECT (SELECT count(*) FROM c WHERE c.k = p.id), p.id FROM p
		WHERE EXISTS (SELECT 1 FROM a WHERE a.k IN (SELECT k FROM b))`)
	// 0: statement; subqueries: the select-list one, EXISTS, and one inside EXISTS.
	parents := map[int]bool{}
	for _, p := range a.Scopes {
		parents[p] = true
	}
	if len(a.Scopes) != 4 || a.Scopes[0] != -1 {
		t.Fatalf("scopes = %v, want 4 levels with the statement first", a.Scopes)
	}
	byName := map[string]int{}
	for _, tb := range a.Tables {
		byName[tb.Name] = tb.Scope
	}
	if byName["p"] != 0 {
		t.Errorf("p is at level %d, want 0", byName["p"])
	}
	for _, n := range []string{"a", "b", "c"} {
		if byName[n] == 0 {
			t.Errorf("table %s should be inside a subquery", n)
		}
	}
	if a.Scopes[byName["b"]] != byName["a"] {
		t.Errorf("b's level should sit inside a's level: scopes %v, levels %v", a.Scopes, byName)
	}
}

func TestBindingsResolveParameters(t *testing.T) {
	a := analyzeSQL(t, `SELECT * FROM t WHERE a = $1 AND b IN ($2, 7, 'x') AND c = ANY($3) AND d = 5 AND $4 = e`,
		42, "two", []int{1, 2}, "four")
	got := map[string][]any{}
	for _, b := range a.Bindings {
		got[b.Column.Name] = b.Values
	}
	want := map[string][]any{
		"a": {42},
		"b": {"two", int64(7), "x"},
		"c": {1, 2},
		"d": {int64(5)},
		"e": {"four"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("bindings = %v\nwant       %v", got, want)
	}
}

func TestLiteralForms(t *testing.T) {
	tests := []struct {
		sql  string
		want any
	}{
		{"a = 5", int64(5)},
		{"a = -5", int64(-5)},
		{"a = 3000000000", int64(3000000000)},
		{"a = -3000000000", int64(-3000000000)},
		{"a = 'text'", "text"},
		{"a = 5::bigint", int64(5)},
		{"a = true", true},
		{"a = 1.5", 1.5},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			a := analyzeSQL(t, "SELECT * FROM t WHERE "+tc.sql)
			if len(a.Bindings) != 1 || !reflect.DeepEqual(a.Bindings[0].Values, []any{tc.want}) {
				t.Errorf("bindings = %+v, want value %#v", a.Bindings, tc.want)
			}
		})
	}
}

func TestColumnReferences(t *testing.T) {
	a := analyzeSQL(t, "SELECT * FROM t WHERE x = 1 AND t.y = 2 AND public.t.z = 3")
	var got []analyze.ColumnRef
	for _, b := range a.Bindings {
		got = append(got, b.Column)
	}
	want := []analyze.ColumnRef{{Name: "x"}, {Qualifier: "t", Name: "y"}, {Qualifier: "public.t", Name: "z"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("columns = %+v, want %+v", got, want)
	}
}

func TestEqualitiesAndUsing(t *testing.T) {
	a := analyzeSQL(t, `SELECT * FROM p JOIN a ON a.pid = p.id AND a.x > 1 JOIN s USING (id, k) WHERE p.id = a.pid`)
	if len(a.Equalities) != 2 {
		t.Fatalf("equalities = %+v, want the ON one and the WHERE one", a.Equalities)
	}
	if e := a.Equalities[0]; e.Left.Qualifier != "a" || e.Left.Name != "pid" || e.Right.Qualifier != "p" || e.Right.Name != "id" {
		t.Errorf("first equality = %+v", e)
	}
	if len(a.Usings) != 1 || !reflect.DeepEqual(a.Usings[0].Columns, []string{"id", "k"}) ||
		!reflect.DeepEqual(a.Usings[0].Left, []int{0, 1}) || !reflect.DeepEqual(a.Usings[0].Right, []int{2}) {
		t.Errorf("usings = %+v, want columns id,k joining tables {0,1} to {2}", a.Usings)
	}
}

func TestConditionsInsideOrAreNotBindings(t *testing.T) {
	a := analyzeSQL(t, "SELECT * FROM t WHERE a = 1 OR b = 2")
	if len(a.Bindings) != 0 {
		t.Errorf("bindings = %+v, want none", a.Bindings)
	}
	if len(a.Notes) != 1 || !strings.Contains(a.Notes[0], "OR") {
		t.Errorf("notes = %v, want one about OR", a.Notes)
	}
}

func TestConstantsInOuterJoinConditionsAreNotBindings(t *testing.T) {
	a := analyzeSQL(t, "SELECT * FROM p LEFT JOIN a ON a.pid = p.id AND a.pid = 5")
	if len(a.Bindings) != 0 || len(a.Equalities) != 1 {
		t.Errorf("bindings %+v equalities %+v: the link counts, the constant must not", a.Bindings, a.Equalities)
	}
	a = analyzeSQL(t, "SELECT * FROM p JOIN a ON a.pid = p.id AND a.pid = 5")
	if len(a.Bindings) != 1 {
		t.Errorf("bindings = %+v, want the constant of an inner join kept", a.Bindings)
	}
}

func TestNotesAreDeduplicated(t *testing.T) {
	a := analyzeSQL(t, "SELECT * FROM t WHERE a > 1 AND a > 2 AND b > 3")
	if len(a.Notes) != 1 {
		t.Errorf("notes = %v, want the repeated comparison noted once", a.Notes)
	}
}

func TestMergeFacts(t *testing.T) {
	a := analyzeSQL(t, `SELECT DISTINCT p.name, count(*), sum(DISTINCT x), max(y) OVER (PARTITION BY z)
		FROM t p GROUP BY p.name, 2, lower(p.name) HAVING count(*) > 1
		ORDER BY p.name DESC NULLS LAST, 2, lower(name) LIMIT $1 OFFSET 20`, int64(10))

	if !a.Distinct || !a.HasHaving || !a.HasWindow {
		t.Errorf("distinct/having/window = %v/%v/%v, want all true", a.Distinct, a.HasHaving, a.HasWindow)
	}
	wantAgg := []analyze.AggregateTerm{{Func: "count", Star: true}, {Func: "sum", Distinct: true}}
	if !reflect.DeepEqual(a.Aggregates, wantAgg) {
		t.Errorf("aggregates = %+v, want %+v (a windowed max is not a plain aggregate)", a.Aggregates, wantAgg)
	}
	if !reflect.DeepEqual(a.GroupBy, []string{"p.name", "#2", "(expression)"}) {
		t.Errorf("group by = %v", a.GroupBy)
	}
	wantOrder := []analyze.OrderTerm{
		{Column: "p.name", Desc: true, Nulls: "last"},
		{Ordinal: 2},
		{Complex: true},
	}
	if !reflect.DeepEqual(a.OrderBy, wantOrder) {
		t.Errorf("order by = %+v\nwant       %+v", a.OrderBy, wantOrder)
	}
	if a.Limit == nil || *a.Limit != 10 || a.Offset == nil || *a.Offset != 20 {
		t.Errorf("limit/offset = %v/%v, want 10/20", a.Limit, a.Offset)
	}
}

func TestLimitForms(t *testing.T) {
	if a := analyzeSQL(t, "SELECT * FROM t LIMIT ALL"); a.Limit != nil {
		t.Errorf("LIMIT ALL: limit = %d, want none", *a.Limit)
	}
	if a := analyzeSQL(t, "SELECT * FROM t"); a.Limit != nil || a.Offset != nil {
		t.Error("no limit or offset expected")
	}
	if a := analyzeSQL(t, "SELECT * FROM t LIMIT 5 + 5"); a.Limit != nil {
		t.Error("an expression limit is not resolved")
	}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		sql  string
		args []any
		is   error
		has  string
	}{
		{"SELECT * FROM t WHERE a = $1", nil, analyze.ErrMissingArgument, "$1"},
		{"SELECT * FROM t WHERE a = $3", []any{1}, analyze.ErrMissingArgument, "1 argument"},
		{"WITH x AS (SELECT 1) SELECT * FROM x", nil, analyze.ErrUnsupportedQuery, "WITH"},
		{"UPDATE t SET a = 1 WHERE b IN (WITH x AS (SELECT 1) SELECT * FROM x)", nil, analyze.ErrUnsupportedQuery, "WITH"},
		{"SELECT * FROM t WHERE a IN (SELECT 1 UNION SELECT 2)", nil, analyze.ErrUnsupportedQuery, "UNION"},
		{"SELECT * INTO u FROM t", nil, analyze.ErrUnsupportedQuery, "INTO"},
		{"SELECT 1; SELECT 2", nil, analyze.ErrUnsupportedQuery, "2 statements"},
		{"", nil, analyze.ErrUnsupportedQuery, "empty"},
		{"SELEC 1", nil, nil, "cannot parse SQL"},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := New().FromSQL(tc.sql, tc.args)
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("error = %v, want it to wrap %v", err, tc.is)
			}
			if !strings.Contains(err.Error(), tc.has) {
				t.Errorf("error = %v, want it to mention %q", err, tc.has)
			}
		})
	}
}

func TestParserIsSafeForConcurrentUse(t *testing.T) {
	p := New()
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 50; j++ {
				a, err := p.FromSQL("SELECT * FROM t WHERE id = $1", []any{j})
				if err != nil || len(a.Bindings) != 1 || a.Bindings[0].Values[0] != j {
					done <- errors.New("wrong or failed analysis under concurrency")
					return
				}
			}
			done <- nil
		}()
	}
	for i := 0; i < 8; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}

func TestInsertFacts(t *testing.T) {
	a := analyzeSQL(t, `INSERT INTO profiles (id, name, note) VALUES ($1, 'a', DEFAULT), (7::bigint, $2, now())
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, note = $3 RETURNING id`, 42, "n", "x")

	if a.Op != analyze.OpInsert || a.Tables[a.Target].Name != "profiles" || a.Insert == nil {
		t.Fatalf("analysis = %+v", a)
	}
	in := a.Insert
	if !reflect.DeepEqual(in.Columns, []string{"id", "name", "note"}) {
		t.Errorf("columns = %v", in.Columns)
	}
	want := [][]analyze.Cell{
		{{Value: 42, Known: true}, {Value: "a", Known: true}, {Known: false}},
		{{Value: int64(7), Known: true}, {Value: "n", Known: true}, {Known: false}},
	}
	if !reflect.DeepEqual(in.Rows, want) {
		t.Errorf("rows = %+v\nwant   %+v", in.Rows, want)
	}
	if !reflect.DeepEqual(in.ConflictSet, []string{"name", "note"}) {
		t.Errorf("conflict set = %v", in.ConflictSet)
	}
}

func TestInsertFormsWithoutKnownRows(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO t (a) SELECT a FROM u",
		"INSERT INTO t DEFAULT VALUES",
	} {
		if a := analyzeSQL(t, sql); a.Op != analyze.OpInsert || a.Insert != nil {
			t.Errorf("%s: want an INSERT with no rows known, got %+v", sql, a.Insert)
		}
	}
	if a := analyzeSQL(t, "INSERT INTO t VALUES (1, 2)"); a.Insert == nil || len(a.Insert.Columns) != 0 || len(a.Insert.Rows) != 1 {
		t.Errorf("an INSERT without a column list: %+v", a.Insert)
	}
	if a := analyzeSQL(t, "INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING"); len(a.Insert.ConflictSet) != 0 {
		t.Errorf("DO NOTHING assigns nothing: %v", a.Insert.ConflictSet)
	}
}

func TestInsertErrors(t *testing.T) {
	for sql, has := range map[string]string{
		"INSERT INTO t (a) VALUES ((SELECT 1))":             "subquery inside VALUES",
		"WITH x AS (SELECT 1) INSERT INTO t (a) VALUES (1)": "WITH",
	} {
		if _, err := New().FromSQL(sql, nil); !errors.Is(err, analyze.ErrUnsupportedQuery) || !strings.Contains(err.Error(), has) {
			t.Errorf("%s: error = %v, want ErrUnsupportedQuery mentioning %q", sql, err, has)
		}
	}
	if _, err := New().FromSQL("INSERT INTO t (a) VALUES ($1)", nil); !errors.Is(err, analyze.ErrMissingArgument) {
		t.Errorf("error = %v, want ErrMissingArgument", err)
	}
}

func TestUpdateSetColumns(t *testing.T) {
	a := analyzeSQL(t, "UPDATE t SET a = 1, b = $1, (c, d) = (1, 2) WHERE id = 5", 9)
	if !reflect.DeepEqual(a.SetColumns, []string{"a", "b", "c", "d"}) {
		t.Errorf("set columns = %v", a.SetColumns)
	}
	if a := analyzeSQL(t, "DELETE FROM t WHERE id = 5"); len(a.SetColumns) != 0 {
		t.Errorf("DELETE sets nothing: %v", a.SetColumns)
	}
}

func TestSplitRows(t *testing.T) {
	const sql = `INSERT INTO profiles (id, name, note) VALUES ($1, $2, 'a'), ($3, $4, DEFAULT), (7, $2, now())
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, note = $5 RETURNING id`
	args := []any{"id0", "name0", "id1", "name1", "note"}
	p := New()

	t.Run("keeps the rows asked for, in order", func(t *testing.T) {
		got, gotArgs, err := p.SplitRows(sql, args, []int{2, 0})
		if err != nil {
			t.Fatal(err)
		}
		// Parameters are renumbered from $1 in the order the statement uses
		// them; rows 2 and 0 share name0 so it appears once.
		if want := []any{"name0", "id0", "note"}; !reflect.DeepEqual(gotArgs, want) {
			t.Errorf("args = %v, want %v", gotArgs, want)
		}
		for _, w := range []string{"VALUES (7, $1, now()), ($2, $1, 'a')", "note = $3", "RETURNING id"} {
			if !strings.Contains(got, w) {
				t.Errorf("SQL does not contain %q:\n%s", w, got)
			}
		}
		if strings.Contains(got, "DEFAULT") || strings.Contains(got, "$4") {
			t.Errorf("row 1 should be gone, with no gaps in the parameters:\n%s", got)
		}
	})

	t.Run("the result reads back as the same rows", func(t *testing.T) {
		got, gotArgs, err := p.SplitRows(sql, args, []int{1})
		if err != nil {
			t.Fatal(err)
		}
		a := analyzeSQL(t, got, gotArgs...)
		if a.Insert == nil || len(a.Insert.Rows) != 1 || a.Insert.Rows[0][0].Value != "id1" || a.Insert.Rows[0][1].Value != "name1" {
			t.Errorf("split statement analyzes as %+v", a.Insert)
		}
	})

	t.Run("is deterministic", func(t *testing.T) {
		first, firstArgs, _ := p.SplitRows(sql, args, []int{0, 2})
		for i := 0; i < 50; i++ {
			again, againArgs, err := p.SplitRows(sql, args, []int{0, 2})
			if err != nil || again != first || !reflect.DeepEqual(againArgs, firstArgs) {
				t.Fatalf("run %d differs:\n%s\n%s", i, first, again)
			}
		}
	})

	t.Run("keeping every row changes nothing but formatting", func(t *testing.T) {
		got, gotArgs, err := p.SplitRows("INSERT INTO t (a) VALUES ($1), ($2)", []any{1, 2}, []int{0, 1})
		if err != nil || got != "INSERT INTO t (a) VALUES ($1), ($2)" || !reflect.DeepEqual(gotArgs, []any{1, 2}) {
			t.Errorf("got %q %v %v", got, gotArgs, err)
		}
	})

	t.Run("errors", func(t *testing.T) {
		for name, tc := range map[string]struct {
			sql  string
			args []any
			rows []int
			is   error
			has  string
		}{
			"row out of range":  {sql, args, []int{3}, nil, "row 3 is out of range"},
			"negative row":      {sql, args, []int{-1}, nil, "out of range"},
			"no rows":           {sql, args, nil, analyze.ErrUnsupportedQuery, "no rows"},
			"not an INSERT":     {"SELECT 1", nil, []int{0}, analyze.ErrUnsupportedQuery, "INSERT ... VALUES"},
			"INSERT ... SELECT": {"INSERT INTO t (a) SELECT 1", nil, []int{0}, analyze.ErrUnsupportedQuery, "INSERT ... VALUES"},
			"two statements":    {"SELECT 1; SELECT 2", nil, []int{0}, analyze.ErrUnsupportedQuery, "2 statements"},
			"missing argument":  {"INSERT INTO t (a) VALUES ($2)", []any{1}, []int{0}, analyze.ErrMissingArgument, "$2"},
			"syntax error":      {"INSERT INTO", nil, []int{0}, nil, "cannot parse SQL"},
		} {
			_, _, err := p.SplitRows(tc.sql, tc.args, tc.rows)
			if err == nil || !strings.Contains(err.Error(), tc.has) || (tc.is != nil && !errors.Is(err, tc.is)) {
				t.Errorf("%s: error = %v, want %v mentioning %q", name, err, tc.is, tc.has)
			}
		}
	})
}
