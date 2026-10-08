package query

import (
	"reflect"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/analyze"
)

func mustBuild(t *testing.T, b interface{ Build() (Statement, error) }) Statement {
	t.Helper()
	st, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestSelectSQL(t *testing.T) {
	tests := []struct {
		name string
		b    interface{ Build() (Statement, error) }
		sql  string
		args []any
	}{
		{"all columns", From("profiles"), `SELECT * FROM "profiles"`, nil},
		{"columns", From("profiles").Columns("id", "name"), `SELECT "id", "name" FROM "profiles"`, nil},
		{"schema", From("sales.orders"), `SELECT * FROM "sales"."orders"`, nil},
		{"eq", From("profiles").Where(Eq("id", 42)), `SELECT * FROM "profiles" WHERE "id" = $1`, []any{42}},
		{"and of conditions", From("profiles").Where(Eq("id", 42), Gt("age", 18)).Where(Like("name", "A%")),
			`SELECT * FROM "profiles" WHERE "id" = $1 AND "age" > $2 AND "name" LIKE $3`, []any{42, 18, "A%"}},
		{"in", From("profiles").Where(In("id", 1, 2, 3)), `SELECT * FROM "profiles" WHERE "id" IN ($1, $2, $3)`, []any{1, 2, 3}},
		{"null tests", From("t").Where(IsNull("a"), NotNull("b")), `SELECT * FROM "t" WHERE "a" IS NULL AND "b" IS NOT NULL`, nil},
		{"every comparison", From("t").Where(Ne("a", 1), Lt("b", 2), Le("c", 3), Ge("d", 4)),
			`SELECT * FROM "t" WHERE "a" <> $1 AND "b" < $2 AND "c" <= $3 AND "d" >= $4`, []any{1, 2, 3, 4}},
		{"order limit offset", From("profiles").OrderBy("name", Asc).OrderBy("id", Desc).Limit(10).Offset(20),
			`SELECT * FROM "profiles" ORDER BY "name", "id" DESC LIMIT 10 OFFSET 20`, nil},
		{"update", Update("profiles").Set("name", "x").Set("age", 3).Where(Eq("id", 42)),
			`UPDATE "profiles" SET "name" = $1, "age" = $2 WHERE "id" = $3`, []any{"x", 3, 42}},
		{"delete", DeleteFrom("profiles").Where(Eq("id", 42)), `DELETE FROM "profiles" WHERE "id" = $1`, []any{42}},
		{"delete everything", DeleteFrom("profiles"), `DELETE FROM "profiles"`, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := mustBuild(t, tc.b)
			if st.SQL() != tc.sql {
				t.Errorf("SQL = %s\nwant  %s", st.SQL(), tc.sql)
			}
			if !reflect.DeepEqual(st.Args(), tc.args) {
				t.Errorf("Args = %v, want %v", st.Args(), tc.args)
			}
		})
	}
}

func TestAnalysis(t *testing.T) {
	st := mustBuild(t, From("profiles").Where(Eq("id", 42), In("tenant", "a", "b"), Gt("age", 1)).OrderBy("name", Desc).Limit(5).Offset(2))
	a := st.Analysis()

	if a.Op != analyze.OpSelect || len(a.Tables) != 1 || a.Tables[0].Name != "profiles" || a.Tables[0].Scope != 0 {
		t.Errorf("op/tables = %v %+v", a.Op, a.Tables)
	}
	if !reflect.DeepEqual(a.Scopes, []int{-1}) {
		t.Errorf("scopes = %v", a.Scopes)
	}
	want := []analyze.Binding{
		{Column: analyze.ColumnRef{Name: "id"}, Values: []any{42}},
		{Column: analyze.ColumnRef{Name: "tenant"}, Values: []any{"a", "b"}},
	}
	if !reflect.DeepEqual(a.Bindings, want) {
		t.Errorf("bindings = %+v, want %+v", a.Bindings, want)
	}
	if len(a.Notes) != 1 || !strings.Contains(a.Notes[0], `">"`) {
		t.Errorf("notes = %v, want one about the > comparison", a.Notes)
	}
	if len(a.OrderBy) != 1 || a.OrderBy[0].Column != "name" || !a.OrderBy[0].Desc {
		t.Errorf("order = %+v", a.OrderBy)
	}
	if a.Limit == nil || *a.Limit != 5 || a.Offset == nil || *a.Offset != 2 {
		t.Errorf("limit/offset = %v/%v", a.Limit, a.Offset)
	}
}

func TestWriteAnalysis(t *testing.T) {
	u := mustBuild(t, Update("sales.orders").Set("n", 1).Where(Eq("customer_id", 7))).Analysis()
	if u.Op != analyze.OpUpdate || u.Target != 0 || u.Tables[0].Schema != "sales" || u.Tables[0].Name != "orders" {
		t.Errorf("update analysis = %+v", u)
	}
	if len(u.Bindings) != 1 || u.Bindings[0].Column.Name != "customer_id" {
		t.Errorf("the SET value must not become a binding: %+v", u.Bindings)
	}
	d := mustBuild(t, DeleteFrom("t")).Analysis()
	if d.Op != analyze.OpDelete || len(d.Bindings) != 0 {
		t.Errorf("delete analysis = %+v", d)
	}
}

func TestBuildTwiceGivesTheSameStatement(t *testing.T) {
	for _, b := range []interface{ Build() (Statement, error) }{
		From("t").Where(Eq("a", 1), In("b", 2, 3)).OrderBy("a", Asc).Limit(1),
		Update("t").Set("a", 1).Where(Eq("b", 2)),
		DeleteFrom("t").Where(Eq("a", 1)),
	} {
		first, second := mustBuild(t, b), mustBuild(t, b)
		if first.SQL() != second.SQL() || !reflect.DeepEqual(first.Args(), second.Args()) ||
			!reflect.DeepEqual(first.Analysis(), second.Analysis()) {
			t.Errorf("Build is not repeatable:\n%+v\n%+v", first, second)
		}
	}
}

func TestBuildErrors(t *testing.T) {
	tests := []struct {
		name string
		b    interface{ Build() (Statement, error) }
		want string
	}{
		{"bad table", From("bad table"), `table "bad table" is not a valid identifier`},
		{"three-part table", From("a.b.c"), "use name or schema.name"},
		{"empty table", From(""), "not a valid identifier"},
		{"injection in a column", From("t").Columns(`id"; DROP TABLE t; --`), "not a valid identifier"},
		{"injection in a condition", From("t").Where(Eq(`x" OR "1"="1`, 1)), "not a valid identifier"},
		{"injection in order by", From("t").OrderBy("a; DROP", Asc), "not a valid identifier"},
		{"injection in set", Update("t").Set("a=1,b", 1), "not a valid identifier"},
		{"empty IN", From("t").Where(In("id")), "needs at least one value"},
		{"update without set", Update("t"), "at least one Set"},
		{"negative limit", From("t").Limit(-1), "Limit cannot be negative"},
		{"negative offset", From("t").Offset(-1), "Offset cannot be negative"},
		{"reports every problem", From("t").Columns("bad col").Where(In("id")).Limit(-1), "bad col"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.b.Build()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}

	_, err := From("t").Columns("bad col").Where(In("id")).Limit(-1).Build()
	for _, w := range []string{"bad col", "at least one value", "Limit"} {
		if err == nil || !strings.Contains(err.Error(), w) {
			t.Errorf("error should list every problem, missing %q: %v", w, err)
		}
	}
}
