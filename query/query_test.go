package query

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/merge"
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
		{"insert one row", InsertInto("profiles").Columns("id", "name").Row(1, "a"),
			`INSERT INTO "profiles" ("id", "name") VALUES ($1, $2)`, []any{1, "a"}},
		{"insert several rows", InsertInto("profiles").Columns("id", "name").Row(1, "a").Row(2, "b"),
			`INSERT INTO "profiles" ("id", "name") VALUES ($1, $2), ($3, $4)`, []any{1, "a", 2, "b"}},
		{"insert on conflict do nothing", InsertInto("t").Columns("id").Row(1).OnConflictDoNothing(),
			`INSERT INTO "t" ("id") VALUES ($1) ON CONFLICT DO NOTHING`, []any{1}},
		{"insert upsert", InsertInto("t").Columns("id", "name").Row(1, "a").OnConflictUpdate([]string{"id"}, "name"),
			`INSERT INTO "t" ("id", "name") VALUES ($1, $2) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"`, []any{1, "a"}},
		{"insert returning", InsertInto("t").Columns("id").Row(1).Returning("id", "name"),
			`INSERT INTO "t" ("id") VALUES ($1) RETURNING "id", "name"`, []any{1}},
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
		{"insert without columns", InsertInto("t").Row(1), "needs Columns"},
		{"insert without rows", InsertInto("t").Columns("id"), "at least one Row"},
		{"insert row of the wrong width", InsertInto("t").Columns("a", "b").Row(1, 2).Row(3), "row 1 has 1 values for 2 columns"},
		{"insert with a bad column", InsertInto("t").Columns("a; DROP").Row(1), "not a valid identifier"},
		{"upsert without a target", InsertInto("t").Columns("a").Row(1).OnConflictUpdate(nil, "a"), "conflict target"},
		{"upsert without columns to set", InsertInto("t").Columns("a").Row(1).OnConflictUpdate([]string{"a"}), "at least one column to set"},
		{"insert with a bad returning column", InsertInto("t").Columns("a").Row(1).Returning("x y"), "not a valid identifier"},
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

func TestInsertAnalysis(t *testing.T) {
	a := mustBuild(t, InsertInto("public.profiles").Columns("name", "id").Row("a", 1).Row("b", 2).
		OnConflictUpdate([]string{"id"}, "name")).Analysis()

	if a.Op != analyze.OpInsert || a.Target != 0 || a.Tables[0].Schema != "public" || a.Tables[0].Name != "profiles" {
		t.Errorf("op/target/table = %v %d %+v", a.Op, a.Target, a.Tables)
	}
	if a.Insert == nil || !reflect.DeepEqual(a.Insert.Columns, []string{"name", "id"}) || !reflect.DeepEqual(a.Insert.ConflictSet, []string{"name"}) {
		t.Fatalf("insert = %+v", a.Insert)
	}
	want := [][]analyze.Cell{
		{{Value: "a", Known: true}, {Value: 1, Known: true}},
		{{Value: "b", Known: true}, {Value: 2, Known: true}},
	}
	if !reflect.DeepEqual(a.Insert.Rows, want) {
		t.Errorf("rows = %+v, want %+v", a.Insert.Rows, want)
	}
}

func TestUpdateAnalysisHasSetColumns(t *testing.T) {
	a := mustBuild(t, Update("t").Set("a", 1).Set("b", 2).Where(Eq("id", 1))).Analysis()
	if !reflect.DeepEqual(a.SetColumns, []string{"a", "b"}) {
		t.Errorf("set columns = %v", a.SetColumns)
	}
}

func TestSplitRows(t *testing.T) {
	st := mustBuild(t, InsertInto("t").Columns("id", "name").Row(1, "a").Row(2, "b").Row(3, "c").OnConflictDoNothing())

	sql, args, err := st.SplitRows([]int{2, 0})
	if err != nil {
		t.Fatal(err)
	}
	if want := `INSERT INTO "t" ("id", "name") VALUES ($1, $2), ($3, $4) ON CONFLICT DO NOTHING`; sql != want {
		t.Errorf("SQL = %s\nwant  %s", sql, want)
	}
	if !reflect.DeepEqual(args, []any{3, "c", 1, "a"}) {
		t.Errorf("args = %v, want rows 2 then 0", args)
	}

	// Splitting does not disturb the statement.
	if len(st.Args()) != 6 || !strings.Contains(st.SQL(), "($5, $6)") {
		t.Errorf("the statement changed: %s %v", st.SQL(), st.Args())
	}

	for name, rows := range map[string][]int{"none": nil, "negative": {-1}, "past the end": {3}} {
		if _, _, err := st.SplitRows(rows); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if _, _, err := mustBuild(t, From("t")).SplitRows([]int{0}); err == nil {
		t.Error("only an INSERT can be split")
	}
}

func TestInsertBuilderCanBeChangedAfterBuild(t *testing.T) {
	b := InsertInto("t").Columns("id").Row(1)
	first := mustBuild(t, b)
	b.Row(2).Columns("id")
	second := mustBuild(t, b)
	if len(first.Args()) != 1 || len(second.Args()) != 2 || len(first.Analysis().Insert.Rows) != 1 {
		t.Errorf("a built statement must not change when the builder does: %v / %v", first.Args(), second.Args())
	}
}

func TestSelectMergePlan(t *testing.T) {
	lim := func(n int64) *int64 { return &n }
	tests := []struct {
		name string
		b    *SelectBuilder
		sql  string
		spec merge.Spec
	}{
		{"plain",
			From("profiles").Columns("id", "name"),
			`SELECT "id", "name" FROM "profiles"`,
			merge.Spec{}},
		{"order on selected columns, limit widened by offset",
			From("profiles").Columns("id", "name").OrderBy("name", Desc).OrderBy("id", Asc).Limit(5).Offset(10),
			`SELECT "id", "name" FROM "profiles" ORDER BY "name" DESC, "id" LIMIT 15`,
			merge.Spec{Order: []merge.Order{{Col: 1, Desc: true, NullsFirst: true}, {Col: 0}}, Offset: 10, Limit: lim(5)}},
		{"offset alone stays out of the shard statement",
			From("profiles").Columns("id").OrderBy("id", Asc).Offset(3),
			`SELECT "id" FROM "profiles" ORDER BY "id"`,
			merge.Spec{Order: []merge.Order{{Col: 0}}, Offset: 3}},
		{"order column that is not selected is added",
			From("profiles").Columns("id").OrderBy("created", Asc).Where(Eq("age", 3)),
			`SELECT "id", "created" FROM "profiles" WHERE "age" = $1 ORDER BY "created"`,
			merge.Spec{Hidden: 1, Order: []merge.Order{{Col: 1}}}},
		{"select * names hidden columns from the end",
			From("profiles").OrderBy("created", Desc).OrderBy("id", Asc).Limit(3),
			`SELECT *, "created", "id" FROM "profiles" ORDER BY "created" DESC, "id" LIMIT 3`,
			merge.Spec{Hidden: 2, Order: []merge.Order{{Col: -2, Desc: true, NullsFirst: true}, {Col: -1}}, Limit: lim(3)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := mustBuild(t, tc.b).MergePlan()
			if err != nil {
				t.Fatal(err)
			}
			if p.SQL != tc.sql {
				t.Errorf("shard SQL:\n got  %s\n want %s", p.SQL, tc.sql)
			}
			if !reflect.DeepEqual(p.Spec, tc.spec) {
				t.Errorf("spec:\n got  %+v\n want %+v", p.Spec, tc.spec)
			}
		})
	}

	t.Run("arguments are shared with the statement", func(t *testing.T) {
		st := mustBuild(t, From("t").Where(Eq("a", 1), In("b", 2, 3)))
		p, _ := st.MergePlan()
		if !reflect.DeepEqual(p.Args, []any{1, 2, 3}) {
			t.Errorf("args = %v", p.Args)
		}
	})

	t.Run("only a SELECT has one", func(t *testing.T) {
		for name, b := range map[string]interface{ Build() (Statement, error) }{
			"update": Update("t").Set("a", 1).Where(Eq("id", 1)),
			"delete": DeleteFrom("t").Where(Eq("id", 1)),
		} {
			if _, err := mustBuild(t, b).MergePlan(); !errors.Is(err, analyze.ErrUnsupportedQuery) {
				t.Errorf("%s: err = %v, want ErrUnsupportedQuery", name, err)
			}
		}
	})
}
