//go:build cgo

package plan_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/analyze/pgparse"
	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/router"
)

// The builder knows its routing without parsing. This test proves that is the
// same routing the parser would find in the SQL the builder wrote.
func TestBuilderAndParserAgree(t *testing.T) {
	reg, r, parser := suiteRegistry(t), suiteRouter(t), pgparse.New()
	k := pickKeys(t, r)

	type built struct {
		name string
		b    interface {
			Build() (query.Statement, error)
		}
	}
	one, two, three := int64(1), int64(2), int64(3)
	_ = two
	cases := []built{
		{"get by key", query.From("profiles").Where(query.Eq("id", 42))},
		{"columns and order", query.From("profiles").Columns("id", "name").Where(query.Eq("id", 42)).OrderBy("name", query.Desc).Limit(10).Offset(5)},
		{"in list", query.From("profiles").Where(query.In("id", k.k1, k.k2))},
		{"in list on one shard", query.From("profiles").Where(query.In("id", k.same1, k.same2))},
		{"in list on every shard", query.From("profiles").Where(query.In("id", k.three...))},
		{"extra conditions", query.From("profiles").Where(query.Eq("id", 42), query.Gt("age", 1), query.Like("name", "a%"))},
		{"eq and in", query.From("profiles").Where(query.In("id", k.k1, k.k2), query.Eq("id", k.k2))},
		{"conflict", query.From("profiles").Where(query.Eq("id", k.k1), query.Eq("id", k.k2))},
		{"no condition", query.From("profiles")},
		{"non-key condition", query.From("profiles").Where(query.Eq("name", "x"))},
		{"range on key", query.From("profiles").Where(query.Gt("id", 5))},
		{"null test on key", query.From("profiles").Where(query.IsNull("id"))},
		{"string key", query.From("events").Where(query.Eq("tenant", "acme"))},
		{"uuid key", query.From("sessions").Where(query.Eq("uid", "123e4567-e89b-12d3-a456-426614174000"))},
		{"text for int key", query.From("profiles").Where(query.Eq("id", "42"))},
		{"colocated table", query.From("addresses").Where(query.Eq("profile_id", 42))},
		{"global table", query.From("countries").Where(query.Eq("code", "US"))},
		{"schema qualified", query.From("public.profiles").Where(query.Eq("id", 42))},
		{"pointer key", query.From("profiles").Where(query.Eq("id", &one))},
		{"update by key", query.Update("profiles").Set("name", "x").Where(query.Eq("id", 42))},
		{"update set", query.Update("profiles").Set("name", "x").Where(query.In("id", k.k1, k.k2))},
		{"update without key", query.Update("profiles").Set("name", "x")},
		{"update global", query.Update("countries").Set("name", "x").Where(query.Eq("code", "US"))},
		{"insert one row", query.InsertInto("profiles").Columns("id", "name").Row(42, "x")},
		{"insert rows on one shard", query.InsertInto("profiles").Columns("id", "name").Row(k.same1, "a").Row(k.same2, "b")},
		{"insert rows on two shards", query.InsertInto("profiles").Columns("id", "name").Row(k.k1, "a").Row(k.k2, "b").Row(k.k1, "c")},
		{"insert key last", query.InsertInto("profiles").Columns("name", "id").Row("a", 42)},
		{"insert without the key", query.InsertInto("profiles").Columns("name").Row("a")},
		{"insert into global", query.InsertInto("countries").Columns("code").Row("US")},
		{"insert into colocated", query.InsertInto("addresses").Columns("id", "profile_id").Row(1, 42)},
		{"upsert", query.InsertInto("profiles").Columns("id", "name").Row(42, "x").OnConflictUpdate([]string{"id"}, "name")},
		{"upsert of the key", query.InsertInto("profiles").Columns("id", "name").Row(42, "x").OnConflictUpdate([]string{"id"}, "id")},
		{"update of the key", query.Update("profiles").Set("id", 5).Where(query.Eq("id", 1))},
		{"delete by key", query.DeleteFrom("profiles").Where(query.Eq("id", &three))},
		{"delete global", query.DeleteFrom("countries")},
		{"unknown table", query.From("mystery").Where(query.Eq("id", 1))},
	}

	opts := plan.Options{AnyShard: func() router.ShardID { return "b" }}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := tc.b.Build()
			if err != nil {
				t.Fatal(err)
			}

			parsed, err := parser.FromSQL(st.SQL(), st.Args())
			if err != nil {
				t.Fatalf("the parser cannot read the builder's SQL %q: %v", st.SQL(), err)
			}

			// What the router reads from the two analyses is identical.
			got, want := st.Analysis(), parsed
			for _, f := range []struct {
				name     string
				got, wnt any
			}{
				{"Op", got.Op, want.Op}, {"Scopes", got.Scopes, want.Scopes}, {"Tables", got.Tables, want.Tables},
				{"Target", got.Target, want.Target}, {"Bindings", got.Bindings, want.Bindings},
				{"Equalities", got.Equalities, want.Equalities}, {"OrderBy", got.OrderBy, want.OrderBy},
				{"Insert", got.Insert, want.Insert}, {"SetColumns", got.SetColumns, want.SetColumns},
				{"Limit", deref(got.Limit), deref(want.Limit)}, {"Offset", deref(got.Offset), deref(want.Offset)},
			} {
				if !reflect.DeepEqual(f.got, f.wnt) {
					t.Errorf("%s differs\nbuilder: %+v\nparser:  %+v\nSQL: %s", f.name, f.got, f.wnt, st.SQL())
				}
			}

			// And so is the routing decision, success or failure.
			bp, berr := plan.Route(got, reg, r, opts)
			pp, perr := plan.Route(want, reg, r, opts)
			if (berr == nil) != (perr == nil) {
				t.Fatalf("builder error = %v, parser error = %v", berr, perr)
			}
			if berr != nil {
				if fmt.Sprint(unwrapSentinel(berr)) != fmt.Sprint(unwrapSentinel(perr)) {
					t.Errorf("different errors:\nbuilder: %v\nparser:  %v", berr, perr)
				}
				return
			}
			if fmt.Sprint(bp.Rows) != fmt.Sprint(pp.Rows) {
				t.Errorf("builder assigns rows %v, parser %v", bp.Rows, pp.Rows)
			}
			if fmt.Sprint(bp.Targets, bp.Strategy) != fmt.Sprint(pp.Targets, pp.Strategy) {
				t.Errorf("builder routes to %v %v, parser to %v %v", bp.Targets, bp.Strategy, pp.Targets, pp.Strategy)
			}
		})
	}
}

func deref(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

// unwrapSentinel names which routing error it is, ignoring the wording.
func unwrapSentinel(err error) string {
	for _, s := range []error{plan.ErrShardKeyRequired, plan.ErrCrossShardJoin, plan.ErrUnknownTable, analyze.ErrUnsupportedQuery} {
		if errorsIs(err, s) {
			return s.Error()
		}
	}
	return err.Error()
}

func errorsIs(err, target error) bool { return errors.Is(err, target) }

// Splitting a built INSERT and splitting its SQL with the parser give the same
// rows, whichever shard-by-shard route is taken.
func TestBuilderAndParserSplitAgree(t *testing.T) {
	st, err := query.InsertInto("profiles").Columns("id", "name", "note").
		Row(1, "a", "x").Row(2, "b", "y").Row(3, "c", "z").Row(4, "d", "w").
		OnConflictDoNothing().Returning("id").Build()
	if err != nil {
		t.Fatal(err)
	}
	parser := pgparse.New()

	for _, rows := range [][]int{{0}, {3}, {1, 2}, {3, 0, 2}, {0, 1, 2, 3}} {
		bSQL, bArgs, err := st.SplitRows(rows)
		if err != nil {
			t.Fatal(err)
		}
		pSQL, pArgs, err := parser.SplitRows(st.SQL(), st.Args(), rows)
		if err != nil {
			t.Fatal(err)
		}
		ba, err := parser.FromSQL(bSQL, bArgs)
		if err != nil {
			t.Fatalf("rows %v: the parser cannot read the builder's split %q: %v", rows, bSQL, err)
		}
		pa, err := parser.FromSQL(pSQL, pArgs)
		if err != nil {
			t.Fatalf("rows %v: cannot read the parser's split %q: %v", rows, pSQL, err)
		}
		if !reflect.DeepEqual(ba.Insert, pa.Insert) {
			t.Errorf("rows %v differ:\nbuilder: %+v\nparser:  %+v", rows, ba.Insert, pa.Insert)
		}
		if len(ba.Insert.Rows) != len(rows) {
			t.Errorf("rows %v: split has %d rows", rows, len(ba.Insert.Rows))
		}
	}
}
