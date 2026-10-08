package plan

import (
	"errors"
	"testing"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
)

// These tests need no SQL parser (and so no cgo): they route statements made
// with the query builder. route_suite_test.go covers raw SQL.

func testSetup(t *testing.T) (*registry.Registry, *router.HashRouter) {
	t.Helper()
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
		registry.Sharded("orders", registry.Key("customer_id"), registry.Type(registry.KeyInt)),
		registry.Global("countries"),
	)
	if err != nil {
		t.Fatal(err)
	}
	r, err := router.New(router.Even("a", "b", "c")...)
	if err != nil {
		t.Fatal(err)
	}
	return reg, r
}

func routeBuilt(t *testing.T, b interface {
	Build() (query.Statement, error)
}) (Plan, error) {
	t.Helper()
	reg, r := testSetup(t)
	st, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return Route(st.Analysis(), reg, r, Options{AnyShard: func() router.ShardID { return "c" }})
}

func TestRouteBuiltStatements(t *testing.T) {
	_, r := testSetup(t)
	owner := func(k any) router.ShardID { id, _ := r.ShardFor(k); return id }

	p, err := routeBuilt(t, query.From("profiles").Where(query.Eq("id", 42)))
	if err != nil || len(p.Targets) != 1 || p.Targets[0] != owner(42) || p.Strategy != Single || p.Reason == "" {
		t.Errorf("by key: %+v, %v", p, err)
	}

	p, err = routeBuilt(t, query.From("profiles").Where(query.In("id", 1, 2, 3, 4, 5, 6, 7, 8)))
	if err != nil || p.Strategy != All || len(p.Targets) != 3 {
		t.Errorf("key list over every shard: %+v, %v", p, err)
	}

	p, err = routeBuilt(t, query.From("countries"))
	if err != nil || len(p.Targets) != 1 || p.Targets[0] != "c" {
		t.Errorf("global read should use AnyShard: %+v, %v", p, err)
	}

	p, err = routeBuilt(t, query.DeleteFrom("countries"))
	if err != nil || p.Strategy != All || len(p.Targets) != 3 {
		t.Errorf("global write should reach every shard: %+v, %v", p, err)
	}

	p, err = routeBuilt(t, query.Update("addresses").Set("city", "x").Where(query.Eq("profile_id", 42)))
	if err != nil || len(p.Targets) != 1 || p.Targets[0] != owner(42) {
		t.Errorf("colocated table routes like its parent: %+v, %v", p, err)
	}
}

func TestRouteRefusals(t *testing.T) {
	tests := []struct {
		name string
		b    interface {
			Build() (query.Statement, error)
		}
		want error
	}{
		{"no condition", query.From("profiles"), ErrShardKeyRequired},
		{"range on the key", query.From("profiles").Where(query.Gt("id", 5)), ErrShardKeyRequired},
		{"other column only", query.From("profiles").Where(query.Eq("name", "x")), ErrShardKeyRequired},
		{"unknown table", query.From("mystery").Where(query.Eq("id", 1)), ErrUnknownTable},
		{"text that is not a number", query.From("profiles").Where(query.Eq("id", "abc")), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := routeBuilt(t, tc.b)
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRouteRefusesNonStatements(t *testing.T) {
	reg, r := testSetup(t)
	_, err := Route(analyze.Analysis{Op: analyze.OpOther, Kind: "CreateStmt"}, reg, r, Options{})
	if !errors.Is(err, analyze.ErrUnsupportedQuery) {
		t.Errorf("error = %v, want ErrUnsupportedQuery", err)
	}
}

func TestRouteInsert(t *testing.T) {
	reg, r := testSetup(t)
	insert := func(table string, in *analyze.Insert) analyze.Analysis {
		return analyze.Analysis{
			Op: analyze.OpInsert, Scopes: []int{-1}, Target: 0, Insert: in,
			Tables: []analyze.TableRef{{ID: 0, Name: table}},
		}
	}

	// An INSERT whose rows are not known cannot be routed.
	if _, err := Route(insert("profiles", nil), reg, r, Options{}); !errors.Is(err, analyze.ErrUnsupportedQuery) {
		t.Errorf("INSERT without rows: error = %v, want ErrUnsupportedQuery", err)
	}

	// Rows are grouped by the shard that owns each key.
	in := &analyze.Insert{
		Columns: []string{"name", "id"},
		Rows: [][]analyze.Cell{
			{{Value: "a", Known: true}, {Value: 1, Known: true}},
			{{Value: "b", Known: true}, {Value: 2, Known: true}},
			{{Value: "c", Known: true}, {Value: 1, Known: true}},
		},
	}
	p, err := Route(insert("profiles", in), reg, r, Options{})
	if err != nil {
		t.Fatal(err)
	}
	owner1, _ := r.ShardFor(1)
	if got := p.Rows[owner1]; len(got) < 2 || got[0] != 0 || got[len(got)-1] != 2 {
		t.Errorf("rows for the owner of key 1 = %v, want rows 0 and 2 in order", got)
	}
	total := 0
	for _, rows := range p.Rows {
		total += len(rows)
	}
	if total != 3 {
		t.Errorf("rows assigned = %d, want every row assigned exactly once", total)
	}

	// A global table takes every INSERT on every shard.
	if p, err := Route(insert("countries", in), reg, r, Options{}); err != nil || p.Strategy != All {
		t.Errorf("INSERT into a global table: %+v, %v", p, err)
	}
}

func TestRouteConflictCannotChangeTheKey(t *testing.T) {
	reg, r := testSetup(t)
	a := analyze.Analysis{
		Op: analyze.OpInsert, Scopes: []int{-1},
		Tables: []analyze.TableRef{{ID: 0, Name: "profiles"}},
		Insert: &analyze.Insert{
			Columns: []string{"id"}, Rows: [][]analyze.Cell{{{Value: 1, Known: true}}},
			ConflictSet: []string{"id"},
		},
	}
	if _, err := Route(a, reg, r, Options{}); !errors.Is(err, ErrShardKeyImmutable) {
		t.Errorf("error = %v, want ErrShardKeyImmutable", err)
	}
}

func TestStatementFor(t *testing.T) {
	p := Plan{SQL: "all", Args: []any{1}, PerShard: map[router.ShardID]ShardStatement{"b": {SQL: "just b", Args: []any{2}}}}
	if sql, args := p.StatementFor("a"); sql != "all" || len(args) != 1 || args[0] != 1 {
		t.Errorf("shard a: %q %v, want the plan's statement", sql, args)
	}
	if sql, args := p.StatementFor("b"); sql != "just b" || len(args) != 1 || args[0] != 2 {
		t.Errorf("shard b: %q %v, want its own statement", sql, args)
	}
}

func TestStrategyString(t *testing.T) {
	if Single.String() != "single" || Multi.String() != "multi" || All.String() != "all" || Strategy(9).String() != "Strategy(9)" {
		t.Error("unexpected Strategy strings")
	}
}
