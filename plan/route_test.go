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

func TestRouteInsertNeedsAKey(t *testing.T) {
	reg, r := testSetup(t)
	a := analyze.Analysis{
		Op: analyze.OpInsert, Scopes: []int{-1}, Target: 0,
		Tables: []analyze.TableRef{{ID: 0, Name: "profiles"}},
	}
	if _, err := Route(a, reg, r, Options{}); !errors.Is(err, ErrShardKeyRequired) {
		t.Errorf("error = %v, want ErrShardKeyRequired", err)
	}
	a.Tables[0].Name = "countries"
	if p, err := Route(a, reg, r, Options{}); err != nil || p.Strategy != All {
		t.Errorf("INSERT into a global table: %+v, %v", p, err)
	}
}

func TestStrategyString(t *testing.T) {
	if Single.String() != "single" || Multi.String() != "multi" || All.String() != "all" || Strategy(9).String() != "Strategy(9)" {
		t.Error("unexpected Strategy strings")
	}
}
