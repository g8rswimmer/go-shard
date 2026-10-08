package shard

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/query"
)

// An analyzer that reads SQL but cannot rewrite it for several shards.
type routeOnly struct{}

func (routeOnly) FromSQL(string, []any) (analyze.Analysis, error) {
	return analyze.Analysis{Op: analyze.OpSelect, Scopes: []int{-1}}, nil
}

func TestFanOutNeedsAMergePlanner(t *testing.T) {
	db := routingDB(t, "a", "b")
	db.analyzer = routeOnly{}

	_, err := db.WithAllShards().Query(context.Background(), "SELECT id FROM profiles")
	if !errors.Is(err, ErrUnsupportedQuery) {
		t.Fatalf("err = %v, want ErrUnsupportedQuery", err)
	}
	for _, want := range []string{"2 shards", "package query", "merge.Planner"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message %q should mention %q", err, want)
		}
	}
}

func TestFanOutOfABuiltNonSelectIsRefused(t *testing.T) {
	db := routingDB(t, "a", "b")
	st, err := query.Update("profiles").Set("name", "x").Where(query.Eq("id", 1)).Build()
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.WithAllShards().QueryStatement(context.Background(), st)
	if !errors.Is(err, ErrUnsupportedQuery) {
		t.Errorf("err = %v, want ErrUnsupportedQuery", err)
	}
}

func TestAllowPartialAndShardErrors(t *testing.T) {
	ctx := context.Background()
	if partialAllowed(ctx) {
		t.Error("partial results must be opt-in")
	}
	if !partialAllowed(AllowPartial(ctx)) {
		t.Error("AllowPartial did not set the option")
	}
	if ShardErrors(nil) != nil {
		t.Error("ShardErrors(nil) should be nil")
	}
	failed := []*ShardError{{Shard: "b"}}
	if got := ShardErrors(&fanRows{failed: failed}); len(got) != 1 || got[0].Shard != "b" {
		t.Errorf("ShardErrors = %v", got)
	}
}
