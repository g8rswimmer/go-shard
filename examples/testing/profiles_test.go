//go:build cgo

package profiles_test

import (
	"context"
	"errors"
	"testing"

	"github.com/g8rswimmer/go-shard"
	profiles "github.com/g8rswimmer/go-shard/examples/testing"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/shardtest"
)

// This is a unit test: no Docker and no PostgreSQL. shardtest.NewFake routes
// every statement with the library's real rules (the registry, the hash, the
// SQL parser), records where it would have gone, and answers with the rows you
// stub. What it checks is routing, which is what breaks when a query changes.
//
// Raw SQL is parsed with cgo. Without cgo, build statements with package query
// (see docs/BUILDING.md).

var shards = []shard.ShardID{"shard-01", "shard-02", "shard-03"}

func newRepo(t *testing.T) (profiles.Repo, *shardtest.Fake) {
	t.Helper()
	reg, err := registry.New(registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)))
	if err != nil {
		t.Fatal(err)
	}
	fake := shardtest.NewFake(t, reg, shards...)
	return profiles.Repo{DB: fake}, fake
}

func TestGetGoesToOneShard(t *testing.T) {
	repo, fake := newRepo(t)
	fake.StubRows("FROM profiles", []string{"id", "name", "country"}, []any{int64(42), "Ada", "UK"})

	got, err := repo.Get(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "Ada" {
		t.Errorf("got %+v", got)
	}

	// The query names the shard key, so exactly one shard answers it.
	fake.AssertSingleShard("FROM profiles")
	t.Logf("LastPlan:\n%s", fake.LastPlan())
}

func TestGetNotFound(t *testing.T) {
	repo, _ := newRepo(t) // no stub: the fake returns no rows
	if _, err := repo.Get(context.Background(), 7); !errors.Is(err, profiles.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestRename(t *testing.T) {
	repo, fake := newRepo(t)
	fake.StubAffected("UPDATE profiles", 1)

	if err := repo.Rename(context.Background(), 42, "Grace"); err != nil {
		t.Fatal(err)
	}
	fake.AssertSingleShard("UPDATE profiles")

	// A stubbed failure reaches the code under test after the statement was
	// routed, so error handling can be tested too.
	boom := errors.New("connection reset")
	fake.StubError("UPDATE profiles", boom)
	if err := repo.Rename(context.Background(), 42, "Grace"); !errors.Is(err, boom) {
		t.Errorf("error = %v, want the stubbed failure", err)
	}
}

func TestCountByCountryAsksEveryShard(t *testing.T) {
	repo, fake := newRepo(t)
	fake.StubRows("GROUP BY country", []string{"country", "count"}, []any{"UK", int64(3)}, []any{"US", int64(5)})

	got, err := repo.CountByCountry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got["UK"] != 3 || got["US"] != 5 {
		t.Errorf("got %v", got)
	}
	fake.AssertFanout("GROUP BY country", shards...)
}

// A query with no shard key is refused unless the code says WithAllShards. The
// fake refuses it too, so forgetting is caught in a unit test, not in production.
func TestForgettingWithAllShardsIsCaught(t *testing.T) {
	_, fake := newRepo(t)
	_, err := fake.Query(context.Background(), "SELECT country, count(*) FROM profiles GROUP BY country")
	if !errors.Is(err, shard.ErrShardKeyRequired) {
		t.Errorf("error = %v, want ErrShardKeyRequired", err)
	}
}

func TestFindByNameFansOut(t *testing.T) {
	repo, fake := newRepo(t)
	fake.StubRows("WHERE name", []string{"id", "name", "country"}, []any{int64(1), "Ada", "UK"})

	got, err := repo.FindByName(context.Background(), "Ada")
	if err != nil || len(got) != 1 {
		t.Fatalf("got %v, %v", got, err)
	}
	fake.AssertFanout("WHERE name", shards...)

	// The merge the real library would do is part of the plan.
	if steps := fake.LastPlan().Merge; len(steps) == 0 || steps[0] != "OrderedMerge(id)" {
		t.Errorf("merge steps = %q", steps)
	}
}
