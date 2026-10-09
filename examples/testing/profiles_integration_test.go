//go:build integration

package profiles_test

import (
	"context"
	"testing"

	profiles "github.com/g8rswimmer/go-shard/examples/testing"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/shardtest"
)

// The same repository against real PostgreSQL shards. shardtest.NewCluster
// starts one container per shard (Docker is required; set SHARDTEST_DSNS to use
// databases you already run, such as the ones from `make up`).
func TestRepoAgainstRealShards(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	cluster.ExecAll(t, "CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL, country text NOT NULL)")

	reg, err := registry.New(registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)))
	if err != nil {
		t.Fatal(err)
	}
	db := cluster.Open(t, reg)
	for id, c := range map[int64]string{1: "UK", 2: "US", 3: "US", 4: "FR", 5: "US"} {
		cluster.Seed(t, db, "profiles", map[string]any{"id": id, "name": "p", "country": c})
	}
	repo := profiles.Repo{DB: db}

	got, err := repo.Get(ctx, 3)
	if err != nil || got.ID != 3 {
		t.Fatalf("Get = %+v, %v", got, err)
	}
	counts, err := repo.CountByCountry(ctx)
	if err != nil || counts["US"] != 3 || counts["UK"] != 1 {
		t.Errorf("counts = %v, %v", counts, err)
	}
	if err := repo.Rename(ctx, 3, "renamed"); err != nil {
		t.Fatal(err)
	}
	found, err := repo.FindByName(ctx, "renamed")
	if err != nil || len(found) != 1 || found[0].ID != 3 {
		t.Errorf("FindByName = %v, %v", found, err)
	}

	// Where the row physically is: the shard the fake would have named.
	owner, err := db.Explain(ctx, "SELECT * FROM profiles WHERE id = $1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if n := cluster.Count(t, owner.Targets[0], "SELECT count(*) FROM profiles WHERE id = 3"); n != 1 {
		t.Errorf("profile 3 is not on %s", owner.Targets[0])
	}
}
