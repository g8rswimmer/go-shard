package shard

import (
	"context"
	"errors"
	"testing"

	"github.com/g8rswimmer/go-shard/migrate"
)

// DB.Migrations migrates the shards in the Config, with the DSNs given there.
func TestDBMigrationsUsesTheConfiguredShards(t *testing.T) {
	cfg := Config{Registry: explainRegistry(t)}
	for _, id := range []ShardID{"shard-01", "shard-02"} {
		cfg.Shards = append(cfg.Shards, ShardConfig{ID: id, DSN: "postgres://" + string(id)})
	}
	p, err := NewPlanner(cfg)
	if err != nil {
		t.Fatal(err)
	}

	var seen []migrate.Shard
	factory := func(_ context.Context, sh migrate.Shard, _ migrate.Config) (migrate.Migrator, error) {
		seen = append(seen, sh)
		return nil, errors.New("stop here")
	}
	r, err := p.db.Migrations(migrate.FromURL("file://x"), migrate.WithFactory(factory), migrate.WithConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Up(context.Background()); !errors.Is(err, migrate.ErrFailed) {
		t.Fatalf("error = %v", err)
	}
	// Halt on failure: only the first shard is tried.
	if len(seen) != 1 || seen[0].ID != "shard-01" || seen[0].DSN != "postgres://shard-01" {
		t.Errorf("shards opened = %+v", seen)
	}
}
