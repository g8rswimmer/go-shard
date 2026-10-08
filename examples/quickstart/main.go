// Quickstart shows the basics: describe your tables, open the shards, write a
// row to the shard that owns its key, then read and update it with plain SQL.
// The library finds the shard from the shard key in the SQL.
//
// It uses the three local shards from `make up`. Set SHARD_DSNS (comma
// separated) to use others.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/registry"
)

const defaultDSNs = "postgres://shard:shard@localhost:5441/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5442/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5443/shard?sslmode=disable"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Describe your tables: profiles are spread across shards by id.
	// The key type is required: it makes "42" and 42 find the same shard.
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
	)
	if err != nil {
		return err
	}

	// 2. Name the shards. Buckets are split evenly in the order listed.
	dsns := os.Getenv("SHARD_DSNS")
	if dsns == "" {
		dsns = defaultDSNs
	}
	cfg := shard.Config{Registry: reg}
	for i, dsn := range strings.Split(dsns, ",") {
		cfg.Shards = append(cfg.Shards, shard.ShardConfig{
			ID:  shard.ShardID(fmt.Sprintf("shard-%02d", i+1)),
			DSN: strings.TrimSpace(dsn),
		})
	}

	// 3. Open. This connects to every shard and fails if any is unreachable.
	db, err := shard.Open(ctx, cfg)
	if err != nil {
		return fmt.Errorf("is `make up` running? %w", err)
	}
	defer db.Close()

	// Schema setup here is by hand; the migrations example shows how to apply
	// it to every shard properly.
	if _, err := db.WithAllShards().Exec(ctx, "CREATE TABLE IF NOT EXISTS profiles (id bigint PRIMARY KEY, name text NOT NULL)"); err != nil {
		return err
	}
	defer func() { // best-effort cleanup so the example can be re-run
		_, _ = db.WithAllShards().Exec(context.Background(), "DROP TABLE IF EXISTS profiles")
	}()

	// 4. Write: WithShardKey picks the shard that owns this id. (Routing an
	// INSERT from its VALUES is not available yet.)
	names := map[int64]string{1: "Ada", 2: "Grace", 3: "Edsger", 4: "Barbara", 5: "Alan", 6: "Margaret"}
	ids := make([]int64, 0, len(names))
	for id := range names {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		res, err := db.WithShardKey(id).Exec(ctx,
			"INSERT INTO profiles (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
			id, names[id])
		if err != nil {
			return err
		}
		for sid := range res.PerShard { // exactly one shard
			fmt.Printf("wrote profile %d (%s) to %s\n", id, names[id], sid)
		}
	}

	// 5. Read and update with plain SQL: the library reads the shard key from
	// the WHERE clause and runs the statement on the shard that owns it.
	rows, err := db.Query(ctx, "SELECT name FROM profiles WHERE id = $1", int64(3))
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		fmt.Printf("read profile 3: %s\n", name)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	res, err := db.Exec(ctx, "UPDATE profiles SET name = $2 WHERE id = $1", int64(3), "Edsger W. Dijkstra")
	if err != nil {
		return err
	}
	for sid, o := range res.PerShard {
		fmt.Printf("renamed profile 3 on %s (%d row)\n", sid, o.RowsAffected)
	}

	// 6. A statement that does not say which shard it belongs to is refused
	// rather than guessed. The error says how to fix it.
	_, err = db.Query(ctx, "SELECT name FROM profiles WHERE name = $1", "Ada")
	if errors.Is(err, shard.ErrShardKeyRequired) {
		fmt.Printf("refused: %v\n", err)
		return nil
	}
	return fmt.Errorf("expected the keyless query to be refused, got %v", err)
}
