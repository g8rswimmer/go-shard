// Migrations shows applying the same schema to every shard and keeping an eye
// on it:
//
//   - Up applies the migrations to all shards in parallel; a migration
//     file is the same for every shard, so sharded, colocated and global tables
//     (and the data in the global ones) exist everywhere;
//   - Status shows each shard's version and whether the shards have drifted;
//   - when a migration fails on one shard, that shard stays at its last good
//     version, the others carry on or stop (HaltOnFailure / ContinueOnFailure),
//     and fixing the cause and running Up again brings everything level.
//
// It uses the three local shards from `make up`. Set SHARD_DSNS (comma
// separated) to use others. It only touches tables prefixed ex_ and its own
// version table.
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/migrate"
	"github.com/g8rswimmer/go-shard/registry"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const defaultDSNs = "postgres://shard:shard@localhost:5441/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5442/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5443/shard?sslmode=disable"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	reg, err := registry.New(
		registry.Sharded("ex_profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Global("ex_countries"),
	)
	if err != nil {
		return err
	}
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
	db, err := shard.Open(ctx, cfg)
	if err != nil {
		return fmt.Errorf("is `make up` running? %w", err)
	}
	defer db.Close()

	all := db.WithAllShards()
	reset := func() error {
		_, err := all.Exec(ctx, "DROP TABLE IF EXISTS ex_profiles, ex_countries, ex_badges, ex_migrations")
		return err
	}
	if err := reset(); err != nil {
		return err
	}
	defer func() { _ = reset() }()

	// The migrations are embedded in the binary (FromURL("file://./migrations")
	// works too). A table of our own keeps this example away from yours.
	runner, err := db.Migrations(
		migrate.FromFS(migrationFiles, "migrations"),
		migrate.WithTable("ex_migrations"),
		migrate.WithConcurrency(1), // one shard at a time, so the output is the same every run
	)
	if err != nil {
		return err
	}

	fmt.Println("== 1. before anything is applied")
	if err := status(ctx, runner); err != nil {
		return err
	}

	// A table from some other project is in the way of migration 3 on shard-02.
	ids := db.Shards()
	bad := ids[1]
	if _, err := db.WithShard(bad).Exec(ctx, "CREATE TABLE ex_badges (junk int)"); err != nil {
		return err
	}

	fmt.Printf("\n== 2. Up, with %s blocked by a leftover ex_badges table\n", bad)
	res, err := runner.Up(ctx)
	fmt.Printf("  error: %s\n", firstLine(err))
	for _, id := range ids {
		sr := res.PerShard[id]
		switch {
		case sr.Skipped:
			fmt.Printf("  %s: not started (HaltOnFailure)\n", id)
		case sr.Err != nil:
			fmt.Printf("  %s: stayed at %s\n", id, sr.After)
		default:
			fmt.Printf("  %s: %s -> %s\n", id, sr.Before, sr.After)
		}
	}

	fmt.Println("\n== 3. Status shows the drift")
	if err := status(ctx, runner); err != nil {
		return err
	}

	fmt.Printf("\n== 4. fix the cause (drop the leftover table on %s) and run Up again\n", bad)
	if _, err := db.WithShard(bad).Exec(ctx, "DROP TABLE ex_badges"); err != nil {
		return err
	}
	res, err = runner.Up(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		sr := res.PerShard[id]
		switch {
		case sr.Before == sr.After:
			fmt.Printf("  %s: already at %s\n", id, sr.After)
		default:
			fmt.Printf("  %s: %s -> %s\n", id, sr.Before, sr.After)
		}
	}
	if err := status(ctx, runner); err != nil {
		return err
	}

	// ex_countries is global, so migration 2 loaded it on every shard.
	fmt.Println("\n== 5. the global table has its rows on every shard")
	for _, id := range ids {
		rows, err := db.WithShard(id).Query(ctx, "SELECT count(*) FROM ex_countries")
		if err != nil {
			return err
		}
		var n int
		if rows.Next() {
			err = rows.Scan(&n)
		}
		rows.Close()
		if err != nil {
			return err
		}
		fmt.Printf("  %s: %d countries\n", id, n)
	}

	fmt.Println("\n== 6. a runner that cannot reach a shard says which")
	bogus, err := migrate.New([]migrate.Shard{
		{ID: "shard-01", DSN: cfg.Shards[0].DSN},
		{ID: "shard-99", DSN: "postgres://shard:shard@localhost:1/shard?sslmode=disable&connect_timeout=2"},
	}, migrate.FromFS(migrationFiles, "migrations"), migrate.WithTable("ex_migrations"))
	if err != nil {
		return err
	}
	st, err := bogus.Status(ctx)
	var se *migrate.ShardError
	if errors.As(err, &se) {
		fmt.Printf("  unreadable: %s (drift: %t)\n", se.Shard, st.Drift())
	}
	return nil
}

func status(ctx context.Context, r *migrate.Runner) error {
	st, err := r.Status(ctx)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSpace(st.String()), "\n") {
		fmt.Println("  " + line)
	}
	return nil
}

// firstLine keeps an error to one line for the output.
func firstLine(err error) string {
	if err == nil {
		return "<nil>"
	}
	line, _, _ := strings.Cut(err.Error(), "\n")
	return line
}
