// Explain shows where a statement will run, and why, without running it.
//
// db.Explain routes a statement with exactly the rules Query and Exec use, so
// it is the way to check a query before it ships:
//
//   - a query by shard key runs on one shard;
//   - a query naming several keys runs on those shards, and is merged;
//   - a query with WithAllShards runs everywhere, and the output lists the
//     merge steps and the SQL each shard receives;
//   - a statement that cannot be placed returns the error running it would.
//
// The ShardPlans option adds PostgreSQL's own EXPLAIN from each shard (is the
// index used?). Analyze runs the statement to measure it, inside a transaction
// that is rolled back.
//
// It uses the three local shards from `make up`. Set SHARD_DSNS (comma
// separated) to use others.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
		registry.Global("countries"),
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
	for _, q := range []string{
		"DROP TABLE IF EXISTS profiles, addresses, countries",
		"CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL, country text NOT NULL, age int NOT NULL)",
		"CREATE TABLE addresses (id bigint PRIMARY KEY, profile_id bigint NOT NULL, city text NOT NULL)",
		"CREATE TABLE countries (code text PRIMARY KEY)",
	} {
		if _, err := all.Exec(ctx, q); err != nil {
			return err
		}
	}
	defer func() { _, _ = all.Exec(context.Background(), "DROP TABLE IF EXISTS profiles, addresses, countries") }()

	for id := int64(1); id <= 30; id++ {
		if _, err := db.Exec(ctx, "INSERT INTO profiles (id, name, country, age) VALUES ($1, $2, $3, $4)",
			id, fmt.Sprintf("profile-%02d", id), []string{"CA", "MX", "US"}[id%3], 20+id); err != nil {
			return err
		}
	}

	steps := []struct {
		title string
		q     shard.Querier
		sql   string
		args  []any
	}{
		{"1. one key: one shard", db, "SELECT name FROM profiles WHERE id = $1", []any{42}},
		{"2. a profile and its addresses: still one shard", db,
			"SELECT p.name, a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1", []any{42}},
		{"3. several keys: those shards, merged", db,
			"SELECT id, name FROM profiles WHERE id IN (1, 2) ORDER BY name LIMIT 3", nil},
		{"4. every shard: the merge steps", all,
			`SELECT country, count(*) AS n, avg(age) FROM profiles
			 GROUP BY country HAVING count(*) > 5 ORDER BY n DESC LIMIT 2`, nil},
		{"5. a batch insert is split by shard", db,
			"INSERT INTO profiles (id, name, country, age) VALUES (101, 'a', 'US', 30), (102, 'b', 'CA', 31), (103, 'c', 'MX', 32)", nil},
		{"6. a global table is written everywhere", db, "INSERT INTO countries (code) VALUES ('US')", nil},
		{"7. refused: no shard key", db, "SELECT * FROM profiles WHERE name = 'x'", nil},
		{"8. refused: a window function on all shards", all, "SELECT id, rank() OVER (ORDER BY age) FROM profiles", nil},
	}
	for _, s := range steps {
		fmt.Printf("== %s\n  %s\n", s.title, strings.Join(strings.Fields(s.sql), " "))
		e, err := s.q.Explain(ctx, s.sql, s.args...)
		switch {
		case err != nil:
			fmt.Printf("  refused: %s\n", firstLine(err))
		default:
			fmt.Println(indent(e.String()))
		}
		fmt.Println()
	}

	// What PostgreSQL itself will do on each shard.
	fmt.Println("== 9. ShardPlans: PostgreSQL's plan on the shard that owns the key")
	e, err := db.Explainer(shard.ShardPlans()).Explain(ctx, "SELECT name FROM profiles WHERE id = $1", 42)
	if err != nil {
		return err
	}
	for _, id := range e.Targets {
		fmt.Printf("  %s:\n%s\n", id, indent(indent(e.ShardPlans[id])))
	}

	// ANALYZE really runs the statement; for a write it is rolled back.
	fmt.Println("\n== 10. Analyze on a DELETE: measured on every shard, then rolled back")
	e, err = all.Explainer(shard.Analyze()).Explain(ctx, "DELETE FROM profiles WHERE age > 40")
	if err != nil {
		return err
	}
	for _, id := range e.Targets {
		plan := e.ShardPlans[id]
		node, _, _ := strings.Cut(plan, "  (")
		fmt.Printf("  %s: %s, measured: %t\n", id, node, strings.Contains(plan, "actual time"))
	}
	var n int64
	rows, err := all.Query(ctx, "SELECT count(*) FROM profiles")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return err
		}
	}
	fmt.Printf("  profiles after the ANALYZE: %d (nothing was deleted)\n", n)
	return nil
}

// indent offsets every line of s by two spaces.
func indent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = "  " + l
	}
	return strings.Join(lines, "\n")
}

// firstLine keeps an error to one line for the output.
func firstLine(err error) string {
	line, _, _ := strings.Cut(err.Error(), "\n")
	return line
}
