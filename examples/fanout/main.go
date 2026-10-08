// Fanout shows reading from every shard and getting one answer back.
//
// A query that names a shard key goes to one shard. A query that does not is
// refused, unless you say WithAllShards: then it runs everywhere and the
// results are merged as one database would give them:
//
//   - ORDER BY, LIMIT and OFFSET (a leaderboard, and its second page);
//   - COUNT, SUM, MIN, MAX and AVG, with GROUP BY and HAVING;
//   - DISTINCT.
//
// It also shows what is refused, and how a shard that does not answer is
// reported instead of failing the query when you ask for that.
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
		registry.Sharded("players", registry.Key("id"), registry.Type(registry.KeyInt)),
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
		"DROP TABLE IF EXISTS players",
		"CREATE TABLE players (id bigint PRIMARY KEY, name text NOT NULL, team text NOT NULL, score int NOT NULL)",
	} {
		if _, err := all.Exec(ctx, q); err != nil {
			return err
		}
	}
	defer func() { _, _ = all.Exec(context.Background(), "DROP TABLE IF EXISTS players") }()

	// 30 players on three teams; each row goes to the shard that owns its id.
	teams := []string{"blue", "green", "red"}
	for id := int64(1); id <= 30; id++ {
		_, err := db.Exec(ctx, "INSERT INTO players (id, name, team, score) VALUES ($1, $2, $3, $4)",
			id, fmt.Sprintf("player-%02d", id), teams[id%3], (id*37)%100)
		if err != nil {
			return err
		}
	}

	fmt.Println("== 1. the five highest scores, from all shards")
	if err := show(ctx, all, "SELECT name, score FROM players ORDER BY score DESC, id LIMIT 5"); err != nil {
		return err
	}

	fmt.Println("\n== 2. the next page")
	if err := show(ctx, all, "SELECT name, score FROM players ORDER BY score DESC, id LIMIT 5 OFFSET 5"); err != nil {
		return err
	}

	fmt.Println("\n== 3. totals per team, busiest first")
	if err := show(ctx, all, `SELECT team, count(*) AS players, sum(score) AS total, avg(score) AS average, max(score) AS best
		FROM players GROUP BY team ORDER BY players DESC, team`); err != nil {
		return err
	}

	fmt.Println("\n== 4. teams whose average is under 50 (HAVING runs after the merge)")
	if err := show(ctx, all, "SELECT team, count(*) FROM players GROUP BY team HAVING avg(score) < 50 ORDER BY team"); err != nil {
		return err
	}

	fmt.Println("\n== 5. distinct teams")
	if err := show(ctx, all, "SELECT DISTINCT team FROM players ORDER BY team"); err != nil {
		return err
	}

	fmt.Println("\n== 6. what is refused")
	_, err = db.Query(ctx, "SELECT name FROM players ORDER BY score DESC LIMIT 3")
	fmt.Printf("  no shard key, no WithAllShards: %v\n", firstLine(err))
	_, err = all.Query(ctx, "SELECT name, rank() OVER (ORDER BY score DESC) FROM players")
	fmt.Printf("  a window function:              %v\n", firstLine(err))
	_, err = all.Query(ctx, "SELECT team, string_agg(name, ',') FROM players GROUP BY team")
	fmt.Printf("  string_agg:                     %v\n", firstLine(err))

	fmt.Println("\n== 7. a shard that does not answer")
	if _, err := db.WithShard("shard-02").Exec(ctx, "DROP TABLE players"); err != nil {
		return err
	}
	_, err = all.Query(ctx, "SELECT count(*) FROM players")
	fmt.Printf("  by default the query fails: %v\n", firstLine(err))
	rows, err := all.Query(shard.AllowPartial(ctx), "SELECT count(*) FROM players")
	if err != nil {
		return err
	}
	defer rows.Close()
	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return err
		}
	}
	fmt.Printf("  with AllowPartial: %d players from the shards that answered\n", n)
	for _, se := range shard.ShardErrors(rows) {
		fmt.Printf("  did not answer: %s\n", firstLine(se))
	}
	return nil
}

// show runs a query and prints its columns and rows.
func show(ctx context.Context, q shard.Querier, sql string) error {
	rows, err := q.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	fmt.Println("  " + strings.Join(cols, "\t"))
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = fmt.Sprint(v)
		}
		fmt.Println("  " + strings.Join(parts, "\t"))
	}
	return rows.Err()
}

// firstLine keeps an error to one line for the output.
func firstLine(err error) string {
	if err == nil {
		return "<nil>"
	}
	line, _, _ := strings.Cut(err.Error(), "\n")
	return line
}
