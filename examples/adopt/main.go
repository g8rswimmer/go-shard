// Adopt shows one way to move the rows of an existing, unsharded database into
// shards (see docs/ADOPTING.md):
//
//   - the old database stays the source; the loader reads it in batches by key
//     and writes through the library, so every row goes to the shard that owns
//     its key and addresses follow their profile (colocation);
//   - parents are loaded before children, and every insert is ON CONFLICT DO
//     NOTHING, so a loader that stops can simply be run again;
//   - afterwards it checks what the library cannot assume: the row counts
//     match, and every row sits on the shard the router chooses for its key.
//
// The "old database" here is a schema called ex_legacy in the first shard's
// database. It uses the three local shards from `make up`. Set SHARD_DSNS
// (comma separated) to use others. It only touches ex_legacy and the tables
// ex_profiles and ex_addresses, and drops them when it finishes.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	// Registers the "pgx" driver for database/sql.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
)

const defaultDSNs = "postgres://shard:shard@localhost:5441/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5442/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5443/shard?sslmode=disable"

const batchSize = 20

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
		registry.Colocated("ex_addresses", registry.With("ex_profiles"), registry.Key("profile_id")),
	)
	if err != nil {
		return err
	}
	dsns := os.Getenv("SHARD_DSNS")
	if dsns == "" {
		dsns = defaultDSNs
	}
	cfg := shard.Config{Registry: reg}
	list := strings.Split(dsns, ",")
	for i, dsn := range list {
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

	// 1. The old database, with some data in it.
	src, err := sql.Open("pgx", strings.TrimSpace(list[0]))
	if err != nil {
		return err
	}
	defer src.Close()
	cleanup := func() {
		_, _ = src.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS ex_legacy CASCADE")
		_, _ = db.WithAllShards().Exec(context.Background(), "DROP TABLE IF EXISTS ex_addresses, ex_profiles")
	}
	cleanup()
	defer cleanup()
	for _, q := range []string{
		"CREATE SCHEMA ex_legacy",
		"CREATE TABLE ex_legacy.profiles (id bigint PRIMARY KEY, name text NOT NULL)",
		"CREATE TABLE ex_legacy.addresses (id bigint PRIMARY KEY, profile_id bigint NOT NULL, city text NOT NULL)",
		"INSERT INTO ex_legacy.profiles SELECT n, 'profile-' || n FROM generate_series(1, 100) n",
		"INSERT INTO ex_legacy.addresses SELECT n * 10 + k, n, 'city-' || k FROM generate_series(1, 100) n, generate_series(0, 1) k",
	} {
		if _, err := src.ExecContext(ctx, q); err != nil {
			return err
		}
	}

	// 2. The new schema, on every shard (use your migrations for this).
	for _, q := range []string{
		"CREATE TABLE ex_profiles (id bigint PRIMARY KEY, name text NOT NULL)",
		"CREATE TABLE ex_addresses (id bigint PRIMARY KEY, profile_id bigint NOT NULL, city text NOT NULL)",
	} {
		if _, err := db.WithAllShards().Exec(ctx, q); err != nil {
			return err
		}
	}

	// 3. Load. Parents first; each batch is read by key order, so a stopped
	// loader could resume from the last key it wrote.
	fmt.Println("== 1. load, parents before children")
	for _, t := range []struct{ name, columns, from, to, key string }{
		{"profiles", "id, name", "ex_legacy.profiles", "ex_profiles", "id"},
		{"addresses", "id, profile_id, city", "ex_legacy.addresses", "ex_addresses", "id"},
	} {
		n, err := load(ctx, src, db, t.from, t.to, t.columns, t.key)
		if err != nil {
			return err
		}
		fmt.Printf("  %s: %d rows copied\n", t.name, n)
	}

	// 4. Running it again changes nothing.
	fmt.Println("\n== 2. run it again: nothing is written twice")
	if _, err := load(ctx, src, db, "ex_legacy.profiles", "ex_profiles", "id, name", "id"); err != nil {
		return err
	}
	fmt.Printf("  profiles after a second run: %d\n", total(ctx, db, "SELECT count(*) FROM ex_profiles"))

	// 5. Check the result.
	fmt.Println("\n== 3. verify")
	for _, t := range []struct{ name, source, target string }{
		{"profiles", "ex_legacy.profiles", "ex_profiles"},
		{"addresses", "ex_legacy.addresses", "ex_addresses"},
	} {
		var want int
		if err := src.QueryRowContext(ctx, "SELECT count(*) FROM "+t.source).Scan(&want); err != nil {
			return err
		}
		got := total(ctx, db, "SELECT count(*) FROM "+t.target)
		fmt.Printf("  %s: %d in the old database, %d on the shards\n", t.name, want, got)
		if want != got {
			return fmt.Errorf("%s: counts differ", t.name)
		}
	}
	misplaced, err := misplacedRows(ctx, db)
	if err != nil {
		return err
	}
	fmt.Printf("  profiles on a shard other than the one the router chooses: %d\n", misplaced)
	// The subquery must see the profile on the same shard, so ask each shard on
	// its own (a subquery across shards is refused).
	orphans := 0
	for _, sid := range db.Shards() {
		orphans += scalar(ctx, db.WithShard(sid), `SELECT count(*) FROM ex_addresses a
			WHERE NOT EXISTS (SELECT 1 FROM ex_profiles p WHERE p.id = a.profile_id)`)
	}
	fmt.Printf("  addresses without their profile on the same shard: %d\n", orphans)
	if misplaced != 0 || orphans != 0 {
		return fmt.Errorf("the load is not consistent")
	}
	return nil
}

// load copies from.* to to in key order, batchSize rows at a time. All columns
// are bigint or text, so values are passed through as read.
func load(ctx context.Context, src *sql.DB, db *shard.DB, from, to, columns, key string) (int, error) {
	cols := strings.Split(columns, ", ")
	copied := 0
	var last int64
	for {
		rows, err := src.QueryContext(ctx,
			fmt.Sprintf("SELECT %s FROM %s WHERE %s > $1 ORDER BY %s LIMIT %d", columns, from, key, key, batchSize), last)
		if err != nil {
			return copied, err
		}
		b := query.InsertInto(to).Columns(cols...).OnConflictDoNothing()
		n := 0
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return copied, err
			}
			b.Row(vals...)
			last, _ = vals[0].(int64) // the key is the first column
			n++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return copied, err
		}
		if n == 0 {
			return copied, nil
		}
		st, err := b.Build()
		if err != nil {
			return copied, err
		}
		if _, err := db.ExecStatement(ctx, st); err != nil {
			return copied, fmt.Errorf("batch ending at %s %d: %w (run the loader again)", key, last, err)
		}
		copied += n
	}
}

// total runs a count(*) on every shard and adds them up.
func total(ctx context.Context, db *shard.DB, sql string) int {
	return scalar(ctx, db.WithAllShards(), sql)
}

func scalar(ctx context.Context, q shard.Querier, sql string) int {
	rows, err := q.Query(ctx, sql)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var n int
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			log.Fatal(err)
		}
	}
	return n
}

// misplacedRows asks each shard for its profile ids and checks that the router
// would send each id back to that shard. For a large table, check a sample.
func misplacedRows(ctx context.Context, db *shard.DB) (int, error) {
	bad := 0
	for _, sid := range db.Shards() {
		rows, err := db.WithShard(sid).Query(ctx, "SELECT id FROM ex_profiles")
		if err != nil {
			return 0, err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return 0, err
			}
			ids = append(ids, id)
		}
		rows.Close()
		for _, id := range ids {
			e, err := db.Explain(ctx, "SELECT id FROM ex_profiles WHERE id = $1", id)
			if err != nil {
				return 0, err
			}
			if len(e.Targets) != 1 || e.Targets[0] != sid {
				bad++
			}
		}
	}
	return bad, nil
}
