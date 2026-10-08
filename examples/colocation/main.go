// Colocation shows why related tables are kept on one shard, and what that
// buys you: a profile and its addresses can be written in one transaction, and
// read back with an ordinary join.
//
//   - addresses are colocated with profiles, so an address always lives on its
//     profile's shard;
//   - a transaction covers one shard, so profile and addresses commit or roll
//     back together;
//   - a statement for a different shard is refused inside the transaction.
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

	// addresses are colocated with profiles: their shard key is the profile's id.
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
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
		"DROP TABLE IF EXISTS addresses, profiles",
		"CREATE TABLE profiles (id bigint PRIMARY KEY, name text NOT NULL)",
		"CREATE TABLE addresses (id bigint PRIMARY KEY, profile_id bigint NOT NULL, city text NOT NULL)",
	} {
		if _, err := all.Exec(ctx, q); err != nil {
			return err
		}
	}
	defer func() { _, _ = all.Exec(context.Background(), "DROP TABLE IF EXISTS addresses, profiles") }()

	// 1. A profile and its addresses, in one transaction. ForTable picks the
	// shard that owns profile 7; the addresses are colocated, so they belong
	// there too.
	fmt.Println("== 1. a profile and its addresses commit together")
	err = db.InTx(ctx, shard.ForTable("profiles", 7), func(tx shard.Tx) error {
		fmt.Printf("  transaction on %s\n", tx.Shard())
		if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES (7, 'Ada')"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO addresses (id, profile_id, city) VALUES (70, 7, 'London'), (71, 7, 'Paris')")
		return err
	})
	if err != nil {
		return err
	}

	// 2. Because they share a shard, an ordinary join finds both, with no
	// scatter-gather: the shard key in the WHERE clause is all it needs.
	fmt.Println("\n== 2. read them back with a join")
	rows, err := db.Query(ctx, `SELECT p.name, a.city FROM profiles p
		JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1 ORDER BY a.city`, 7)
	if err != nil {
		return err
	}
	for rows.Next() {
		var name, city string
		if err := rows.Scan(&name, &city); err != nil {
			rows.Close()
			return err
		}
		fmt.Printf("  %s lives in %s\n", name, city)
	}
	rows.Close()

	// 3. If any step fails, nothing is kept. Here the second address repeats
	// an id, so the transaction rolls back the profile too.
	fmt.Println("\n== 3. a failure rolls everything back")
	err = db.InTx(ctx, shard.ForTable("profiles", 8), func(tx shard.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES (8, 'Grace')"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO addresses (id, profile_id, city) VALUES (80, 8, 'NYC'), (80, 8, 'DC')")
		return err
	})
	fmt.Printf("  the transaction failed: %v\n", firstLine(err))
	fmt.Printf("  profiles named Grace afterwards: %d\n", count(ctx, db, "SELECT count(*) FROM profiles WHERE id = $1", 8))

	// 4. A transaction covers one shard. A statement that belongs to another
	// shard is refused before anything is sent, and the transaction carries on.
	fmt.Println("\n== 4. a statement for another shard is refused")
	stray, err := idOnAnotherShard(ctx, db, 9)
	if err != nil {
		return err
	}
	err = db.InTx(ctx, shard.ForTable("profiles", 9), func(tx shard.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES (9, 'Barbara')"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO profiles (id, name) VALUES ($1, 'Stray')", stray)
		if !errors.Is(err, shard.ErrCrossShardTx) {
			return fmt.Errorf("expected the stray insert to be refused, got %v", err)
		}
		fmt.Printf("  refused: %v\n", firstLine(err))
		return nil // carry on and commit the rest
	})
	if err != nil {
		return err
	}
	fmt.Printf("  Barbara was saved: %d, the stray was not: %d\n",
		count(ctx, db, "SELECT count(*) FROM profiles WHERE id = $1", 9),
		count(ctx, db, "SELECT count(*) FROM profiles WHERE id = $1", stray))
	return nil
}

// count runs a count(*) query that names a shard key.
func count(ctx context.Context, db *shard.DB, sql string, id any) int {
	rows, err := db.Query(ctx, sql, id)
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

// idOnAnotherShard finds an id that does not live on the same shard as id.
func idOnAnotherShard(ctx context.Context, db *shard.DB, id int) (int, error) {
	shardOf := func(k int) (shard.ShardID, error) {
		tx, err := db.Begin(ctx, shard.ForTable("profiles", k))
		if err != nil {
			return "", err
		}
		defer func() { _ = tx.Rollback() }()
		return tx.Shard(), nil
	}
	home, err := shardOf(id)
	if err != nil {
		return 0, err
	}
	for k := id + 1; k < id+1000; k++ {
		if s, err := shardOf(k); err != nil || s != home {
			return k, err
		}
	}
	return 0, errors.New("no id on another shard found")
}

func firstLine(err error) string {
	if err == nil {
		return "<nil>"
	}
	line, _, _ := strings.Cut(err.Error(), "\n")
	return line
}
