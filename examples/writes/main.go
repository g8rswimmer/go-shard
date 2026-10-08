// Writes shows how go-shard handles writing to several shards:
//
//   - one INSERT with many rows is split so each shard receives only its own;
//   - when a shard fails, the others still commit, and the result says exactly
//     which rows were lost;
//   - an idempotency key makes the retry safe: shards that already applied the
//     write do not apply it again.
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
	"github.com/g8rswimmer/go-shard/query"
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
		registry.Sharded("items", registry.Key("id"), registry.Type(registry.KeyInt)),
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

	// Set up a clean table on every shard, and the table that records
	// idempotency keys. (Use your migrations for this in a real program.)
	all := db.WithAllShards()
	for _, q := range []string{
		"DROP TABLE IF EXISTS items, items_away",
		"CREATE TABLE items (id bigint PRIMARY KEY, label text NOT NULL)",
	} {
		if _, err := all.Exec(ctx, q); err != nil {
			return err
		}
	}
	if err := db.EnsureIdempotencyTable(ctx); err != nil {
		return err
	}
	defer func() { // leave the shards as we found them
		_, _ = all.Exec(context.Background(), "DROP TABLE IF EXISTS items, items_away")
		_, _ = all.Exec(context.Background(), "DROP TABLE IF EXISTS go_shard_idempotency_keys")
	}()

	// 1. One INSERT, many rows, several shards. The library reads the shard
	// key of each row and sends every shard only its own rows.
	fmt.Println("== 1. a batch is split across shards")
	res, err := db.ExecStatement(ctx, batch(1, 12))
	if err != nil {
		return err
	}
	show(res)

	// 2. A shard fails. Here we break shard-02 by renaming its table; in real
	// life it could be an outage, a full disk or a failover in progress. The
	// other shards commit their rows anyway: there is no atomicity across
	// shards, so the result must tell you what happened.
	fmt.Println("\n== 2. one shard fails, the others commit")
	if _, err := db.WithShard("shard-02").Exec(ctx, "ALTER TABLE items RENAME TO items_away"); err != nil {
		return err
	}
	// The key identifies this operation. Reuse it to retry.
	ctx2 := shard.WithIdempotencyKey(ctx, "load-batch-2")
	res, err = db.ExecStatement(ctx2, batch(101, 112))
	var shardErr *shard.ShardError
	if !errors.As(err, &shardErr) {
		return fmt.Errorf("expected a shard to fail, got %v", err)
	}
	show(res)
	fmt.Printf("failed on %s: rows %v were not written\n", shardErr.Shard, res.FailedRows())

	// 3. Repair the shard and retry with the same key and the same statement.
	// Shards that already applied it do not apply it again; the failed one does.
	fmt.Println("\n== 3. retry with the same idempotency key")
	if _, err := db.WithShard("shard-02").Exec(ctx, "ALTER TABLE items_away RENAME TO items"); err != nil {
		return err
	}
	res, err = db.ExecStatement(ctx2, batch(101, 112))
	if err != nil {
		return err
	}
	show(res)
	fmt.Printf("total rows reported: %d (all 12, none written twice)\n", res.RowsAffected())

	// 4. Without a key, repeating a batch that partly succeeded would fail on
	// the rows that were already written.
	fmt.Println("\n== 4. the same retry without a key")
	_, err = db.ExecStatement(ctx, batch(101, 112))
	fmt.Printf("error: %v\n", firstLine(err))
	return nil
}

// batch builds one INSERT of rows with ids from..to.
func batch(from, to int64) shard.Statement {
	b := query.InsertInto("items").Columns("id", "label")
	for id := from; id <= to; id++ {
		b.Row(id, fmt.Sprintf("item-%d", id))
	}
	st, err := b.Build()
	if err != nil {
		log.Fatal(err)
	}
	return st
}

// show prints what each shard did, in shard order.
func show(res shard.WriteResult) {
	ids := make([]shard.ShardID, 0, len(res.PerShard))
	for id := range res.PerShard {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		o := res.PerShard[id]
		switch {
		case o.Err != nil:
			fmt.Printf("  %s: FAILED (rows %v): %v\n", id, o.Rows, firstLine(o.Err))
		case o.Replayed:
			fmt.Printf("  %s: already applied, %d rows (rows %v)\n", id, o.RowsAffected, o.Rows)
		default:
			fmt.Printf("  %s: wrote %d rows (rows %v)\n", id, o.RowsAffected, o.Rows)
		}
	}
}

func firstLine(err error) string {
	if err == nil {
		return "<nil>"
	}
	line, _, _ := strings.Cut(err.Error(), "\n")
	return line
}
