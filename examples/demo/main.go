// Demo is the command-line companion of docs/DEMO.md: one subcommand per
// scenario, each printing what the library did so you can check it by eye.
//
//	go run ./examples/demo <command>
//
// Run it without a command to list them. It uses the three local shards from
// `make up` (set SHARD_DSNS, comma separated, to use others) and the tables
// profiles, addresses and countries, plus demo_migrations and the idempotency
// table. `make demo-setup` creates them and `make demo-reset` starts over.
//
// Everything printed is the same on every run, so the guide's expected output
// can be checked by CI.
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/migrate"
	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const defaultDSNs = "postgres://shard:shard@localhost:5441/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5442/shard?sslmode=disable," +
	"postgres://shard:shard@localhost:5443/shard?sslmode=disable"

const (
	profiles      = 30 // ids 1..30 are the dataset
	setupVersion  = 2  // migrations setup applies; 3 is left for the migration scenario
	longNameID    = 9001
	batchFrom     = 201
	batchTo       = 212
	stoppedShard  = "shard-02"
	migrationsTbl = "demo_migrations"
)

var commands = []struct {
	name, args, help string
	run              func(context.Context, []string) error
}{
	{"setup", "", "migrate to version 2 and load the dataset (make demo-setup)", setup},
	{"reset", "", "drop everything and set up again (make demo-reset)", reset},
	{"status", "", "migration versions and row counts per shard", status},
	{"where", "[id]", "which shard a key lives on: Explain, then look at every shard", where},
	{"colocated", "[id]", "a profile and its addresses are on the same shard", colocated},
	{"fanout", "", "an all-shards query: sorted, limited and aggregated", fanout},
	{"nokey", "", "a query with no shard key is refused", nokey},
	{"tx", "", "a transaction rolls back; another shard is refused", tx},
	{"batch", "", "a batch write while shard-02 is stopped (uses docker compose)", batch},
	{"migrate-break", "", "put a row on one shard that migration 3 cannot accept", migrateBreak},
	{"migrate", "", "apply migration 3: fails on one shard, shows the drift", migrateUp},
	{"migrate-fix", "", "remove that row and run the migration again", migrateFix},
	{"health", "", "ping every shard and show connection pools", health},
	{"logs", "", "the structured log for a few statements", logs},
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	for _, c := range commands {
		if c.name != os.Args[1] {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		err := c.run(ctx, os.Args[2:])
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
	usage()
	os.Exit(2)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: go run ./examples/demo <command>")
	for _, c := range commands {
		fmt.Fprintf(os.Stderr, "  %-14s %-5s %s\n", c.name, c.args, c.help)
	}
}

func dsns() []string {
	s := os.Getenv("SHARD_DSNS")
	if s == "" {
		s = defaultDSNs
	}
	var out []string
	for _, d := range strings.Split(s, ",") {
		out = append(out, strings.TrimSpace(d))
	}
	return out
}

func open(ctx context.Context, hooks observe.Hooks) (*shard.DB, error) {
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
		registry.Global("countries"),
	)
	if err != nil {
		return nil, err
	}
	cfg := shard.Config{Registry: reg, Hooks: hooks}
	for i, dsn := range dsns() {
		cfg.Shards = append(cfg.Shards, shard.ShardConfig{
			ID:  shard.ShardID(fmt.Sprintf("shard-%02d", i+1)),
			DSN: dsn,
		})
	}
	db, err := shard.Open(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("is `make up` running? %w", err)
	}
	return db, nil
}

func runner(db *shard.DB, opts ...migrate.Option) (*migrate.Runner, error) {
	opts = append(opts, migrate.WithTable(migrationsTbl), migrate.WithConcurrency(1)) // one at a time: same output every run
	return db.Migrations(migrate.FromFS(migrationFiles, "migrations"), opts...)
}

// setup and reset

func setup(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return load(ctx, db)
}

func load(ctx context.Context, db *shard.DB) error {
	r, err := runner(db)
	if err != nil {
		return err
	}
	if _, err := r.UpTo(ctx, setupVersion); err != nil {
		return err
	}
	if err := db.EnsureIdempotencyTable(ctx); err != nil {
		return err
	}

	// Rows are routed like any other write: each goes to the shard that owns its id.
	p := query.InsertInto("profiles").Columns("id", "name", "tier", "score").OnConflictDoNothing()
	a := query.InsertInto("addresses").Columns("id", "profile_id", "city").OnConflictDoNothing()
	tiers := []string{"bronze", "silver", "gold"}
	cities := []string{"Lisbon", "Auckland", "Boston", "Porto"}
	for id := int64(1); id <= profiles; id++ {
		p.Row(id, fmt.Sprintf("profile-%02d", id), tiers[id%3], (id*37)%100)
		a.Row(id*10, id, cities[id%4])
		a.Row(id*10+1, id, cities[(id+1)%4])
	}
	for _, b := range []*query.InsertBuilder{p, a} {
		st, err := b.Build()
		if err != nil {
			return err
		}
		if _, err := db.ExecStatement(ctx, st); err != nil {
			return err
		}
	}
	fmt.Printf("ready: migrated %d shards to version %d, %d profiles loaded\n", len(db.Shards()), setupVersion, profiles)
	return nil
}

func reset(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.WithAllShards().Exec(ctx,
		"DROP TABLE IF EXISTS profiles, addresses, countries, "+migrationsTbl+", go_shard_idempotency_keys"); err != nil {
		return err
	}
	return load(ctx, db)
}

// scenarios

func status(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return printStatus(ctx, db)
}

func printStatus(ctx context.Context, db *shard.DB) error {
	r, err := runner(db)
	if err != nil {
		return err
	}
	st, err := r.Status(ctx)
	if err != nil {
		return err
	}
	fmt.Print(indent(st.String()))
	fmt.Println("rows:")
	for _, id := range db.Shards() {
		fmt.Printf("  %s: %d profiles, %d addresses\n", id,
			count(ctx, db.WithShard(id), "SELECT count(*) FROM profiles"),
			count(ctx, db.WithShard(id), "SELECT count(*) FROM addresses"))
	}
	return nil
}

func where(ctx context.Context, args []string) error {
	id, err := idArg(args, 7)
	if err != nil {
		return err
	}
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	const sql = "SELECT name, tier, score FROM profiles WHERE id = $1"
	fmt.Printf("== Explain: %s  [id = %d]\n", sql, id)
	e, err := db.Explain(ctx, sql, id)
	if err != nil {
		return err
	}
	fmt.Print(indent(e.String()))

	fmt.Println("== the row")
	if err := show(ctx, db, sql, id); err != nil {
		return err
	}

	fmt.Printf("== profiles with id %d, counted on every shard directly\n", id)
	for _, sid := range db.Shards() {
		fmt.Printf("  %s: %d\n", sid,
			count(ctx, db.WithShard(sid), "SELECT count(*) FROM profiles WHERE id = $1", id))
	}
	return nil
}

func colocated(ctx context.Context, args []string) error {
	id, err := idArg(args, 7)
	if err != nil {
		return err
	}
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	fmt.Printf("== profile %d and its addresses, on every shard\n", id)
	for _, sid := range db.Shards() {
		s := db.WithShard(sid)
		fmt.Printf("  %s: %d profile, %d addresses\n", sid,
			count(ctx, s, "SELECT count(*) FROM profiles WHERE id = $1", id),
			count(ctx, s, "SELECT count(*) FROM addresses WHERE profile_id = $1", id))
	}

	const sql = "SELECT p.name, a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1 ORDER BY a.city"
	fmt.Println("== a join of the two, with the shard key in WHERE")
	e, err := db.Explain(ctx, sql, id)
	if err != nil {
		return err
	}
	fmt.Print(indent(e.String()))
	return show(ctx, db, sql, id)
}

func fanout(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	all := db.WithAllShards()

	for _, q := range []struct{ title, sql string }{
		{"the 5 highest scores", "SELECT id, name, score FROM profiles ORDER BY score DESC, id LIMIT 5"},
		{"per tier: count, sum, min and max of the score",
			"SELECT tier, count(*), sum(score), min(score), max(score) FROM profiles GROUP BY tier ORDER BY tier"},
		{"everyone", "SELECT count(*), sum(score) FROM profiles"},
	} {
		fmt.Printf("== %s\n", q.title)
		e, err := all.Explain(ctx, q.sql)
		if err != nil {
			return err
		}
		fmt.Printf("  strategy: %s on %d shards\n", e.Strategy, len(e.Targets))
		for i, step := range e.Merge {
			fmt.Printf("  merge %d: %s\n", i+1, step)
		}
		if err := show(ctx, all, q.sql); err != nil {
			return err
		}
	}
	return nil
}

func nokey(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	const sql = "SELECT id, name FROM profiles WHERE tier = 'gold'"
	fmt.Printf("== %s\n", sql)
	_, err = db.Query(ctx, sql)
	if !errors.Is(err, shard.ErrShardKeyRequired) {
		return fmt.Errorf("expected ErrShardKeyRequired, got %v", err)
	}
	fmt.Printf("  refused: %v\n", firstLine(err))
	fmt.Println("== the same query, asking for every shard on purpose")
	fmt.Printf("  gold profiles: %d\n", count(ctx, db.WithAllShards(), "SELECT count(*) FROM profiles WHERE tier = 'gold'"))
	return nil
}

func tx(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	const id = 1001
	fmt.Println("== a profile and its addresses in one transaction, then a failure")
	err = db.InTx(ctx, shard.ForTable("profiles", id), func(t shard.Tx) error {
		fmt.Printf("  transaction on %s\n", t.Shard())
		if _, err := t.Exec(ctx, "INSERT INTO profiles (id, name, tier, score) VALUES ($1, 'temp', 'bronze', 0)", id); err != nil {
			return err
		}
		// The second address repeats an id, so the statement fails.
		_, err := t.Exec(ctx, "INSERT INTO addresses (id, profile_id, city) VALUES (10010, $1, 'Lisbon'), (10010, $1, 'Porto')", id)
		return err
	})
	fmt.Printf("  failed: %v\n", firstLine(err))
	fmt.Printf("  profiles with id %d afterwards: %d\n", id, count(ctx, db, "SELECT count(*) FROM profiles WHERE id = $1", id))

	fmt.Println("== a statement for another shard is refused")
	other, err := idOnAnotherShard(ctx, db, id)
	if err != nil {
		return err
	}
	err = db.InTx(ctx, shard.ForTable("profiles", id), func(t shard.Tx) error {
		_, err := t.Exec(ctx, "INSERT INTO profiles (id, name, tier, score) VALUES ($1, 'stray', 'bronze', 0)", other)
		if !errors.Is(err, shard.ErrCrossShardTx) {
			return fmt.Errorf("expected ErrCrossShardTx, got %v", err)
		}
		fmt.Printf("  refused: %v\n", firstLine(err))
		return err // roll back
	})
	if !errors.Is(err, shard.ErrCrossShardTx) {
		return err
	}
	fmt.Printf("  profiles with id %d afterwards: %d\n", other, count(ctx, db, "SELECT count(*) FROM profiles WHERE id = $1", other))
	return nil
}

func batch(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	defer func() { // leave the dataset as it was
		_, _ = db.WithAllShards().Exec(context.Background(), "DELETE FROM profiles WHERE id >= $1 AND id <= $2", batchFrom, batchTo)
	}()

	b := query.InsertInto("profiles").Columns("id", "name", "tier", "score")
	for id := int64(batchFrom); id <= batchTo; id++ {
		b.Row(id, fmt.Sprintf("batch-%d", id), "bronze", 1)
	}
	st, err := b.Build()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("demo-batch-%d", time.Now().UnixNano())

	fmt.Printf("== stopping %s (docker compose stop %[1]s)\n", stoppedShard)
	if out, err := exec.Command("docker", "compose", "stop", stoppedShard).CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose stop: %v\n%s", err, out)
	}
	defer func() { _ = exec.Command("docker", "compose", "start", stoppedShard).Run() }()

	fmt.Printf("== one INSERT of %d profiles, ids %d to %d\n", batchTo-batchFrom+1, batchFrom, batchTo)
	res, err := db.ExecStatement(shard.WithIdempotencyKey(ctx, key), st)
	var se *shard.ShardError
	if !errors.As(err, &se) {
		return fmt.Errorf("expected a shard to fail, got %v", err)
	}
	showWrite(res)
	fmt.Printf("  failed on %s: rows %v were not written\n", se.Shard, res.FailedRows())

	fmt.Printf("== starting %s again\n", stoppedShard)
	if out, err := exec.Command("docker", "compose", "start", stoppedShard).CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose start: %v\n%s", err, out)
	}
	if err := waitHealthy(ctx, db); err != nil {
		return err
	}

	fmt.Println("== the same statement and key again")
	res, err = db.ExecStatement(shard.WithIdempotencyKey(ctx, key), st)
	if err != nil {
		return err
	}
	showWrite(res)
	n := count(ctx, db.WithAllShards(), "SELECT count(*) FROM profiles WHERE id >= $1 AND id <= $2", batchFrom, batchTo)
	fmt.Printf("  profiles %d to %d on all shards: %d\n", batchFrom, batchTo, n)
	return nil
}

// bad names the shard that holds the over-long name; migration 3 fails there.
func migrateBreak(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	name := strings.Repeat("x", 40)
	res, err := db.Exec(ctx, "INSERT INTO profiles (id, name, tier, score) VALUES ($1, $2, 'bronze', 0)", longNameID, name)
	if err != nil {
		return err
	}
	for id := range res.PerShard {
		fmt.Printf("profile %d, with a %d-character name, is on %s\n", longNameID, len(name), id)
	}
	return nil
}

func migrateUp(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return migrateAll(ctx, db)
}

func migrateAll(ctx context.Context, db *shard.DB) error {
	r, err := runner(db, migrate.WithPolicy(migrate.ContinueOnFailure))
	if err != nil {
		return err
	}
	res, err := r.Up(ctx)
	fmt.Println("== Up")
	for _, id := range db.Shards() {
		sr := res.PerShard[id]
		switch {
		case sr.Err != nil:
			fmt.Printf("  %s: FAILED, stayed at %s\n", id, sr.After)
		case sr.Before == sr.After:
			fmt.Printf("  %s: already at %s\n", id, sr.After)
		default:
			fmt.Printf("  %s: %s -> %s\n", id, sr.Before, sr.After)
		}
	}
	fmt.Println("== status")
	st, serr := r.Status(ctx)
	if serr != nil {
		return serr
	}
	fmt.Print(indent(st.String()))
	if err != nil {
		fmt.Printf("  Up returned: %v\n", firstLine(err))
	}
	return nil
}

func migrateFix(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "DELETE FROM profiles WHERE id = $1", longNameID); err != nil {
		return err
	}
	fmt.Printf("removed profile %d\n", longNameID)
	return migrateAll(ctx, db)
}

func health(ctx context.Context, _ []string) error {
	db, err := open(ctx, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	stats := db.PoolStats()
	for _, h := range db.Health(ctx) {
		fmt.Printf("%s: healthy=%t, connection limit %d\n", h.ID, h.Healthy(), stats[h.ID].MaxOpenConnections)
	}
	return nil
}

func logs(ctx context.Context, _ []string) error {
	// No times or durations, so the output is the same every run.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey || a.Key == "duration" {
				return slog.Attr{}
			}
			return a
		},
	}))
	// Per-shard lines for the first statement only: the shards of a fan-out
	// finish in any order, which would change the output between runs.
	detailed, err := open(ctx, observe.Slog(logger, observe.WithShardEvents()))
	if err != nil {
		return err
	}
	defer detailed.Close()
	db, err := open(ctx, observe.Slog(logger))
	if err != nil {
		return err
	}
	defer db.Close()

	_ = drain(detailed.Query(ctx, "SELECT name FROM profiles WHERE id = $1", 7))
	_ = drain(db.WithAllShards().Query(ctx, "SELECT id FROM profiles ORDER BY id LIMIT 3"))
	_ = drain(db.Query(ctx, "SELECT name FROM profiles WHERE tier = 'gold'"))
	return nil
}

// helpers

func idArg(args []string, def int64) (int64, error) {
	if len(args) == 0 {
		return def, nil
	}
	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("the id must be a number: %w", err)
	}
	return id, nil
}

func count(ctx context.Context, q shard.Querier, sql string, args ...any) int {
	rows, err := q.Query(ctx, sql, args...)
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

func drain(rows shard.Rows, err error) error {
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

// show prints the columns and rows of a query.
func show(ctx context.Context, q shard.Querier, sql string, args ...any) error {
	rows, err := q.Query(ctx, sql, args...)
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

func showWrite(res shard.WriteResult) {
	ids := make([]shard.ShardID, 0, len(res.PerShard))
	for id := range res.PerShard {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		o := res.PerShard[id]
		switch {
		case o.Err != nil:
			fmt.Printf("  %s: FAILED (rows %v)\n", id, o.Rows)
		case o.Replayed:
			fmt.Printf("  %s: already applied, %d rows (rows %v)\n", id, o.RowsAffected, o.Rows)
		default:
			fmt.Printf("  %s: wrote %d rows (rows %v)\n", id, o.RowsAffected, o.Rows)
		}
	}
}

// waitHealthy waits for every shard to answer after a restart.
func waitHealthy(ctx context.Context, db *shard.DB) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, h := range db.Health(ctx) {
			ok = ok && h.Healthy()
		}
		if ok {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return errors.New("a shard did not come back within 30s")
}

// idOnAnotherShard finds an id that does not live on the same shard as id.
func idOnAnotherShard(ctx context.Context, db *shard.DB, id int) (int, error) {
	shardOf := func(k int) (shard.ShardID, error) {
		t, err := db.Begin(ctx, shard.ForTable("profiles", k))
		if err != nil {
			return "", err
		}
		defer func() { _ = t.Rollback() }()
		return t.Shard(), nil
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

func indent(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("  " + line + "\n")
	}
	return b.String()
}

func firstLine(err error) string {
	if err == nil {
		return "<nil>"
	}
	line, _, _ := strings.Cut(err.Error(), "\n")
	return line
}
