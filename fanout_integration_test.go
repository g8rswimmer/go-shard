//go:build integration

package shard_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/shardtest"
)

func fanoutRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg, err := registry.New(
		registry.Sharded("events", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Colocated("event_tags", registry.With("events"), registry.Key("event_id")),
		registry.Global("countries"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

const (
	eventsDDL = `CREATE TABLE events (
		id bigint PRIMARY KEY, kind text NOT NULL, region text, score int, amount numeric(10,2) NOT NULL,
		ratio double precision NOT NULL, created timestamptz NOT NULL, note text NOT NULL)`
	tagsDDL = "CREATE TABLE event_tags (event_id bigint NOT NULL, tag text NOT NULL, PRIMARY KEY (event_id, tag))"
)

var (
	// Text values are lower case letters and digits only, so that byte order
	// and any database collation agree.
	kinds   = []string{"alpha", "beta", "delta", "gamma", "omega", "sigma"}
	regions = []string{"east", "north", "south", "west"}
	tagList = []string{"red", "green", "blue", "gold", "teal"}
)

type eventRow struct {
	id      int64
	kind    string
	region  any // string or nil
	score   any // int or nil
	amount  string
	ratio   float64
	created time.Time
	note    string
	tags    []string
}

func makeEvents(rng *rand.Rand, n int) []eventRow {
	ids := rng.Perm(n * 3)[:n]
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]eventRow, n)
	for i, id := range ids {
		r := eventRow{
			id:      int64(id + 1),
			kind:    kinds[rng.Intn(len(kinds))],
			amount:  fmt.Sprintf("%d.%02d", rng.Intn(1000), rng.Intn(100)),
			ratio:   rng.Float64() * 10,
			created: base.Add(time.Duration(rng.Intn(60*24*90)) * time.Minute),
			note:    fmt.Sprintf("n%05d", id),
		}
		if rng.Intn(5) != 0 {
			r.region = regions[rng.Intn(len(regions))]
		}
		if rng.Intn(6) != 0 {
			r.score = rng.Intn(100)
		}
		for _, k := range rng.Perm(len(tagList))[:rng.Intn(4)] {
			r.tags = append(r.tags, tagList[k])
		}
		rows[i] = r
	}
	return rows
}

// combinedDB is one connection to shard 0 whose search_path is a private
// schema, so that ordinary table names resolve to a single database that holds
// every row. It is the reference the sharded answer is compared with.
func combinedDB(t *testing.T, cluster *shardtest.Cluster) *sql.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := cluster.Direct(t, cluster.IDs()[0]).Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// The connection goes back to the pool, so undo what was set on it.
		_, _ = conn.ExecContext(context.Background(), "RESET search_path")
		_ = conn.Close()
	})
	for _, q := range []string{
		"DROP SCHEMA IF EXISTS combined CASCADE", "CREATE SCHEMA combined", "SET search_path = combined",
		eventsDDL, tagsDDL,
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	return conn
}

func TestFanOut(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	db := cluster.Open(t, fanoutRegistry(t))
	cluster.ExecAll(t, eventsDDL)
	cluster.ExecAll(t, tagsDDL)
	rng := rand.New(rand.NewSource(1))
	events := makeEvents(rng, 300)
	reference := combinedDB(t, cluster)
	loadEvents(t, reference, events)
	for _, e := range events {
		_, err := db.WithShardKey(e.id).Exec(ctx,
			"INSERT INTO events (id, kind, region, score, amount, ratio, created, note) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
			e.id, e.kind, e.region, e.score, e.amount, e.ratio, e.created, e.note)
		if err != nil {
			t.Fatal(err)
		}
	}

	read := func(t *testing.T, rows shard.Rows, err error) [][]any {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out, err := readRows(rows)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	t.Run("rows from every shard come back in order", func(t *testing.T) {
		rows, err := db.WithAllShards().Query(ctx, "SELECT id FROM events ORDER BY id LIMIT 20 OFFSET 5")
		got := read(t, rows, err)
		if len(got) != 20 {
			t.Fatalf("%d rows, want 20", len(got))
		}
		want := make([]int64, 0, len(events))
		for _, e := range events {
			want = append(want, e.id)
		}
		sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
		for i, r := range got {
			if r[0] != want[5+i] {
				t.Fatalf("row %d = %v, want %d", i, r[0], want[5+i])
			}
		}
	})

	t.Run("a row query that spans shards by key is merged without WithAllShards", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT id FROM events WHERE id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ORDER BY id DESC")
		got := read(t, rows, err)
		prev := int64(math.MaxInt64)
		for _, r := range got {
			if r[0].(int64) >= prev {
				t.Fatalf("not descending: %v", got)
			}
			prev = r[0].(int64)
		}
	})

	t.Run("aggregates add up across shards", func(t *testing.T) {
		var wantCount, wantSum int64
		for _, e := range events {
			wantCount++
			if s, ok := e.score.(int); ok {
				wantSum += int64(s)
			}
		}
		rows, err := db.WithAllShards().Query(ctx, "SELECT count(*), sum(score), min(id), max(id) FROM events")
		got := read(t, rows, err)
		if len(got) != 1 || got[0][0] != wantCount || got[0][1] != wantSum {
			t.Errorf("got %v, want count %d sum %d", got, wantCount, wantSum)
		}
	})

	t.Run("built statements fan out without a parser", func(t *testing.T) {
		st, err := query.From("events").Columns("id", "kind").OrderBy("kind", query.Desc).OrderBy("id", query.Asc).Limit(7).Offset(2).Build()
		if err != nil {
			t.Fatal(err)
		}
		rows, err := db.WithAllShards().QueryStatement(ctx, st)
		got := read(t, rows, err)
		ref := queryConn(t, reference, "SELECT id, kind FROM events ORDER BY kind DESC, id LIMIT 7 OFFSET 2")
		if len(got) != 7 || len(ref) != 7 {
			t.Fatalf("got %d rows, reference %d", len(got), len(ref))
		}
		for i := range got {
			if got[i][0] != ref[i][0] || got[i][1] != ref[i][1] {
				t.Errorf("row %d = %v, want %v", i, got[i], ref[i])
			}
		}

		// ordering by a column that is not selected, with and without a column list
		st, _ = query.From("events").Columns("id").OrderBy("note", query.Asc).Limit(5).Build()
		rows, err = db.WithAllShards().QueryStatement(ctx, st)
		got = read(t, rows, err)
		ref = queryConn(t, reference, "SELECT id FROM events ORDER BY note LIMIT 5")
		if fmt.Sprint(got) != fmt.Sprint(ref) {
			t.Errorf("hidden order column: got %v, want %v", got, ref)
		}
		st, _ = query.From("events").OrderBy("note", query.Desc).Limit(5).Build()
		rows, err = db.WithAllShards().QueryStatement(ctx, st)
		got = read(t, rows, err)
		ref = queryConn(t, reference, "SELECT * FROM events ORDER BY note DESC LIMIT 5")
		if len(got) != 5 || got[0][0] != ref[0][0] || got[4][0] != ref[4][0] {
			t.Errorf("select * with a hidden order column: got %v, want %v", got, ref)
		}
		if len(got[0]) != 8 {
			t.Errorf("select * returned %d columns, want 8 (the hidden column leaked)", len(got[0]))
		}
	})

	t.Run("a result is scanned into Go types", func(t *testing.T) {
		rows, err := db.WithAllShards().Query(ctx, "SELECT kind, count(*) AS n, avg(score) FROM events GROUP BY kind ORDER BY n DESC, kind LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		if strings.Join(cols, ",") != "kind,n,avg" {
			t.Errorf("columns = %v, want kind,n,avg (hidden columns must not show)", cols)
		}
		if !rows.Next() {
			t.Fatal("no row")
		}
		var kind string
		var n int
		var avg float64
		if err := rows.Scan(&kind, &n, &avg); err != nil {
			t.Fatal(err)
		}
		if kind == "" || n == 0 || avg <= 0 || avg >= 100 {
			t.Errorf("scanned %q %d %v", kind, n, avg)
		}
		if rows.Next() {
			t.Error("LIMIT 1 returned more than one row")
		}
	})

	t.Run("merging stops at MaxMergeRows", func(t *testing.T) {
		cfg := cluster.Config(fanoutRegistry(t))
		cfg.MaxMergeRows = 3
		small, err := shard.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer small.Close()
		if _, err := small.WithAllShards().Query(ctx, "SELECT note, count(*) FROM events GROUP BY note"); !errors.Is(err, shard.ErrMergeLimitExceeded) {
			t.Errorf("GROUP BY over the limit: err = %v, want ErrMergeLimitExceeded", err)
		}
		rows, err := small.WithAllShards().Query(ctx, "SELECT DISTINCT note FROM events")
		if err != nil {
			t.Fatal(err)
		}
		_, err = readRows(rows)
		_ = rows.Close()
		if !errors.Is(err, shard.ErrMergeLimitExceeded) {
			t.Errorf("DISTINCT over the limit: err = %v, want ErrMergeLimitExceeded", err)
		}
		// streaming is not counted: an ordered read of everything is fine
		rows, err = small.WithAllShards().Query(ctx, "SELECT id FROM events ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		got, err := readRows(rows)
		_ = rows.Close()
		if err != nil || len(got) != len(events) {
			t.Errorf("ordered read: %d rows, err %v; want %d rows", len(got), err, len(events))
		}
	})

	t.Run("a shard that fails fails the query, or is reported with AllowPartial", func(t *testing.T) {
		ids := cluster.IDs()
		for _, id := range ids[:2] {
			if _, err := cluster.Direct(t, id).ExecContext(ctx, "CREATE TABLE partial_read (n int)"); err != nil {
				t.Fatal(err)
			}
			if _, err := cluster.Direct(t, id).ExecContext(ctx, "INSERT INTO partial_read VALUES (1), (2)"); err != nil {
				t.Fatal(err)
			}
		}
		// partial_read is missing on ids[2]; it is not in the registry, so route it explicitly
		_, err := db.WithAllShards().Query(ctx, "SELECT count(*) FROM partial_read")
		var se *shard.ShardError
		if !errors.As(err, &se) || se.Shard != ids[2] {
			t.Fatalf("err = %v, want a ShardError for %s", err, ids[2])
		}

		rows, err := db.WithAllShards().Query(shard.AllowPartial(ctx), "SELECT count(*), sum(n) FROM partial_read")
		got := read(t, rows, err)
		if len(got) != 1 || got[0][0] != int64(4) || got[0][1] != int64(6) {
			t.Errorf("partial result = %v, want count 4 sum 6 from the two shards that answered", got)
		}
		failed := shard.ShardErrors(rows)
		if len(failed) != 1 || failed[0].Shard != ids[2] {
			t.Errorf("ShardErrors = %v, want only %s", failed, ids[2])
		}
		if f := shard.ShardErrors(nil); f != nil {
			t.Errorf("ShardErrors(nil) = %v", f)
		}
	})

	t.Run("an unmergeable query is refused before anything runs", func(t *testing.T) {
		for name, q := range map[string]string{
			"window":       "SELECT id, row_number() OVER (ORDER BY id) FROM events",
			"subquery":     "SELECT id FROM events WHERE score > (SELECT avg(score) FROM events)",
			"string_agg":   "SELECT string_agg(kind, ',') FROM events",
			"distinct agg": "SELECT count(DISTINCT kind) FROM events",
			"union":        "SELECT id FROM events UNION SELECT id FROM events",
		} {
			if _, err := db.WithAllShards().Query(ctx, q); !errors.Is(err, shard.ErrUnsupportedQuery) {
				t.Errorf("%s: err = %v, want ErrUnsupportedQuery", name, err)
			}
		}
	})

	t.Run("abandoned rows release their connections", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			rows, err := db.WithAllShards().Query(ctx, "SELECT id FROM events ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			rows.Next()
			_ = rows.Close() // stop after one row
		}
		for _, h := range db.Health(ctx) {
			if h.Err != nil || h.Pool.InUse != 0 {
				t.Errorf("%s: in use %d, err %v: connections were not released", h.ID, h.Pool.InUse, h.Err)
			}
		}
	})
}

// ---- helpers ---------------------------------------------------------------

type rowSet interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	Err() error
}

func readRows(rows rowSet) ([][]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}

func loadEvents(t *testing.T, conn *sql.Conn, events []eventRow) {
	t.Helper()
	ctx := context.Background()
	for _, e := range events {
		if _, err := conn.ExecContext(ctx,
			"INSERT INTO events (id, kind, region, score, amount, ratio, created, note) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
			e.id, e.kind, e.region, e.score, e.amount, e.ratio, e.created, e.note); err != nil {
			t.Fatal(err)
		}
		for _, tag := range e.tags {
			if _, err := conn.ExecContext(ctx, "INSERT INTO event_tags (event_id, tag) VALUES ($1, $2)", e.id, tag); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func queryConn(t *testing.T, conn *sql.Conn, q string, args ...any) [][]any {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), q, args...)
	if err != nil {
		t.Fatalf("reference %s: %v", q, err)
	}
	defer rows.Close()
	out, err := readRows(rows)
	if err != nil {
		t.Fatalf("reference %s: %v", q, err)
	}
	return out
}

// sameValue compares two column values. Floating point and decimal values may
// differ in their last digits, because adding in a different order rounds
// differently, and in how many decimals an average shows.
func sameValue(a, b any) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case float64:
		y, ok := b.(float64)
		return ok && closeEnough(x, y)
	case string:
		y, ok := b.(string)
		if !ok {
			return false
		}
		if x == y {
			return true
		}
		rx, okx := new(big.Rat).SetString(x)
		ry, oky := new(big.Rat).SetString(y)
		if !okx || !oky {
			return false
		}
		fx, _ := rx.Float64()
		fy, _ := ry.Float64()
		return closeEnough(fx, fy)
	case time.Time:
		y, ok := b.(time.Time)
		return ok && x.Equal(y)
	default:
		return a == b
	}
}

func closeEnough(x, y float64) bool {
	return math.Abs(x-y) <= 1e-9*math.Max(1, math.Max(math.Abs(x), math.Abs(y)))
}

// canonical renders a row so that rows that are "the same" render equally.
func canonical(row []any) string {
	parts := make([]string, len(row))
	for i, v := range row {
		switch x := v.(type) {
		case nil:
			parts[i] = "NULL"
		case float64:
			parts[i] = fmt.Sprintf("%.8g", x)
		case string:
			switch r, ok := new(big.Rat).SetString(x); {
			case ok:
				f, _ := r.Float64()
				parts[i] = fmt.Sprintf("%.8g", f)
			default:
				parts[i] = "s:" + x
			}
		case time.Time:
			parts[i] = x.UTC().Format(time.RFC3339Nano)
		default:
			parts[i] = fmt.Sprint(v)
		}
	}
	return strings.Join(parts, "|")
}
