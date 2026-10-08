package shard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/router"
)

func TestWriteResultHelpers(t *testing.T) {
	boom := errors.New("boom")
	r := WriteResult{PerShard: map[ShardID]ShardOutcome{
		"a": {RowsAffected: 2, Rows: []int{0, 3}},
		"b": {Rows: []int{1, 4}, Err: boom},
		"c": {RowsAffected: 5, Rows: []int{2}, Replayed: true},
		"d": {Rows: []int{5}, Err: boom},
	}}

	if got := r.RowsAffected(); got != 7 {
		t.Errorf("RowsAffected = %d, want 7 (failed shards are not counted, replays are)", got)
	}
	if got := fmt.Sprint(r.FailedRows()); got != "[1 4 5]" {
		t.Errorf("FailedRows = %s, want [1 4 5]: only the rows of failed shards, sorted", got)
	}
	failed := r.Failed()
	if len(failed) != 2 || !errors.Is(failed["b"], boom) || !errors.Is(failed["d"], boom) {
		t.Errorf("Failed = %v", failed)
	}

	ok := WriteResult{PerShard: map[ShardID]ShardOutcome{"a": {RowsAffected: 1}}}
	if ok.Failed() != nil || len(ok.FailedRows()) != 0 {
		t.Error("a fully successful write has nothing failed")
	}
	if (WriteResult{}).Failed() != nil || (WriteResult{}).FailedRows() != nil {
		t.Error("an empty result has nothing failed")
	}
}

func TestIdempotencyKeyInContext(t *testing.T) {
	ctx := context.Background()
	if _, ok := IdempotencyKey(ctx); ok {
		t.Error("a plain context has no key")
	}
	ctx = WithIdempotencyKey(ctx, "order-1")
	if k, ok := IdempotencyKey(ctx); !ok || k != "order-1" {
		t.Errorf("key = %q, %v", k, ok)
	}
	if k, _ := IdempotencyKey(WithIdempotencyKey(ctx, "order-2")); k != "order-2" {
		t.Errorf("an inner key replaces the outer one, got %q", k)
	}
}

func TestEmptyIdempotencyKeyIsAnError(t *testing.T) {
	db := routingDB(t, "a", "b")
	_, err := db.WithShard("a").Exec(WithIdempotencyKey(context.Background(), ""), "UPDATE profiles SET x = 1")
	if err == nil || !strings.Contains(err.Error(), "idempotency key is empty") {
		t.Errorf("error = %v", err)
	}
}

func TestIdempotencyDDLDefaultsTheTable(t *testing.T) {
	if !strings.Contains(IdempotencyDDL(""), `"go_shard_idempotency_keys"`) {
		t.Error("the default table name should be used")
	}
	if !strings.Contains(IdempotencyDDL("app.keys"), `"app"."keys"`) {
		t.Error("a named table should be used")
	}
}

func TestConfigRejectsABadIdempotencyTable(t *testing.T) {
	cfg := Config{Shards: shards("a"), Registry: testRegistry(t), IdempotencyTable: `keys"; DROP TABLE x`}
	if _, err := cfg.validate(); !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "IdempotencyTable") {
		t.Errorf("error = %v", err)
	}
	cfg.IdempotencyTable = "app.keys"
	if _, err := cfg.validate(); err != nil {
		t.Errorf("schema.name should be accepted: %v", err)
	}
}

// ---- splitting a multi-shard INSERT ------------------------------------------

func twoShardPlan() plan.Plan {
	return plan.Plan{
		SQL:     "INSERT ALL",
		Targets: []router.ShardID{"a", "b"},
		Rows:    map[router.ShardID][]int{"a": {0, 2}, "b": {1}},
	}
}

func TestSplitInsertGivesEachShardItsRows(t *testing.T) {
	p := twoShardPlan()
	var asked [][]int
	r := request{split: func(rows []int) (string, []any, error) {
		asked = append(asked, rows)
		return fmt.Sprintf("INSERT rows %v", rows), []any{len(rows)}, nil
	}}
	if err := splitInsert(&p, r); err != nil {
		t.Fatal(err)
	}
	if p.PerShard["a"].SQL != "INSERT rows [0 2]" || p.PerShard["b"].SQL != "INSERT rows [1]" {
		t.Errorf("per-shard statements = %+v", p.PerShard)
	}
	if sql, _ := p.StatementFor("a"); sql != "INSERT rows [0 2]" {
		t.Errorf("StatementFor(a) = %q", sql)
	}
	if len(asked) != 2 {
		t.Errorf("the splitter was asked %d times, want once per shard", len(asked))
	}
}

func TestSplitInsertLeavesOtherPlansAlone(t *testing.T) {
	failing := request{split: func([]int) (string, []any, error) { return "", nil, errors.New("must not be called") }}

	one := plan.Plan{SQL: "x", Targets: []router.ShardID{"a"}, Rows: map[router.ShardID][]int{"a": {0, 1}}}
	if err := splitInsert(&one, failing); err != nil || one.PerShard != nil {
		t.Errorf("one shard needs no split: %v %+v", err, one.PerShard)
	}
	update := plan.Plan{SQL: "x", Targets: []router.ShardID{"a", "b"}}
	if err := splitInsert(&update, failing); err != nil || update.PerShard != nil {
		t.Errorf("a statement without rows is sent unchanged: %v %+v", err, update.PerShard)
	}
}

func TestSplitInsertErrors(t *testing.T) {
	p := twoShardPlan()
	if err := splitInsert(&p, request{}); !errors.Is(err, ErrUnsupportedQuery) || !strings.Contains(err.Error(), "separate calls") {
		t.Errorf("no splitter: error = %v, want ErrUnsupportedQuery saying what to do", err)
	}

	p = twoShardPlan()
	boom := errors.New("cannot rebuild")
	err := splitInsert(&p, request{split: func([]int) (string, []any, error) { return "", nil, boom }})
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "splitting the INSERT for") {
		t.Errorf("error = %v", err)
	}
}

func TestBuiltInsertsAreSplittable(t *testing.T) {
	st, err := query.InsertInto("profiles").Columns("id", "name").Row(1, "a").Row(2, "b").Row(3, "c").Build()
	if err != nil {
		t.Fatal(err)
	}
	r := statementRequest(st)
	if r.split == nil {
		t.Fatal("a built INSERT must be splittable")
	}
	p := plan.Plan{SQL: st.SQL(), Args: st.Args(), Targets: []router.ShardID{"a", "b"},
		Rows: map[router.ShardID][]int{"a": {0, 2}, "b": {1}}}
	if err := splitInsert(&p, r); err != nil {
		t.Fatal(err)
	}
	if got := p.PerShard["a"]; got.SQL != `INSERT INTO "profiles" ("id", "name") VALUES ($1, $2), ($3, $4)` || fmt.Sprint(got.Args) != "[1 a 3 c]" {
		t.Errorf("shard a = %+v", got)
	}
	if got := p.PerShard["b"]; got.SQL != `INSERT INTO "profiles" ("id", "name") VALUES ($1, $2)` || fmt.Sprint(got.Args) != "[2 b]" {
		t.Errorf("shard b = %+v", got)
	}
}

func TestWithoutARowSplitterRawSQLCannotBeSplit(t *testing.T) {
	db := autoDB(t, cannedAnalyzer{}, "a", "b") // cannedAnalyzer does not implement analyze.RowSplitter
	r := (&scoped{db: db}).sqlRequest("INSERT INTO profiles (id) VALUES (1), (2)", nil)
	if r.split != nil {
		t.Error("an analyzer that cannot split gives a request that cannot split")
	}
}
