package shard

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata/explain")

// explainRegistry is the example domain: profiles with colocated addresses,
// plus a global table.
func explainRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
		registry.Global("countries"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func explainPlanner(t testing.TB) *Planner {
	t.Helper()
	cfg := Config{Registry: explainRegistry(t)}
	for _, id := range []ShardID{"shard-01", "shard-02", "shard-03"} {
		cfg.Shards = append(cfg.Shards, ShardConfig{ID: id})
	}
	p, err := NewPlanner(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// golden compares got with testdata/explain/<name>.golden; -update rewrites it.
func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", "explain", name+".golden")
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run the tests with -update to create it)", err)
	}
	if got != string(want) {
		t.Errorf("Explain output differs from %s (run with -update if the change is intended):\n--- got\n%s--- want\n%s", path, got, want)
	}
}

// Built statements carry their own analysis, so these run without a parser.
func TestExplainBuiltStatements(t *testing.T) {
	p := explainPlanner(t)
	ctx := context.Background()

	build := func(b interface {
		Build() (query.Statement, error)
	}) query.Statement {
		st, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	tests := []struct {
		name string
		st   query.Statement
		via  func() Explainer
	}{
		{"built_single", build(query.From("profiles").Columns("id", "name").Where(query.Eq("id", 42))), nil},
		{"built_in_two_keys", build(query.From("profiles").Where(query.In("id", 1, 2, 3, 4, 5)).OrderBy("name", query.Asc)), nil},
		{
			"built_all_ordered_page",
			build(query.From("profiles").Columns("id", "name").OrderBy("created_at", query.Desc).OrderBy("id", query.Asc).Limit(20).Offset(40)),
			func() Explainer { return p.WithAllShards() },
		},
		{
			"built_all_star_hidden_order",
			build(query.From("profiles").OrderBy("created_at", query.Desc).Limit(5)),
			func() Explainer { return p.WithAllShards() },
		},
		{
			"built_update_all_shards",
			build(query.Update("profiles").Set("name", "x").Where(query.Eq("name", "y"))),
			func() Explainer { return p.WithAllShards() },
		},
		{"built_global_read", build(query.From("countries").Where(query.Eq("code", "US"))), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ex Explainer = p
			if tc.via != nil {
				ex = tc.via()
			}
			e, err := ex.ExplainStatement(ctx, tc.st)
			if err != nil {
				t.Fatal(err)
			}
			golden(t, tc.name, e.String())
		})
	}
}

// Explain refuses what running the statement would refuse.
func TestExplainReturnsTheErrorsOfRunning(t *testing.T) {
	p := explainPlanner(t)
	ctx := context.Background()

	st, err := query.From("profiles").Where(query.Eq("name", "x")).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.ExplainStatement(ctx, st); !errors.Is(err, ErrShardKeyRequired) {
		t.Errorf("no key: error = %v, want ErrShardKeyRequired", err)
	}
	if _, err := p.WithShard("nope").ExplainStatement(ctx, st); !errors.Is(err, ErrUnknownShard) {
		t.Errorf("unknown shard: error = %v, want ErrUnknownShard", err)
	}
	if _, err := p.WithShardKey(struct{}{}).ExplainStatement(ctx, st); err == nil {
		t.Error("an unroutable key should fail")
	}
}

func TestExplainFields(t *testing.T) {
	p := explainPlanner(t)
	st, err := query.From("profiles").Columns("id").OrderBy("name", query.Desc).Limit(3).Build()
	if err != nil {
		t.Fatal(err)
	}
	e, err := p.WithAllShards().ExplainStatement(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case e.Strategy != All || len(e.Targets) != 3:
		t.Errorf("strategy %v targets %v, want all of 3", e.Strategy, e.Targets)
	case e.Op != "SELECT":
		t.Errorf("Op = %q, want SELECT", e.Op)
	case e.ShardSQL != `SELECT "id", "name" FROM "profiles" ORDER BY "name" DESC LIMIT 3`:
		t.Errorf("ShardSQL = %s", e.ShardSQL)
	case len(e.Merge) != 3 || !strings.HasPrefix(e.Merge[0], "OrderedMerge(name DESC)"):
		t.Errorf("Merge = %q", e.Merge)
	case e.ShardPlans != nil:
		t.Errorf("ShardPlans = %v, want none unless asked for", e.ShardPlans)
	default:
		// as expected
	}
}

// A Planner connects to nothing, so it cannot ask shards for their plans.
func TestPlannerCannotAskShardsForPlans(t *testing.T) {
	p := explainPlanner(t)
	st, err := query.From("profiles").Where(query.Eq("id", 1)).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Explainer(ShardPlans()).ExplainStatement(context.Background(), st); err == nil {
		t.Error("a Planner has no shards to ask")
	}
}

func TestNewPlannerValidatesLikeOpen(t *testing.T) {
	if _, err := NewPlanner(Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("empty config: error = %v, want ErrInvalidConfig", err)
	}
	reg := explainRegistry(t)
	if _, err := NewPlanner(Config{Registry: reg, Shards: []ShardConfig{{ID: "a"}, {ID: "a"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("duplicate shard: error = %v, want ErrInvalidConfig", err)
	}
	if _, err := Open(context.Background(), Config{Registry: reg, Shards: []ShardConfig{{ID: "a"}}}); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("Open still requires a DSN: error = %v", err)
	}
}

func TestExplainStringWithShardPlansAndPerShardSQL(t *testing.T) {
	e := Explain{
		Op: "INSERT", Strategy: Multi, Targets: []ShardID{"a", "b"}, Reason: "rows belong to a and b",
		PerShard: map[ShardID]string{"b": "INSERT ... ($1)", "a": "INSERT ... ($1), ($2)"},
		Rows:     map[ShardID][]int{"a": {0, 2}, "b": {1}},
		Notes:    []string{"careful"},
		ShardPlans: map[ShardID]string{
			"a": "Insert on profiles\n  -> Result",
		},
	}
	golden(t, "string_per_shard", e.String())
}

// Inside a transaction, Explain checks the statement against the transaction's
// shard exactly as running it would.
func TestTxExplain(t *testing.T) {
	ctx := context.Background()
	db := autoDB(t, nil, "a", "b", "c")
	tx := txOn(db, "b")
	mine, other := keyOwnedBy(t, db, "b"), keyOwnedBy(t, db, "c")

	st, err := query.From("profiles").Where(query.Eq("id", mine)).Build()
	if err != nil {
		t.Fatal(err)
	}
	e, err := tx.ExplainStatement(ctx, st)
	if err != nil || e.Strategy != Single || len(e.Targets) != 1 || e.Targets[0] != "b" {
		t.Fatalf("explain of its own shard = %+v, %v", e, err)
	}

	st, err = query.From("profiles").Where(query.Eq("id", other)).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExplainStatement(ctx, st); !errors.Is(err, ErrCrossShardTx) {
		t.Errorf("explain of another shard: error = %v, want ErrCrossShardTx", err)
	}

	// ANALYZE would run the statement inside the transaction.
	st, _ = query.From("profiles").Where(query.Eq("id", mine)).Build()
	if _, err := tx.Explainer(Analyze()).ExplainStatement(ctx, st); !errors.Is(err, ErrUnsupportedQuery) {
		t.Errorf("analyze in a transaction: error = %v, want ErrUnsupportedQuery", err)
	}

	u, err := tx.Unchecked().ExplainStatement(ctx, st)
	if err != nil || u.Targets[0] != "b" || !strings.Contains(u.Reason, "Unchecked") {
		t.Errorf("unchecked explain = %+v, %v", u, err)
	}
}

func TestAnalyzeImpliesShardPlans(t *testing.T) {
	switch c := newExplainConfig(nil); {
	case c.shardPlans || c.analyze:
		t.Fatal("no options ask for nothing")
	default:
		// as documented
	}
	switch c := newExplainConfig([]ExplainOption{ShardPlans()}); {
	case !c.shardPlans || c.analyze:
		t.Error("ShardPlans asks for plans, not ANALYZE")
	default:
		// as documented
	}
	switch c := newExplainConfig([]ExplainOption{Analyze()}); {
	case !c.shardPlans || !c.analyze:
		t.Error("Analyze asks for plans and ANALYZE")
	default:
		// as documented
	}
}

func TestExplainerOptionsReachEveryQuerier(t *testing.T) {
	p := explainPlanner(t)
	st, err := query.From("profiles").Where(query.Eq("id", 1)).Build()
	if err != nil {
		t.Fatal(err)
	}
	// Each of these has no shard to ask, so the option must be what fails.
	for name, ex := range map[string]Explainer{
		"planner":     p.Explainer(Analyze()),
		"planner key": p.WithShardKey(1).Explainer(ShardPlans()),
		"planner all": p.WithAllShards().Explainer(ShardPlans()),
	} {
		if _, err := ex.ExplainStatement(context.Background(), st); err == nil {
			t.Errorf("%s: option was ignored", name)
		}
	}
	if _, err := p.Explainer().ExplainStatement(context.Background(), st); err != nil {
		t.Errorf("no options is plain Explain: %v", err)
	}
}
