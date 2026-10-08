package shard

import (
	"errors"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
)

// A built query.Statement is a Statement.
var _ Statement = query.Statement{}

// failingAnalyzer proves that something was not parsed.
type failingAnalyzer struct{ called *int }

func (f failingAnalyzer) FromSQL(string, []any) (analyze.Analysis, error) {
	*f.called++
	return analyze.Analysis{}, errors.New("the analyzer must not be called here")
}

// cannedAnalyzer returns a fixed Analysis, standing in for a custom parser.
type cannedAnalyzer struct{ a analyze.Analysis }

func (c cannedAnalyzer) FromSQL(string, []any) (analyze.Analysis, error) { return c.a, nil }

func autoDB(t *testing.T, an analyze.Analyzer, ids ...ShardID) *DB {
	t.Helper()
	db := routingDB(t, ids...)
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Global("countries"),
	)
	if err != nil {
		t.Fatal(err)
	}
	db.registry = reg
	db.analyzer = an
	return db
}

func TestBuiltStatementsAreNotParsed(t *testing.T) {
	calls := 0
	db := autoDB(t, failingAnalyzer{&calls}, "a", "b", "c")

	st, err := query.From("profiles").Where(query.Eq("id", 42)).Build()
	if err != nil {
		t.Fatal(err)
	}
	s := &scoped{db: db}
	p, err := s.plan(statementRequest(st))
	if err != nil {
		t.Fatal(err)
	}
	want, _ := db.router.ShardFor(42)
	if len(p.Targets) != 1 || p.Targets[0] != want {
		t.Errorf("targets = %v, want [%s]", p.Targets, want)
	}
	if calls != 0 {
		t.Errorf("the analyzer was called %d times for a built statement", calls)
	}
	if p.SQL != st.SQL() || len(p.Args) != 1 {
		t.Errorf("plan lost the SQL or arguments: %+v", p)
	}
}

func TestExplicitRoutesSkipAnalysis(t *testing.T) {
	calls := 0
	db := autoDB(t, failingAnalyzer{&calls}, "a", "b")
	for _, q := range []Querier{db.WithShardKey(1), db.WithShard("a"), db.WithAllShards()} {
		// This SQL could not be routed automatically (and would fail to analyze).
		if _, err := q.(*scoped).plan((&scoped{db: db}).sqlRequest("WITH x AS (SELECT 1) SELECT * FROM x", nil)); err != nil {
			t.Errorf("explicit route failed: %v", err)
		}
	}
	if calls != 0 {
		t.Errorf("the analyzer was called %d times although the caller named the shard", calls)
	}
}

func TestCustomAnalyzerDecidesTheRoute(t *testing.T) {
	// A custom analyzer saying "profiles WHERE id = 7".
	an := cannedAnalyzer{analyze.Analysis{
		Op:       analyze.OpSelect,
		Scopes:   []int{-1},
		Tables:   []analyze.TableRef{{ID: 0, Name: "profiles"}},
		Bindings: []analyze.Binding{{Column: analyze.ColumnRef{Name: "id"}, Values: []any{7}}},
	}}
	db := autoDB(t, an, "a", "b", "c")
	p, err := (&scoped{db: db}).plan((&scoped{db: db}).sqlRequest("anything", nil))
	if err != nil {
		t.Fatal(err)
	}
	want, _ := db.router.ShardFor(7)
	if len(p.Targets) != 1 || p.Targets[0] != want {
		t.Errorf("targets = %v, want [%s]", p.Targets, want)
	}
}

func TestAnalyzerErrorsAreReturned(t *testing.T) {
	calls := 0
	db := autoDB(t, failingAnalyzer{&calls}, "a")
	_, err := (&scoped{db: db}).plan((&scoped{db: db}).sqlRequest("SELECT 1", nil))
	if err == nil || !strings.Contains(err.Error(), "must not be called") {
		t.Errorf("error = %v, want the analyzer's error", err)
	}
}

func TestWithoutAnAnalyzerRawSQLNeedsARoute(t *testing.T) {
	db := autoDB(t, nil, "a", "b")
	_, err := (&scoped{db: db}).plan((&scoped{db: db}).sqlRequest("SELECT * FROM profiles WHERE id = 1", nil))
	if !errors.Is(err, ErrShardKeyRequired) {
		t.Fatalf("error = %v, want ErrShardKeyRequired", err)
	}
	for _, w := range []string{"cgo", "WithShardKey", "package query", "Config.Analyzer"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("error should mention %q: %v", w, err)
		}
	}
}

func TestAnyShardTakesTurns(t *testing.T) {
	db := autoDB(t, nil, "a", "b", "c")
	var got []ShardID
	for i := 0; i < 7; i++ {
		got = append(got, db.anyShard())
	}
	want := []ShardID{"a", "b", "c", "a", "b", "c", "a"}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("shards chosen = %v, want %v", got, want)
		}
	}
}

func TestSentinelsAreTheLowerLevelErrors(t *testing.T) {
	// errors.Is works whichever package an error came from.
	for name, pair := range map[string][2]error{
		"ErrShardKeyRequired": {ErrShardKeyRequired, plan.ErrShardKeyRequired},
		"ErrCrossShardJoin":   {ErrCrossShardJoin, plan.ErrCrossShardJoin},
		"ErrUnknownTable":     {ErrUnknownTable, plan.ErrUnknownTable},
		"ErrUnsupportedQuery": {ErrUnsupportedQuery, analyze.ErrUnsupportedQuery},
		"ErrMissingArgument":  {ErrMissingArgument, analyze.ErrMissingArgument},
	} {
		if !errors.Is(pair[0], pair[1]) {
			t.Errorf("%s must be the same value the lower package returns", name)
		}
	}
}
