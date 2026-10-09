//go:build cgo

package shard

import (
	"context"
	"errors"
	"testing"
)

// Raw SQL goes through the parser: these are the Explain outputs a reader of
// the docs sees for a single-shard, multi-shard and all-shard query, a split
// INSERT and a global write.
func TestExplainRawSQL(t *testing.T) {
	p := explainPlanner(t)
	ctx := context.Background()

	tests := []struct {
		name string
		via  func() Explainer
		sql  string
		args []any
	}{
		{"sql_single_by_key", nil, "SELECT name FROM profiles WHERE id = $1", []any{42}},
		{"sql_colocated_join", nil, "SELECT p.name, a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1", []any{42}},
		{"sql_multi_any", nil, "SELECT id, name FROM profiles WHERE id = ANY($1) ORDER BY name LIMIT 10", []any{[]int{1, 2, 3, 4, 5}}},
		{
			"sql_all_grouped", func() Explainer { return p.WithAllShards() },
			"SELECT country, count(*) AS n, avg(age) FROM profiles GROUP BY country HAVING count(*) > 10 ORDER BY 2 DESC, country LIMIT 5", nil,
		},
		{"sql_all_distinct", func() Explainer { return p.WithAllShards() }, "SELECT DISTINCT country FROM profiles ORDER BY country", nil},
		{"sql_all_count", func() Explainer { return p.WithAllShards() }, "SELECT count(*) FROM profiles", nil},
		{
			"sql_insert_split", nil,
			"INSERT INTO profiles (id, name) VALUES ($1, 'a'), ($2, 'b'), ($3, 'c'), ($4, 'd')", []any{1, 2, 3, 4},
		},
		{"sql_global_write", nil, "INSERT INTO countries (code) VALUES ('US')", nil},
		{"sql_ddl_on_all", func() Explainer { return p.WithAllShards() }, "CREATE INDEX profiles_name ON profiles (name)", nil},
		{"sql_override_by_key", func() Explainer { return p.WithShardKey(7) }, "UPDATE profiles SET name = 'x' WHERE id = 7", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ex Explainer = p
			if tc.via != nil {
				ex = tc.via()
			}
			e, err := ex.Explain(ctx, tc.sql, tc.args...)
			if err != nil {
				t.Fatal(err)
			}
			golden(t, tc.name, e.String())
		})
	}
}

func TestExplainRefusesWhatRunningRefuses(t *testing.T) {
	p := explainPlanner(t)
	ctx := context.Background()
	tests := []struct {
		name string
		via  func() Explainer
		sql  string
		want error
	}{
		{"no shard key", nil, "SELECT * FROM profiles WHERE name = 'x'", ErrShardKeyRequired},
		{"cross-shard join", nil, "SELECT * FROM profiles p JOIN profiles q ON q.name = p.name", ErrCrossShardJoin},
		{"unknown table", nil, "SELECT * FROM nowhere WHERE id = 1", ErrUnknownTable},
		{"window function on all shards", func() Explainer { return p.WithAllShards() }, "SELECT id, rank() OVER (ORDER BY age) FROM profiles", ErrUnsupportedQuery},
		{"unmergeable aggregate", func() Explainer { return p.WithAllShards() }, "SELECT string_agg(name, ',') FROM profiles", ErrUnsupportedQuery},
		{"shard key update", nil, "UPDATE profiles SET id = 2 WHERE id = 1", ErrShardKeyImmutable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ex Explainer = p
			if tc.via != nil {
				ex = tc.via()
			}
			if _, err := ex.Explain(ctx, tc.sql); !errors.Is(err, tc.want) {
				t.Errorf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

// An override does not stop the statement from being read, so Explain can still
// say what kind it is and whether rows would be merged.
func TestExplainOverrideStillReadsTheStatement(t *testing.T) {
	p := explainPlanner(t)
	e, err := p.WithAllShards().Explain(context.Background(), "SELECT id FROM profiles ORDER BY id LIMIT 3")
	if err != nil {
		t.Fatal(err)
	}
	if e.Op != "SELECT" || len(e.Merge) == 0 {
		t.Errorf("Op %q Merge %q: want a SELECT with merge steps", e.Op, e.Merge)
	}
}
