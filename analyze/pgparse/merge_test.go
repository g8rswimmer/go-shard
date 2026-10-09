//go:build cgo

package pgparse

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/merge"
)

func lim(n int64) *int64 { return &n }

func TestPlanMerge(t *testing.T) {
	asc := func(col int) merge.Order { return merge.Order{Col: col} }

	tests := []struct {
		name    string
		sql     string
		args    []any
		wantSQL string
		want    merge.Spec
		wantArg []any
	}{
		{
			name:    "order, limit and offset: shards get offset+limit, merge applies the real ones",
			sql:     "SELECT id, name FROM profiles ORDER BY name DESC LIMIT 5 OFFSET 10",
			wantSQL: "SELECT id, name FROM profiles ORDER BY name DESC LIMIT 15",
			want:    merge.Spec{Order: []merge.Order{{Col: 1, Desc: true, NullsFirst: true}}, Offset: 10, Limit: lim(5)},
		},
		{
			name:    "limit and offset from parameters are resolved, and unused parameters renumbered away",
			sql:     "SELECT id FROM profiles WHERE age > $1 ORDER BY id LIMIT $2 OFFSET $3",
			args:    []any{18, int64(7), int64(2)},
			wantSQL: "SELECT id FROM profiles WHERE age > $1 ORDER BY id LIMIT 9",
			wantArg: []any{18},
			want:    merge.Spec{Order: []merge.Order{asc(0)}, Offset: 2, Limit: lim(7)},
		},
		{
			name:    "offset alone is removed from the shards",
			sql:     "SELECT id FROM profiles ORDER BY id OFFSET 4",
			wantSQL: "SELECT id FROM profiles ORDER BY id",
			want:    merge.Spec{Order: []merge.Order{asc(0)}, Offset: 4},
		},
		{
			name:    "no order: rows are concatenated",
			sql:     "SELECT id FROM profiles WHERE age > 3",
			wantSQL: "SELECT id FROM profiles WHERE age > 3",
			want:    merge.Spec{},
		},
		{
			name:    "an order column that is not selected becomes a hidden column",
			sql:     "SELECT name FROM profiles ORDER BY id",
			wantSQL: "SELECT name, id FROM profiles ORDER BY id",
			want:    merge.Spec{Hidden: 1, Order: []merge.Order{asc(1)}},
		},
		{
			name:    "select * names the hidden column from the end",
			sql:     "SELECT * FROM profiles ORDER BY created_at DESC NULLS LAST, id",
			wantSQL: "SELECT *, created_at, id FROM profiles ORDER BY created_at DESC NULLS LAST, id",
			want: merge.Spec{Hidden: 2, Order: []merge.Order{
				{Col: -2, Desc: true, NullsFirst: false}, {Col: -1},
			}},
		},
		{
			name:    "an ordinal and an alias find their columns",
			sql:     "SELECT id, name AS n FROM profiles ORDER BY 1 DESC, n",
			wantSQL: "SELECT id, name AS n FROM profiles ORDER BY 1 DESC, n",
			want:    merge.Spec{Order: []merge.Order{{Col: 0, Desc: true, NullsFirst: true}, asc(1)}},
		},
		{
			name:    "a qualified order column finds the selected one",
			sql:     "SELECT p.id, a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id ORDER BY a.city, p.id",
			wantSQL: "SELECT p.id, a.city FROM profiles p JOIN addresses a ON a.profile_id = p.id ORDER BY a.city, p.id",
			want:    merge.Spec{Order: []merge.Order{asc(1), asc(0)}},
		},
		{
			name:    "an order expression is selected as a hidden column",
			sql:     "SELECT id FROM profiles ORDER BY lower(name)",
			wantSQL: "SELECT id, lower(name) FROM profiles ORDER BY lower(name)",
			want:    merge.Spec{Hidden: 1, Order: []merge.Order{asc(1)}},
		},
		{
			name:    "distinct",
			sql:     "SELECT DISTINCT country FROM profiles ORDER BY country LIMIT 3",
			wantSQL: "SELECT DISTINCT country FROM profiles ORDER BY country LIMIT 3",
			want:    merge.Spec{Distinct: true, Order: []merge.Order{asc(0)}, Limit: lim(3)},
		},
		{
			name:    "count without grouping is one group",
			sql:     "SELECT count(*) FROM profiles",
			wantSQL: "SELECT count(*) FROM profiles",
			want:    merge.Spec{Aggregate: true, Columns: []merge.Column{{Func: merge.Count}}},
		},
		{
			name:    "sum min max",
			sql:     "SELECT sum(age), min(age), max(age) FROM profiles",
			wantSQL: "SELECT sum(age), min(age), max(age) FROM profiles",
			want: merge.Spec{Aggregate: true, Columns: []merge.Column{
				{Func: merge.Sum}, {Func: merge.Min}, {Func: merge.Max}}},
		},
		{
			name:    "avg becomes sum plus a hidden count",
			sql:     "SELECT avg(age) FROM profiles",
			wantSQL: "SELECT sum(age) AS avg, count(age) FROM profiles",
			want: merge.Spec{Aggregate: true, Hidden: 1, Columns: []merge.Column{
				{Func: merge.Avg, Count: 1}, {Func: merge.Count}}},
		},
		{
			name:    "group by a selected column",
			sql:     "SELECT country, count(*) FROM profiles GROUP BY country",
			wantSQL: "SELECT country, count(*) FROM profiles GROUP BY country",
			want: merge.Spec{Aggregate: true, Columns: []merge.Column{
				{Func: merge.Pass, Key: true}, {Func: merge.Count}}},
		},
		{
			name:    "group by an ordinal",
			sql:     "SELECT count(*), country FROM profiles GROUP BY 2",
			wantSQL: "SELECT count(*), country FROM profiles GROUP BY 2",
			want: merge.Spec{Aggregate: true, Columns: []merge.Column{
				{Func: merge.Count}, {Func: merge.Pass, Key: true}}},
		},
		{
			name:    "a group column that is not selected is added",
			sql:     "SELECT count(*) FROM profiles GROUP BY country",
			wantSQL: "SELECT count(*), country FROM profiles GROUP BY country",
			want: merge.Spec{Aggregate: true, Hidden: 1, Columns: []merge.Column{
				{Func: merge.Count}, {Func: merge.Pass, Key: true}}},
		},
		{
			name:    "group by an expression",
			sql:     "SELECT lower(country) AS c, count(*) FROM profiles GROUP BY lower(country)",
			wantSQL: "SELECT lower(country) AS c, count(*), lower(country) FROM profiles GROUP BY lower(country)",
			want: merge.Spec{Aggregate: true, Hidden: 1, Columns: []merge.Column{
				{Func: merge.Pass}, {Func: merge.Count}, {Func: merge.Pass, Key: true}}},
		},
		{
			name:    "group by an output alias",
			sql:     "SELECT lower(country) AS c, count(*) FROM profiles GROUP BY c",
			wantSQL: "SELECT lower(country) AS c, count(*) FROM profiles GROUP BY c",
			want: merge.Spec{Aggregate: true, Columns: []merge.Column{
				{Func: merge.Pass, Key: true}, {Func: merge.Count}}},
		},
		{
			name: "having, order and limit move to the merge",
			sql:  "SELECT country, count(*) AS n FROM profiles GROUP BY country HAVING count(*) > $1 ORDER BY n DESC, country LIMIT 3 OFFSET 1",
			args: []any{int64(5)},
			// no ORDER BY, HAVING, LIMIT or OFFSET on the shards; the HAVING count is a hidden column
			wantSQL: "SELECT country, count(*) AS n, count(*) FROM profiles GROUP BY country",
			want: merge.Spec{
				Aggregate: true, Hidden: 1, Offset: 1, Limit: lim(3),
				Columns: []merge.Column{{Func: merge.Pass, Key: true}, {Func: merge.Count}, {Func: merge.Count}},
				Having:  &merge.Cond{Op: ">", Col: 2, Value: int64(5)},
				Order:   []merge.Order{{Col: 1, Desc: true, NullsFirst: true}, asc(0)},
			},
		},
		{
			name:    "having with and, or, not and the constant on the left",
			sql:     "SELECT country FROM profiles GROUP BY country HAVING sum(age) >= 10 AND NOT (min(age) < 3 OR 5 = max(age))",
			wantSQL: "SELECT country, sum(age), min(age), max(age) FROM profiles GROUP BY country",
			want: merge.Spec{
				Aggregate: true, Hidden: 3,
				Columns: []merge.Column{{Func: merge.Pass, Key: true}, {Func: merge.Sum}, {Func: merge.Min}, {Func: merge.Max}},
				Having: &merge.Cond{Op: "and", Args: []merge.Cond{
					{Op: ">=", Col: 1, Value: int64(10)},
					{Op: "not", Args: []merge.Cond{{Op: "or", Args: []merge.Cond{
						{Op: "<", Col: 2, Value: int64(3)},
						{Op: "=", Col: 3, Value: int64(5)}, // 5 = max(age)
					}}}},
				}},
			},
		},
		{
			name:    "order by an aggregate that is not selected",
			sql:     "SELECT country FROM profiles GROUP BY country ORDER BY count(*) DESC",
			wantSQL: "SELECT country, count(*) FROM profiles GROUP BY country",
			want: merge.Spec{
				Aggregate: true, Hidden: 1,
				Columns: []merge.Column{{Func: merge.Pass, Key: true}, {Func: merge.Count}},
				Order:   []merge.Order{{Col: 1, Desc: true, NullsFirst: true}},
			},
		},
		{
			name:    "order by avg adds the sum and count pair",
			sql:     "SELECT country FROM profiles GROUP BY country ORDER BY avg(age)",
			wantSQL: "SELECT country, sum(age), count(age) FROM profiles GROUP BY country",
			want: merge.Spec{
				Aggregate: true, Hidden: 2,
				Columns: []merge.Column{{Func: merge.Pass, Key: true}, {Func: merge.Avg, Count: 2}, {Func: merge.Count}},
				Order:   []merge.Order{asc(1)},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New().PlanMerge(tc.sql, tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if p.SQL != tc.wantSQL {
				t.Errorf("shard SQL:\n got  %s\n want %s", p.SQL, tc.wantSQL)
			}
			got := stripLabels(p.Spec)
			if tc.want.Columns == nil && got.Columns != nil {
				t.Errorf("Columns = %v, want none (select *-safe)", got.Columns)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("spec:\n got  %+v\n want %+v", got, tc.want)
			}
			if tc.wantArg != nil || len(p.Args) > 0 {
				if !reflect.DeepEqual(p.Args, tc.wantArg) {
					t.Errorf("args = %v, want %v", p.Args, tc.wantArg)
				}
			}
		})
	}
}

func TestPlanMergeRejects(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string // part of the message
	}{
		{"window function", "SELECT id, row_number() OVER (ORDER BY id) FROM profiles", "window functions"},
		{"subquery in where", "SELECT id FROM profiles WHERE age > (SELECT avg(age) FROM profiles)", "subqueries"},
		{"subquery in from", "SELECT id FROM (SELECT id FROM profiles) x", "subqueries"},
		{"exists", "SELECT id FROM profiles p WHERE EXISTS (SELECT 1 FROM addresses a WHERE a.profile_id = p.id)", "subqueries"},
		{"union", "SELECT id FROM profiles UNION SELECT id FROM profiles", "UNION"},
		{"cte", "WITH x AS (SELECT 1) SELECT * FROM x", "WITH"},
		{"for update", "SELECT id FROM profiles FOR UPDATE", "FOR UPDATE"},
		{"distinct on", "SELECT DISTINCT ON (country) country, id FROM profiles ORDER BY country, id", "DISTINCT ON"},
		{"with ties", "SELECT id FROM profiles ORDER BY id FETCH FIRST 3 ROWS WITH TIES", "WITH TIES"},
		{"count distinct", "SELECT count(DISTINCT country) FROM profiles", "DISTINCT"},
		{"filter", "SELECT count(*) FILTER (WHERE age > 3) FROM profiles", "FILTER"},
		{"string_agg", "SELECT string_agg(name, ',') FROM profiles", "string_agg"},
		{"array_agg", "SELECT country, array_agg(id) FROM profiles GROUP BY country", "array_agg"},
		{"stddev", "SELECT stddev(age) FROM profiles", "stddev"},
		{"expression around aggregate", "SELECT sum(age) / count(*) FROM profiles", "around an aggregate"},
		{"star with group by", "SELECT * FROM profiles GROUP BY id", "SELECT *"},
		{"rollup", "SELECT country, count(*) FROM profiles GROUP BY ROLLUP (country)", "GROUPING SETS"},
		{"having on an expression", "SELECT country FROM profiles GROUP BY country HAVING count(*) + 1 > 3", "around an aggregate"},
		{"having on two aggregates", "SELECT country FROM profiles GROUP BY country HAVING sum(age) > count(*)", "constant"},
		{"having like", "SELECT country FROM profiles GROUP BY country HAVING country LIKE 'a%'", "HAVING"},
		{"order using", "SELECT id FROM profiles ORDER BY id USING <", "USING"},
		{"distinct with hidden order column", "SELECT DISTINCT * FROM profiles ORDER BY id", "DISTINCT"},
		{"negative limit", "SELECT id FROM profiles LIMIT -1", "negative"},
		{"limit expression", "SELECT id FROM profiles LIMIT 1 + 1", "LIMIT"},
		{"ordinal out of range", "SELECT id FROM profiles ORDER BY 3", "position"},
		{"not a select", "UPDATE profiles SET name = 'x'", "SELECT"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New().PlanMerge(tc.sql, nil)
			if !errors.Is(err, analyze.ErrUnsupportedQuery) {
				t.Fatalf("err = %v, want ErrUnsupportedQuery", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("message %q does not mention %q", err, tc.want)
			}
		})
	}

	t.Run("missing argument", func(t *testing.T) {
		_, err := New().PlanMerge("SELECT id FROM profiles LIMIT $1", nil)
		if !errors.Is(err, analyze.ErrMissingArgument) {
			t.Errorf("err = %v, want ErrMissingArgument", err)
		}
	})
	t.Run("syntax error", func(t *testing.T) {
		if _, err := New().PlanMerge("SELEC 1", nil); err == nil {
			t.Error("expected an error")
		}
	})
}

// stripLabels clears the descriptive labels, which the golden specs above do
// not repeat; TestPlanMergeLabels covers them.
func stripLabels(s merge.Spec) merge.Spec {
	s.Columns = slices.Clone(s.Columns)
	for i := range s.Columns {
		s.Columns[i].Label = ""
	}
	s.Order = slices.Clone(s.Order)
	for i := range s.Order {
		s.Order[i].Label = ""
	}
	return s
}

func TestPlanMergeLabels(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		steps []string
	}{
		{
			"ordered merge with limit",
			"SELECT id, name FROM profiles ORDER BY created_at DESC, id LIMIT 20 OFFSET 40",
			[]string{"OrderedMerge(created_at DESC, id)", "Offset(40)", "Limit(20)", "DropHidden(1)"},
		},
		{
			"no order",
			"SELECT id FROM profiles",
			[]string{"Concatenate(in shard order)"},
		},
		{
			"ordinal and alias read as the column",
			"SELECT team, count(*) AS n FROM players GROUP BY team ORDER BY 2 DESC, team",
			[]string{"Aggregate(by team: count(*) = sum of counts)", "Sort(count(*) DESC, team)"},
		},
		{
			"avg, having and not",
			"SELECT team, avg(score) FROM players GROUP BY team HAVING avg(score) > 3 AND NOT count(*) < 2 ORDER BY 1",
			[]string{"Aggregate(by team: avg(score) = sum / count, count(*) = sum of counts)", "Having(avg(score) > 3 AND NOT (count(*) < 2))", "Sort(team)", "DropHidden(4)"},
		},
		{
			"distinct",
			"SELECT DISTINCT team FROM players ORDER BY team",
			[]string{"OrderedMerge(team)", "Distinct"},
		},
		{
			"nulls placement is only mentioned when it is not the default",
			"SELECT id FROM profiles ORDER BY id NULLS FIRST, name DESC NULLS LAST",
			[]string{"OrderedMerge(id NULLS FIRST, name DESC NULLS LAST)", "DropHidden(1)"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New().PlanMerge(tc.sql, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := p.Spec.Steps(); !slices.Equal(got, tc.steps) {
				t.Errorf("steps:\n got  %q\n want %q", got, tc.steps)
			}
		})
	}
}
