package merge

import (
	"slices"
	"testing"
)

func TestSteps(t *testing.T) {
	ten := int64(10)
	tests := []struct {
		name string
		spec Spec
		want []string
	}{
		{"nothing to do", Spec{}, []string{"Concatenate(in shard order)"}},
		{
			"ordered with paging and a hidden column",
			Spec{Order: []Order{{Col: 1, Desc: true, NullsFirst: true, Label: "created_at"}, {Col: 0, Label: "id"}}, Offset: 5, Limit: &ten, Hidden: 1},
			[]string{"OrderedMerge(created_at DESC, id)", "Offset(5)", "Limit(10)", "DropHidden(1)"},
		},
		{
			"unlabelled terms are named by position",
			Spec{Order: []Order{{Col: -1}}},
			[]string{"OrderedMerge(column -1)"},
		},
		{
			"nulls only when not the default",
			Spec{Order: []Order{{Col: 0, Label: "a", NullsFirst: true}, {Col: 1, Label: "b", Desc: true}}},
			[]string{"OrderedMerge(a NULLS FIRST, b DESC NULLS LAST)"},
		},
		{
			"aggregate without groups",
			Spec{Aggregate: true, Columns: []Column{{Func: Count, Label: "count(*)"}, {Func: Max, Label: "max(x)"}}},
			[]string{"Aggregate(count(*) = sum of counts, max(x) = largest)"},
		},
		{
			"group, avg pair, having",
			Spec{
				Aggregate: true, Hidden: 1,
				Columns: []Column{
					{Func: Pass, Key: true, Label: "team"},
					{Func: Avg, Count: 2, Label: "avg(score)"},
					{Func: Count, Label: "count(score)"},
				},
				Having: &Cond{Op: "or", Args: []Cond{
					{Op: ">", Col: 1, Value: 3},
					{Op: "and", Args: []Cond{{Op: "=", Col: 0, Value: "it's"}, {Op: "not", Args: []Cond{{Op: "<", Col: 9, Value: 1}}}}},
				}},
				Order: []Order{{Col: 1, Desc: true, NullsFirst: true, Label: "avg(score)"}},
			},
			[]string{
				"Aggregate(by team: avg(score) = sum / count)",
				"Having(avg(score) > 3 OR (team = 'it''s' AND NOT (column 9 < 1)))",
				"Sort(avg(score) DESC)",
				"DropHidden(1)",
			},
		},
		{"distinct comes after the order", Spec{Distinct: true, Order: []Order{{Col: 0, Label: "a"}}}, []string{"OrderedMerge(a)", "Distinct"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.spec.Steps(); !slices.Equal(got, tc.want) {
				t.Errorf("steps:\n got  %q\n want %q", got, tc.want)
			}
		})
	}
}
