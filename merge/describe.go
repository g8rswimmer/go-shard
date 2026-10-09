package merge

import (
	"fmt"
	"slices"
	"strings"
)

// Steps describes, in the order they happen, what Merge does with the rows the
// shards return. It is for people (Explain output): the text is stable but is
// not an interface to parse.
//
//	Aggregate(by team: count(*) = sum of counts, avg(score) = sum / count)
//	Having(count(*) > 5)
//	Sort(count(*) DESC)
//	Offset(40)
//	Limit(20)
//
// Without aggregation the shards sort their own rows and the merge only
// interleaves them, which reads OrderedMerge(created_at DESC). Rows with no
// ORDER BY are Concatenate(in shard order).
func (s Spec) Steps() []string {
	var steps []string
	if s.Aggregate {
		steps = append(steps, s.aggregateStep())
		if s.Having != nil {
			steps = append(steps, "Having("+s.condText(*s.Having)+")")
		}
	}
	switch {
	case len(s.Order) > 0 && s.Aggregate:
		steps = append(steps, "Sort("+s.orderText()+")")
	case len(s.Order) > 0:
		steps = append(steps, "OrderedMerge("+s.orderText()+")")
	case !s.Aggregate:
		steps = append(steps, "Concatenate(in shard order)")
	default:
		// aggregated groups come out in no particular order
	}
	if s.Distinct {
		steps = append(steps, "Distinct")
	}
	if s.Offset > 0 {
		steps = append(steps, fmt.Sprintf("Offset(%d)", s.Offset))
	}
	if s.Limit != nil {
		steps = append(steps, fmt.Sprintf("Limit(%d)", *s.Limit))
	}
	if s.Hidden > 0 {
		steps = append(steps, fmt.Sprintf("DropHidden(%d)", s.Hidden))
	}
	return steps
}

func (s Spec) aggregateStep() string {
	var keys, funcs []string
	pairs := map[int]bool{} // the COUNT that goes with an AVG is part of it
	for _, c := range s.Columns {
		if c.Func == Avg {
			pairs[c.Count] = true
		}
	}
	for i, c := range s.Columns {
		switch {
		case pairs[i]:
			// described by its AVG
		case c.Key:
			keys = append(keys, c.name())
		case c.Func == Pass:
			// selected as is: not combined
		default:
			// A HAVING or ORDER BY on an aggregate that is also selected adds a
			// second copy of it; say it once.
			f := c.name() + " = " + c.Func.how()
			if !slices.Contains(funcs, f) {
				funcs = append(funcs, f)
			}
		}
	}
	switch {
	case len(keys) > 0 && len(funcs) > 0:
		return fmt.Sprintf("Aggregate(by %s: %s)", strings.Join(keys, ", "), strings.Join(funcs, ", "))
	case len(keys) > 0:
		return fmt.Sprintf("Aggregate(by %s)", strings.Join(keys, ", "))
	default:
		return fmt.Sprintf("Aggregate(%s)", strings.Join(funcs, ", "))
	}
}

func (f Func) how() string {
	switch f {
	case Count:
		return "sum of counts"
	case Sum:
		return "sum of sums"
	case Min:
		return "smallest"
	case Max:
		return "largest"
	case Avg:
		return "sum / count"
	default:
		return f.String()
	}
}

func (c Column) name() string {
	if c.Label != "" {
		return c.Label
	}
	return "column"
}

func (s Spec) orderText() string {
	terms := make([]string, len(s.Order))
	for i, o := range s.Order {
		t := o.Label
		if t == "" {
			t = fmt.Sprintf("column %d", o.Col)
		}
		if o.Desc {
			t += " DESC"
		}
		// PostgreSQL puts NULL last in ascending order and first in descending
		// order; only a different choice is worth saying.
		switch {
		case o.NullsFirst && !o.Desc:
			t += " NULLS FIRST"
		case !o.NullsFirst && o.Desc:
			t += " NULLS LAST"
		default:
			// the default
		}
		terms[i] = t
	}
	return strings.Join(terms, ", ")
}

func (s Spec) condText(c Cond) string {
	switch c.Op {
	case "and", "or":
		parts := make([]string, len(c.Args))
		for i, a := range c.Args {
			parts[i] = s.condText(a)
			if a.Op == "and" || a.Op == "or" {
				parts[i] = "(" + parts[i] + ")"
			}
		}
		return strings.Join(parts, " "+strings.ToUpper(c.Op)+" ")
	case "not":
		if len(c.Args) != 1 {
			return "NOT (?)"
		}
		return "NOT (" + s.condText(c.Args[0]) + ")"
	default:
		label := fmt.Sprintf("column %d", c.Col)
		if c.Col >= 0 && c.Col < len(s.Columns) && s.Columns[c.Col].Label != "" {
			label = s.Columns[c.Col].Label
		}
		value := fmt.Sprint(c.Value)
		if str, ok := c.Value.(string); ok {
			value = "'" + strings.ReplaceAll(str, "'", "''") + "'"
		}
		return label + " " + c.Op + " " + value
	}
}
