//go:build integration

package shard_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/g8rswimmer/go-shard/shardtest"
)

// genQuery is a generated SELECT and what is known about its result.
type genQuery struct {
	sql string
	// total is true when the ORDER BY decides the position of every row, so the
	// sharded result must equal the reference row for row. Otherwise rows are
	// compared as a set, and sorted checks that the order columns are in order.
	total  bool
	sorted []sortCheck
}

// sortCheck verifies that result column col is in order, as far as the query
// asks for it.
type sortCheck struct {
	col        int
	desc       bool
	nullsFirst bool
}

type gen struct{ rng *rand.Rand }

func (g gen) pick(s []string) string { return s[g.rng.Intn(len(s))] }
func (g gen) chance(n int) bool      { return g.rng.Intn(n) == 0 }

// where returns zero to two conditions on events columns.
func (g gen) where(alias string) string {
	p := func(col string) string {
		if alias != "" {
			return alias + "." + col
		}
		return col
	}
	preds := []func() string{
		func() string { return fmt.Sprintf("%s >= %d", p("score"), g.rng.Intn(100)) },
		func() string { return fmt.Sprintf("%s < %d", p("score"), g.rng.Intn(100)) },
		func() string { return fmt.Sprintf("%s = '%s'", p("kind"), g.pick(kinds)) },
		func() string { return fmt.Sprintf("%s <> '%s'", p("kind"), g.pick(kinds)) },
		func() string { return p("region") + " IS NULL" },
		func() string { return p("region") + " IS NOT NULL" },
		func() string { return fmt.Sprintf("%s < %d.50", p("amount"), g.rng.Intn(1000)) },
		func() string { return fmt.Sprintf("%s %% %d = 0", p("id"), 2+g.rng.Intn(5)) },
		func() string {
			ids := make([]string, 1+g.rng.Intn(12))
			for i := range ids {
				ids[i] = fmt.Sprint(1 + g.rng.Intn(900))
			}
			return fmt.Sprintf("%s IN (%s)", p("id"), strings.Join(ids, ", "))
		},
	}
	n := g.rng.Intn(3)
	if n == 0 {
		return ""
	}
	conds := make([]string, n)
	for i := range conds {
		conds[i] = preds[g.rng.Intn(len(preds))]()
	}
	op := " AND "
	if g.chance(4) && !strings.Contains(strings.Join(conds, ""), " IN (") {
		op = " OR "
	}
	return " WHERE " + strings.Join(conds, op)
}

func (g gen) limit(total bool) string {
	if !total || g.chance(2) {
		return ""
	}
	s := fmt.Sprintf(" LIMIT %d", g.rng.Intn(40))
	if g.chance(2) {
		s += fmt.Sprintf(" OFFSET %d", g.rng.Intn(60))
	}
	return s
}

func dirSQL(g gen) (sql string, desc, nullsFirst bool) {
	switch g.rng.Intn(5) {
	case 0:
		return " DESC", true, true
	case 1:
		return " ASC", false, false
	case 2:
		return " DESC NULLS LAST", true, false
	case 3:
		return " ASC NULLS FIRST", false, true
	default:
		return "", false, false
	}
}

var eventCols = []string{"id", "kind", "region", "score", "amount", "ratio", "created", "note"}

// rows generates a SELECT of plain columns.
func (g gen) rows() genQuery {
	var cols []string
	switch {
	case g.chance(5):
		cols = []string{"*"}
	default:
		for _, i := range g.rng.Perm(len(eventCols))[:1+g.rng.Intn(5)] {
			cols = append(cols, eventCols[i])
		}
	}
	visible := func(c string) int {
		if cols[0] == "*" {
			for i, ec := range eventCols {
				if ec == c {
					return i
				}
			}
		}
		for i, sc := range cols {
			if sc == c {
				return i
			}
		}
		return -1
	}

	var q genQuery
	var order []string
	hasID := false
	for _, i := range g.rng.Perm(len(eventCols))[:g.rng.Intn(4)] {
		c := eventCols[i]
		d, desc, nf := dirSQL(g)
		order = append(order, c+d)
		hasID = hasID || c == "id"
		if len(q.sorted) == len(order)-1 && visible(c) >= 0 { // a prefix of visible keys can be checked
			q.sorted = append(q.sorted, sortCheck{col: visible(c), desc: desc, nullsFirst: nf})
		}
	}
	q.total = hasID
	lim := ""
	if g.chance(2) {
		if !hasID {
			order = append(order, "id")
			q.total = true
		}
		lim = g.limit(true)
	}
	q.sql = "SELECT " + strings.Join(cols, ", ") + " FROM events" + g.where("")
	if len(order) > 0 {
		q.sql += " ORDER BY " + strings.Join(order, ", ")
	}
	q.sql += lim
	return q
}

// distinct generates SELECT DISTINCT, ordered by every selected column.
func (g gen) distinct() genQuery {
	pool := []string{"kind", "region", "score / 25 AS band", "score % 3 AS m"}
	var cols []string
	for _, i := range g.rng.Perm(len(pool))[:1+g.rng.Intn(3)] {
		cols = append(cols, pool[i])
	}
	var order []string
	var q genQuery
	for i, c := range cols {
		d, desc, nf := dirSQL(g)
		order = append(order, fmt.Sprint(i+1)+d)
		q.sorted = append(q.sorted, sortCheck{col: i, desc: desc, nullsFirst: nf})
		_ = c
	}
	q.total = true
	q.sql = "SELECT DISTINCT " + strings.Join(cols, ", ") + " FROM events" + g.where("") +
		" ORDER BY " + strings.Join(order, ", ") + g.limit(true)
	return q
}

var aggs = []string{
	"count(*)", "count(score)", "count(region)", "sum(score)", "sum(amount)", "sum(ratio)",
	"min(score)", "max(score)", "min(amount)", "max(amount)", "max(created)", "min(created)", "min(note)", "max(note)", "min(kind)",
	"avg(score)", "avg(amount)", "avg(ratio)",
}

// aggregate generates aggregates with an optional GROUP BY, HAVING and ORDER BY.
func (g gen) aggregate() genQuery {
	groupPool := []struct{ sel, group string }{
		{"kind", "kind"}, {"region", "region"}, {"score / 25 AS band", "score / 25"},
		{"lower(kind) AS lk", "lower(kind)"}, {"score % 3 AS m", "score % 3"},
	}
	var groups []struct{ sel, group string }
	if !g.chance(4) {
		for _, i := range g.rng.Perm(len(groupPool))[:1+g.rng.Intn(2)] {
			groups = append(groups, groupPool[i])
		}
	}
	var items, groupBy, groupNames []string
	for _, gr := range groups {
		items = append(items, gr.sel)
		groupBy = append(groupBy, gr.group)
		name := gr.sel
		if i := strings.LastIndex(name, " AS "); i >= 0 {
			name = name[i+4:]
		}
		groupNames = append(groupNames, name)
	}
	var aggItems []string
	for _, i := range g.rng.Perm(len(aggs))[:1+g.rng.Intn(4)] {
		aggItems = append(aggItems, aggs[i])
	}
	items = append(items, aggItems...)

	var q genQuery
	sql := "SELECT " + strings.Join(items, ", ") + " FROM events" + g.where("")
	if len(groups) > 0 {
		sql += " GROUP BY " + strings.Join(groupBy, ", ")
	}

	if g.chance(2) {
		havings := []string{
			fmt.Sprintf("count(*) > %d", g.rng.Intn(20)),
			fmt.Sprintf("sum(score) >= %d", g.rng.Intn(800)),
			fmt.Sprintf("avg(score) < %d", 20+g.rng.Intn(60)),
			fmt.Sprintf("min(score) <= %d", g.rng.Intn(30)),
			fmt.Sprintf("max(amount) > %d", g.rng.Intn(900)),
			fmt.Sprintf("%d < count(region)", g.rng.Intn(15)),
		}
		h := havings[g.rng.Intn(len(havings))]
		switch g.rng.Intn(4) {
		case 0:
			h += " AND " + havings[g.rng.Intn(len(havings))]
		case 1:
			h += " OR " + havings[g.rng.Intn(len(havings))]
		case 2:
			h = "NOT (" + h + ")"
		default:
			// a single condition
		}
		sql += " HAVING " + h
	}

	switch {
	case len(groups) == 0:
		q.total = true // one row
	case g.chance(4):
		// no ORDER BY: compare as a set
	default:
		var order []string
		if g.chance(2) {
			a := aggItems[g.rng.Intn(len(aggItems))]
			d, desc, nf := dirSQL(g)
			order = append(order, a+d)
			q.sorted = append(q.sorted, sortCheck{col: len(groups) + indexOf(aggItems, a), desc: desc, nullsFirst: nf})
		}
		for i, n := range groupNames {
			d, desc, nf := dirSQL(g)
			switch {
			case g.chance(3):
				order = append(order, fmt.Sprint(i+1)+d)
			default:
				order = append(order, n+d)
			}
			if len(q.sorted) == len(order)-1 {
				q.sorted = append(q.sorted, sortCheck{col: i, desc: desc, nullsFirst: nf})
			}
		}
		q.total = true // the group columns are unique per row
		sql += " ORDER BY " + strings.Join(order, ", ") + g.limit(true)
	}
	q.sql = sql
	return q
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return 0
}

// joins generates queries over the colocated event_tags table.
func (g gen) joins() genQuery {
	const from = " FROM events e JOIN event_tags t ON t.event_id = e.id"
	w := g.where("e")
	var q genQuery
	switch g.rng.Intn(3) {
	case 0:
		q.sql = "SELECT e.id, t.tag" + from + w + " ORDER BY t.tag, e.id" + g.limit(true)
		q.total = true
	case 1:
		q.sql = "SELECT e.kind, t.tag, count(*), avg(e.score)" + from + w + " GROUP BY e.kind, t.tag ORDER BY e.kind, t.tag" + g.limit(true)
		q.total = true
	default:
		q.sql = "SELECT t.tag, count(*) AS n, sum(e.amount)" + from + w + " GROUP BY t.tag HAVING count(*) > 2 ORDER BY n DESC, t.tag"
		q.total = true
	}
	return q
}

func (g gen) next() genQuery {
	switch n := g.rng.Intn(10); {
	case n < 4:
		return g.rows()
	case n < 5:
		return g.distinct()
	case n < 9:
		return g.aggregate()
	default:
		return g.joins()
	}
}

// compareCell orders two values as PostgreSQL does for the checks above.
func compareCell(a, b any) int {
	switch x := a.(type) {
	case int64:
		return cmpOrdered(x, b.(int64))
	case float64:
		return cmpOrdered(x, b.(float64))
	case string:
		if y, ok := b.(string); ok {
			if fx, ok1 := parseNum(x); ok1 {
				if fy, ok2 := parseNum(y); ok2 {
					return cmpOrdered(fx, fy)
				}
			}
			return strings.Compare(x, y)
		}
	case time.Time:
		return x.Compare(b.(time.Time))
	default:
		// not comparable here
	}
	return 0
}

func parseNum(s string) (float64, bool) {
	var f float64
	_, err := fmt.Sscanf(s, "%g", &f)
	if err != nil {
		return 0, false
	}
	return f, !strings.ContainsAny(s, "abcdfghijklmnopqrstuvwxyz")
}

func cmpOrdered[T int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// checkSorted reports the first adjacent pair that is out of order.
func checkSorted(rows [][]any, keys []sortCheck) (int, bool) {
	for i := 1; i < len(rows); i++ {
		for _, k := range keys {
			a, b := rows[i-1][k.col], rows[i][k.col]
			var n int
			switch {
			case a == nil && b == nil:
				continue
			case a == nil:
				n = 1
				if k.nullsFirst {
					n = -1
				}
			case b == nil:
				n = -1
				if k.nullsFirst {
					n = 1
				}
			default:
				n = compareCell(a, b)
				if k.desc {
					n = -n
				}
			}
			switch {
			case n < 0:
				goto next // in order on this key; later keys do not matter
			case n > 0:
				return i, false
			default:
				// a tie: the next key decides
			}
		}
	next:
	}
	return 0, true
}

// TestFanOutMatchesOneDatabase runs generated queries on a single database
// holding every row and on three shards, and requires the same answers.
func TestFanOutMatchesOneDatabase(t *testing.T) {
	ctx := context.Background()
	cluster := shardtest.NewCluster(t, 3)
	db := cluster.Open(t, fanoutRegistry(t))
	cluster.ExecAll(t, eventsDDL)
	cluster.ExecAll(t, tagsDDL)

	// FANOUT_SEED and FANOUT_QUERIES let a run explore further than the default.
	seed, queries := int64(20260601), 600
	if v, err := strconv.ParseInt(os.Getenv("FANOUT_SEED"), 10, 64); err == nil {
		seed = v
	}
	if v, err := strconv.Atoi(os.Getenv("FANOUT_QUERIES")); err == nil {
		queries = v
	}
	rng := rand.New(rand.NewSource(seed))
	events := makeEvents(rng, 700)
	for _, e := range events {
		if _, err := db.WithShardKey(e.id).Exec(ctx,
			"INSERT INTO events (id, kind, region, score, amount, ratio, created, note) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
			e.id, e.kind, e.region, e.score, e.amount, e.ratio, e.created, e.note); err != nil {
			t.Fatal(err)
		}
		for _, tag := range e.tags {
			if _, err := db.WithShardKey(e.id).Exec(ctx, "INSERT INTO event_tags (event_id, tag) VALUES ($1, $2)", e.id, tag); err != nil {
				t.Fatal(err)
			}
		}
	}
	reference := combinedDB(t, cluster)
	loadEvents(t, reference, events)

	// The data must really be spread out, or the test proves nothing.
	for _, id := range cluster.IDs() {
		if n := cluster.Count(t, id, "SELECT count(*) FROM events"); n < 100 {
			t.Fatalf("%s holds only %d of %d events", id, n, len(events))
		}
	}

	g := gen{rand.New(rand.NewSource(seed + 1))}
	var withRows, grouped, ordered int
	for i := 0; i < queries; i++ {
		q := g.next()
		want := queryConn(t, reference, q.sql)
		rows, err := db.WithAllShards().Query(ctx, q.sql)
		if err != nil {
			t.Fatalf("query %d failed on the shards: %v\n%s", i, err, q.sql)
		}
		got, err := readRows(rows)
		_ = rows.Close()
		if err != nil {
			t.Fatalf("query %d: reading the merged rows: %v\n%s", i, err, q.sql)
		}

		if msg := diffResults(q, got, want); msg != "" {
			t.Fatalf("query %d differs from one database: %s\n%s\nsharded (%d rows): %v\nreference (%d rows): %v",
				i, msg, q.sql, len(got), head(got), len(want), head(want))
		}
		if len(want) > 0 {
			withRows++
		}
		if strings.Contains(q.sql, "GROUP BY") || strings.Contains(q.sql, "count(") {
			grouped++
		}
		if strings.Contains(q.sql, "ORDER BY") {
			ordered++
		}
	}
	t.Logf("%d queries matched one database (%d returned rows, %d aggregate, %d ordered)", queries, withRows, grouped, ordered)
	if withRows < queries/2 {
		t.Errorf("only %d of %d generated queries returned rows: the generator is too restrictive to prove much", withRows, queries)
	}
}

func head(rows [][]any) [][]any {
	if len(rows) > 4 {
		return rows[:4]
	}
	return rows
}

func diffResults(q genQuery, got, want [][]any) string {
	if len(got) != len(want) {
		return fmt.Sprintf("%d rows, want %d", len(got), len(want))
	}
	if q.total {
		for i := range got {
			if len(got[i]) != len(want[i]) {
				return fmt.Sprintf("row %d has %d columns, want %d", i, len(got[i]), len(want[i]))
			}
			for c := range got[i] {
				if !sameValue(got[i][c], want[i][c]) {
					return fmt.Sprintf("row %d column %d = %v (%T), want %v (%T)", i, c, got[i][c], got[i][c], want[i][c], want[i][c])
				}
			}
		}
		return ""
	}
	a, b := make([]string, len(got)), make([]string, len(want))
	for i := range got {
		a[i], b[i] = canonical(got[i]), canonical(want[i])
	}
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return fmt.Sprintf("as a set, row %q has no match (want %q)", a[i], b[i])
		}
	}
	if i, ok := checkSorted(got, q.sorted); !ok {
		return fmt.Sprintf("rows %d and %d are out of order", i-1, i)
	}
	return ""
}
