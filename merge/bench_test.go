package merge

import (
	"fmt"
	"testing"
)

// shardsOf returns n sources of rows rows each: (id, name), the ids interleaved
// across shards and ascending within each, as an ORDER BY id on each shard gives.
func shardsOf(n, rows int) []Source {
	srcs := make([]Source, n)
	for s := range srcs {
		srcs[s] = &fakeSource{cols: []string{"id", "name"}, n: rows, gen: func(i int) []any {
			return row(int64(i*n+s), "name")
		}}
	}
	return srcs
}

// drain reads every row of the result into typed destinations, as a caller does.
func drain(b *testing.B, r *Rows) int {
	b.Helper()
	var (
		id   int64
		name string
		n    int
	)
	for r.Next() {
		if err := r.Scan(&id, &name); err != nil {
			b.Fatal(err)
		}
		n++
	}
	if err := r.Err(); err != nil {
		b.Fatal(err)
	}
	_ = r.Close()
	return n
}

func BenchmarkOrderedMerge(b *testing.B) {
	const rows = 10_000
	for _, shards := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			spec := Spec{Columns: plain(2), Order: []Order{{Col: 0}}}
			total := 0
			for b.Loop() {
				r, err := Merge(spec, shardsOf(shards, rows), Options{})
				if err != nil {
					b.Fatal(err)
				}
				total += drain(b, r)
			}
			b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}

// ORDER BY ... LIMIT 20 on every shard: the merge stops after 20 rows, however
// many each shard could return.
func BenchmarkOrderedMergeLimit(b *testing.B) {
	spec := Spec{Columns: plain(2), Order: []Order{{Col: 0}}, Limit: ptr(20)}
	for b.Loop() {
		r, err := Merge(spec, shardsOf(8, 10_000), Options{})
		if err != nil {
			b.Fatal(err)
		}
		if n := drain(b, r); n != 20 {
			b.Fatalf("read %d rows", n)
		}
	}
}

func BenchmarkConcatenate(b *testing.B) {
	spec := Spec{Columns: plain(2)}
	total := 0
	for b.Loop() {
		r, err := Merge(spec, shardsOf(8, 10_000), Options{})
		if err != nil {
			b.Fatal(err)
		}
		total += drain(b, r)
	}
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "rows/s")
}

// SELECT team, count(*), sum(score) ... GROUP BY team on 8 shards: each sends one
// row per group.
func BenchmarkAggregateGroupBy(b *testing.B) {
	for _, groups := range []int{10, 1000, 20_000} {
		b.Run(fmt.Sprintf("groups=%d", groups), func(b *testing.B) {
			spec := Spec{
				Columns:   []Column{{Key: true}, {Func: Count}, {Func: Sum}},
				Aggregate: true,
			}
			for b.Loop() {
				srcs := make([]Source, 8)
				for s := range srcs {
					srcs[s] = &fakeSource{cols: []string{"team", "n", "total"}, n: groups, gen: func(i int) []any {
						return row(fmt.Sprintf("team-%d", i), int64(5), int64(100))
					}}
				}
				r, err := Merge(spec, srcs, Options{})
				if err != nil {
					b.Fatal(err)
				}
				n := 0
				var team string
				var cnt, sum int64
				for r.Next() {
					if err := r.Scan(&team, &cnt, &sum); err != nil {
						b.Fatal(err)
					}
					n++
				}
				_ = r.Close()
				if n != groups || cnt != 40 {
					b.Fatalf("%d groups, count %d", n, cnt)
				}
			}
		})
	}
}

func BenchmarkDistinct(b *testing.B) {
	spec := Spec{Columns: plain(1), Distinct: true}
	for b.Loop() {
		srcs := make([]Source, 8)
		for s := range srcs {
			srcs[s] = &fakeSource{cols: []string{"region"}, n: 10_000, gen: func(i int) []any {
				return row(fmt.Sprintf("region-%d", i%500))
			}}
		}
		r, err := Merge(spec, srcs, Options{})
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		var region string
		for r.Next() {
			if err := r.Scan(&region); err != nil {
				b.Fatal(err)
			}
			n++
		}
		_ = r.Close()
		if n != 500 {
			b.Fatalf("%d distinct values", n)
		}
	}
}
