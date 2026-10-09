package merge

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"testing"
	"time"
)

// fakeSource serves rows from a slice, or from gen when it is set (so a test
// can serve millions of rows without holding them).
type fakeSource struct {
	cols   []string
	types  []string
	rows   [][]any
	gen    func(i int) []any
	n      int
	i      int
	cur    []any
	err    error
	failAt int // fail after this many rows when err is set
	closed bool
}

func (f *fakeSource) Columns() ([]string, error) { return f.cols, nil }
func (f *fakeSource) TypeNames() []string        { return f.types }
func (f *fakeSource) Err() error                 { return nil }
func (f *fakeSource) Close() error               { f.closed = true; return nil }
func (f *fakeSource) Next() bool {
	if f.err != nil && f.i >= f.failAt {
		return false
	}
	switch {
	case f.gen != nil && f.i < f.n:
		f.cur = f.gen(f.i)
	case f.gen == nil && f.i < len(f.rows):
		f.cur = f.rows[f.i]
	default:
		return false
	}
	f.i++
	return true
}
func (f *fakeSource) Scan(dest ...any) error {
	for i, d := range dest {
		*(d.(*any)) = f.cur[i]
	}
	return nil
}

func src(cols []string, rows ...[]any) *fakeSource { return &fakeSource{cols: cols, rows: rows} }

func row(v ...any) []any { return v }

func ptr(n int64) *int64 { return &n }

func plain(n int) []Column { return make([]Column, n) }

func collect(t *testing.T, r *Rows) [][]any {
	t.Helper()
	var out [][]any
	for r.Next() {
		cols, _ := r.Columns()
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := r.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, vals)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("iteration: %v", err)
	}
	return out
}

func TestMergeStreaming(t *testing.T) {
	cols := []string{"id", "name"}
	three := func() []Source {
		return []Source{
			src(cols, row(int64(1), "a"), row(int64(4), "d"), row(int64(7), "g")),
			src(cols, row(int64(2), "b"), row(int64(5), "e")),
			src(cols, row(int64(3), "c"), row(int64(6), "f"), row(int64(8), "h")),
		}
	}
	asc := []Order{{Col: 0}}

	tests := []struct {
		name string
		spec Spec
		want [][]any
	}{
		{"ordered ascending", Spec{Columns: plain(2), Order: asc}, [][]any{
			{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}, {int64(4), "d"},
			{int64(5), "e"}, {int64(6), "f"}, {int64(7), "g"}, {int64(8), "h"}}},
		{"limit and offset", Spec{Columns: plain(2), Order: asc, Offset: 2, Limit: ptr(3)}, [][]any{
			{int64(3), "c"}, {int64(4), "d"}, {int64(5), "e"}}},
		{"limit zero", Spec{Columns: plain(2), Order: asc, Limit: ptr(0)}, nil},
		{"offset past the end", Spec{Columns: plain(2), Order: asc, Offset: 100}, nil},
		{"unordered is each shard in turn", Spec{Columns: plain(2)}, [][]any{
			{int64(1), "a"}, {int64(4), "d"}, {int64(7), "g"},
			{int64(2), "b"}, {int64(5), "e"},
			{int64(3), "c"}, {int64(6), "f"}, {int64(8), "h"}}},
		{"hidden column is dropped", Spec{Columns: plain(2), Hidden: 1, Order: asc, Limit: ptr(2)}, [][]any{
			{int64(1)}, {int64(2)}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srcs := three()
			r, err := Merge(tc.spec, srcs, Options{})
			if err != nil {
				t.Fatal(err)
			}
			got := collect(t, r)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v\nwant %v", got, tc.want)
			}
			_ = r.Close()
			for i, s := range srcs {
				if !s.(*fakeSource).closed {
					t.Errorf("source %d was not closed", i)
				}
			}
		})
	}
}

func TestMergeOrdering(t *testing.T) {
	cols := []string{"v"}
	mk := func(rows ...any) Source {
		var rs [][]any
		for _, r := range rows {
			rs = append(rs, row(r))
		}
		return src(cols, rs...)
	}
	tests := []struct {
		name  string
		order Order
		a, b  []any
		want  []any
	}{
		{"nulls last when ascending", Order{Col: 0}, []any{int64(1), nil}, []any{int64(2)}, []any{int64(1), int64(2), nil}},
		{"nulls first when asked", Order{Col: 0, NullsFirst: true}, []any{nil, int64(1)}, []any{int64(2)}, []any{nil, int64(1), int64(2)}},
		{"descending", Order{Col: 0, Desc: true}, []any{int64(9), int64(3)}, []any{int64(5)}, []any{int64(9), int64(5), int64(3)}},
		{"descending nulls first", Order{Col: 0, Desc: true, NullsFirst: true}, []any{nil, int64(3)}, []any{int64(5)}, []any{nil, int64(5), int64(3)}},
		{"text by bytes", Order{Col: 0}, []any{"apple", "cherry"}, []any{"banana"}, []any{"apple", "banana", "cherry"}},
		{"floats", Order{Col: 0}, []any{1.5, 3.5}, []any{2.5}, []any{1.5, 2.5, 3.5}},
		{"times", Order{Col: 0},
			[]any{time.Unix(10, 0), time.Unix(30, 0)}, []any{time.Unix(20, 0)},
			[]any{time.Unix(10, 0), time.Unix(20, 0), time.Unix(30, 0)}},
		{"bools", Order{Col: 0}, []any{false, true}, []any{false}, []any{false, false, true}},
	}
	for _, tc := range tests {
		// Either shard may come first: comparisons run in both directions.
		for _, flip := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/flipped=%v", tc.name, flip), func(t *testing.T) {
				srcs := []Source{mk(tc.a...), mk(tc.b...)}
				if flip {
					srcs[0], srcs[1] = srcs[1], srcs[0]
				}
				r, err := Merge(Spec{Columns: plain(1), Order: []Order{tc.order}}, srcs, Options{})
				if err != nil {
					t.Fatal(err)
				}
				var got []any
				for _, rw := range collect(t, r) {
					got = append(got, rw[0])
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("got %v, want %v", got, tc.want)
				}
			})
		}
	}

	t.Run("numeric text sorts as numbers", func(t *testing.T) {
		a := &fakeSource{cols: cols, types: []string{"NUMERIC"}, rows: [][]any{row("9.5"), row("100")}}
		b := &fakeSource{cols: cols, types: []string{"NUMERIC"}, rows: [][]any{row("20")}}
		r, err := Merge(Spec{Columns: plain(1), Order: []Order{{Col: 0}}}, []Source{a, b}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		got := collect(t, r)
		want := [][]any{{"9.5"}, {"20"}, {"100"}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("a tie goes to the lower shard", func(t *testing.T) {
		cols := []string{"k", "from"}
		a := src(cols, row(int64(1), "a"))
		b := src(cols, row(int64(1), "b"))
		r, _ := Merge(Spec{Columns: plain(2), Order: []Order{{Col: 0}}}, []Source{b, a}, Options{})
		got := collect(t, r)
		if got[0][1] != "b" || got[1][1] != "a" {
			t.Errorf("tie order = %v, want shard order (b then a, as passed)", got)
		}
	})

	t.Run("types that cannot be compared are an error", func(t *testing.T) {
		a := src(cols, row(int64(1)))
		b := src(cols, row("x"))
		r, err := Merge(Spec{Columns: plain(1), Order: []Order{{Col: 0}}}, []Source{a, b}, Options{})
		if err == nil {
			for r.Next() {
			}
			err = r.Err()
		}
		if err == nil {
			t.Fatal("expected an error comparing a number with text")
		}
	})
}

func TestMergeDistinct(t *testing.T) {
	cols := []string{"c"}
	a := src(cols, row("x"), row("y"))
	b := src(cols, row("y"), row("z"), row("x"))
	r, err := Merge(Spec{Columns: plain(1), Distinct: true, Offset: 1, Limit: ptr(2)}, []Source{a, b}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	got := collect(t, r)
	want := [][]any{{"y"}, {"z"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	t.Run("bounded by MaxRows", func(t *testing.T) {
		a := src(cols, row("1"), row("2"), row("3"))
		r, err := Merge(Spec{Columns: plain(1), Distinct: true}, []Source{a}, Options{MaxRows: 2})
		if err != nil {
			t.Fatal(err)
		}
		for r.Next() {
		}
		if !errors.Is(r.Err(), ErrLimitExceeded) {
			t.Errorf("err = %v, want ErrLimitExceeded", r.Err())
		}
	})
}

func TestMergeAggregate(t *testing.T) {
	// SELECT country, count(*), sum(n), min(n), max(n), avg(n) ... GROUP BY country
	// per shard: country, count, sum, min, max, avg-sum, [hidden] avg-count
	cols := []string{"country", "count", "sum", "min", "max", "sum", "count"}
	spec := Spec{
		Columns: []Column{
			{Func: Pass, Key: true}, {Func: Count}, {Func: Sum}, {Func: Min}, {Func: Max},
			{Func: Avg, Count: 6}, {Func: Count},
		},
		Hidden:    1,
		Aggregate: true,
	}
	a := src(cols,
		row("us", int64(2), int64(10), int64(4), int64(6), int64(10), int64(2)),
		row("fr", int64(1), int64(5), int64(5), int64(5), int64(5), int64(1)))
	b := src(cols,
		row("us", int64(3), int64(30), int64(1), int64(20), int64(30), int64(3)),
		row("de", int64(1), nil, nil, nil, nil, int64(0)))

	r, err := Merge(spec, []Source{a, b}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	got := collect(t, r)
	// "us": count 5, sum 40, min 1, max 20, avg 8; "fr"; "de" has only NULLs.
	want := [][]any{
		{"us", int64(5), int64(40), int64(1), int64(20), "8.0000000000000000"},
		{"fr", int64(1), int64(5), int64(5), int64(5), "5.0000000000000000"},
		{"de", int64(1), nil, nil, nil, nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %v\nwant %v", got, want)
	}

	t.Run("one group when there is no key", func(t *testing.T) {
		cols := []string{"count", "sum"}
		spec := Spec{Columns: []Column{{Func: Count}, {Func: Sum}}, Aggregate: true}
		r, err := Merge(spec, []Source{
			src(cols, row(int64(0), nil)),
			src(cols, row(int64(4), 2.5)),
			src(cols, row(int64(1), 0.5)),
		}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		got := collect(t, r)
		want := [][]any{{int64(5), 3.0}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("numeric sums keep their scale", func(t *testing.T) {
		cols := []string{"sum"}
		spec := Spec{Columns: []Column{{Func: Sum}}, Aggregate: true}
		r, err := Merge(spec, []Source{
			src(cols, row("10.25")), src(cols, row("0.5")), src(cols, row("3")),
		}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if got := collect(t, r); got[0][0] != "13.75" {
			t.Errorf("sum = %v, want 13.75", got[0][0])
		}
	})

	t.Run("min and max of numeric text compare as numbers", func(t *testing.T) {
		mk := func(v string) Source {
			return &fakeSource{cols: []string{"m"}, types: []string{"NUMERIC"}, rows: [][]any{row(v)}}
		}
		spec := Spec{Columns: []Column{{Func: Max}}, Aggregate: true}
		r, err := Merge(spec, []Source{mk("9"), mk("100"), mk("20")}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if got := collect(t, r); got[0][0] != "100" {
			t.Errorf("max = %v, want 100", got[0][0])
		}
	})

	t.Run("having, order, offset and limit apply to merged groups", func(t *testing.T) {
		cols := []string{"k", "count"}
		spec := Spec{
			Columns:   []Column{{Func: Pass, Key: true}, {Func: Count}},
			Aggregate: true,
			Having:    &Cond{Op: "and", Args: []Cond{{Op: ">", Col: 1, Value: int64(1)}, {Op: "<>", Col: 0, Value: "skip"}}},
			Order:     []Order{{Col: 1, Desc: true}, {Col: 0}},
			Offset:    1,
			Limit:     ptr(2),
		}
		a := src(cols, row("a", int64(1)), row("b", int64(2)), row("c", int64(2)), row("skip", int64(9)))
		b := src(cols, row("a", int64(2)), row("d", int64(1)), row("e", int64(7)))
		r, err := Merge(spec, []Source{a, b}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		got := collect(t, r)
		// counts: a=3 b=2 c=2 d=1 e=7 skip=9 -> having: a, b, c, e -> e(7) a(3) b(2) c(2) -> offset 1, limit 2
		want := [][]any{{"a", int64(3)}, {"b", int64(2)}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("having with or and not", func(t *testing.T) {
		cols := []string{"k", "count"}
		spec := Spec{
			Columns:   []Column{{Func: Pass, Key: true}, {Func: Count}},
			Aggregate: true,
			Having: &Cond{Op: "or", Args: []Cond{
				{Op: "=", Col: 1, Value: int64(1)},
				{Op: "not", Args: []Cond{{Op: "<", Col: 1, Value: int64(5)}}},
			}},
			Order: []Order{{Col: 0}},
		}
		a := src(cols, row("a", int64(1)), row("b", int64(3)), row("c", int64(8)))
		r, err := Merge(spec, []Source{a}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		got := collect(t, r)
		want := [][]any{{"a", int64(1)}, {"c", int64(8)}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("having uses SQL's three-valued logic", func(t *testing.T) {
		// NOT (avg < 43) is unknown, not true, when avg is NULL
		cols := []string{"k", "n"}
		spec := Spec{
			Columns:   []Column{{Func: Pass, Key: true}, {Func: Sum}},
			Aggregate: true,
			Having:    &Cond{Op: "not", Args: []Cond{{Op: "<", Col: 1, Value: int64(43)}}},
			Order:     []Order{{Col: 0}},
		}
		a := src(cols, row("null", nil), row("low", int64(10)), row("high", int64(50)))
		r, err := Merge(spec, []Source{a}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		got := collect(t, r)
		want := [][]any{{"high", int64(50)}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		// OR with an unknown operand is true if the other is
		spec.Having = &Cond{Op: "or", Args: []Cond{{Op: "<", Col: 1, Value: int64(43)}, {Op: "=", Col: 0, Value: "null"}}}
		a = src(cols, row("null", nil), row("low", int64(10)), row("high", int64(50)))
		r, _ = Merge(spec, []Source{a}, Options{})
		got = collect(t, r)
		want = [][]any{{"low", int64(10)}, {"null", nil}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("or: got %v, want %v", got, want)
		}
	})

	t.Run("averages order as numbers", func(t *testing.T) {
		// per-shard sum and count; the sums are bigint, so nothing marks the
		// average as numeric except that it is one
		cols := []string{"k", "sum", "count"}
		spec := Spec{
			Columns:   []Column{{Func: Pass, Key: true}, {Func: Avg, Count: 2}, {Func: Count}},
			Hidden:    1,
			Aggregate: true,
			Order:     []Order{{Col: 1, Desc: true}},
		}
		a := src(cols, row("nine", int64(19), int64(2)), row("big", int64(884), int64(10)))
		r, err := Merge(spec, []Source{a}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		got := collect(t, r)
		if len(got) != 2 || got[0][0] != "big" {
			t.Errorf("got %v: 88.4 must sort above 9.5", got)
		}
	})

	t.Run("groups are bounded by MaxRows", func(t *testing.T) {
		cols := []string{"k", "count"}
		spec := Spec{Columns: []Column{{Func: Pass, Key: true}, {Func: Count}}, Aggregate: true}
		a := src(cols, row("a", int64(1)), row("b", int64(1)), row("c", int64(1)))
		srcs := []Source{a}
		_, err := Merge(spec, srcs, Options{MaxRows: 2})
		if !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("err = %v, want ErrLimitExceeded", err)
		}
		if !a.closed {
			t.Error("the source was not closed after the failure")
		}
	})

	t.Run("sources are closed once everything is read", func(t *testing.T) {
		cols := []string{"count"}
		a := src(cols, row(int64(1)))
		r, err := Merge(Spec{Columns: []Column{{Func: Count}}, Aggregate: true}, []Source{a}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !a.closed {
			t.Error("an aggregate keeps connections busy after reading everything")
		}
		_ = r.Close()
	})
}

func TestMergeErrors(t *testing.T) {
	cols := []string{"a"}
	t.Run("column count mismatch", func(t *testing.T) {
		a := src([]string{"a", "b"}, row(1, 2))
		if _, err := Merge(Spec{Columns: plain(1)}, []Source{a}, Options{}); err == nil {
			t.Fatal("expected an error")
		}
		if !a.closed {
			t.Error("source not closed on failure")
		}
	})
	t.Run("no sources", func(t *testing.T) {
		if _, err := Merge(Spec{Columns: plain(1)}, nil, Options{}); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("order column out of range", func(t *testing.T) {
		if _, err := Merge(Spec{Columns: plain(1), Order: []Order{{Col: 3}}}, []Source{src(cols)}, Options{}); err == nil {
			t.Fatal("expected an error")
		}
	})
	t.Run("scan needs Next first and the right width", func(t *testing.T) {
		r, _ := Merge(Spec{Columns: plain(1)}, []Source{src(cols, row(int64(1)))}, Options{})
		var v int64
		if err := r.Scan(&v); err == nil {
			t.Error("Scan before Next should fail")
		}
		r.Next()
		if err := r.Scan(&v, &v); err == nil {
			t.Error("Scan with the wrong number of destinations should fail")
		}
	})
}

func TestScan(t *testing.T) {
	r, err := Merge(Spec{Columns: plain(6)}, []Source{src(
		[]string{"a", "b", "c", "d", "e", "f"},
		row(int64(7), "12", 2.5, []byte("hi"), nil, true),
	)}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Next() {
		t.Fatal("no row")
	}
	var (
		a  int
		b  int64
		c  float64
		d  string
		e  sql.NullString
		f  bool
		ep *string
	)
	if err := r.Scan(&a, &b, &c, &d, &e, &f); err != nil {
		t.Fatal(err)
	}
	if a != 7 || b != 12 || c != 2.5 || d != "hi" || e.Valid || !f {
		t.Errorf("scanned %v %v %v %v %v %v", a, b, c, d, e, f)
	}
	var x any
	var s string
	var i8 int8
	for _, dest := range []any{&x, &s} {
		if err := assign(dest, int64(5)); err != nil {
			t.Errorf("assign(%T): %v", dest, err)
		}
	}
	if s != "5" {
		t.Errorf("int to string = %q", s)
	}
	if err := assign(&i8, int64(300)); err == nil {
		t.Error("overflow should fail")
	}
	if err := assign(&a, nil); err == nil {
		t.Error("NULL into int should fail")
	}
	if err := assign(a, int64(1)); err == nil {
		t.Error("non-pointer destination should fail")
	}
	// A pointer to a pointer is how database/sql scans a nullable column.
	ep = &s
	if err := assign(&ep, nil); err != nil || ep != nil {
		t.Errorf("NULL into **string = %v, %v; want a nil pointer", ep, err)
	}
	if err := assign(&ep, int64(9)); err != nil || ep == nil || *ep != "9" {
		t.Errorf("value into **string = %v, %v; want a pointer to 9", ep, err)
	}
	var ip *int
	if err := assign(&ip, "x"); err == nil {
		t.Error("a bad value into **int should fail")
	}
}

// A merge must not hold the result in memory: serving millions of rows from
// three shards, in order, should use memory that does not grow with the count.
func TestStreamingMemoryIsFlat(t *testing.T) {
	heapAfter := func(rows int) uint64 {
		mk := func(shard int) Source {
			return &fakeSource{cols: []string{"id", "pad"}, n: rows, gen: func(i int) []any {
				return []any{int64(i*3 + shard), "padding-padding-padding"}
			}}
		}
		r, err := Merge(Spec{Columns: plain(2), Order: []Order{{Col: 0}}}, []Source{mk(0), mk(1), mk(2)}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		var peak uint64
		var ms runtime.MemStats
		var last int64 = -1
		n := 0
		for r.Next() {
			var id int64
			var pad string
			if err := r.Scan(&id, &pad); err != nil {
				t.Fatal(err)
			}
			if id <= last {
				t.Fatalf("row %d out of order: %d after %d", n, id, last)
			}
			last = id
			n++
			if n%(rows) == 0 { // a few samples, taken while streaming
				runtime.GC()
				runtime.ReadMemStats(&ms)
				peak = max(peak, ms.HeapInuse)
			}
		}
		if n != rows*3 {
			t.Fatalf("got %d rows, want %d", n, rows*3)
		}
		return peak
	}

	small := heapAfter(10_000)
	large := heapAfter(300_000)
	t.Logf("heap in use: %d KiB for 30k rows, %d KiB for 900k rows", small/1024, large/1024)
	if large > small+8<<20 {
		t.Errorf("memory grew with the result: %d KiB for 30k rows, %d KiB for 900k rows", small/1024, large/1024)
	}
}

func TestFuncString(t *testing.T) {
	for f, want := range map[Func]string{Pass: "pass", Count: "count", Sum: "sum", Min: "min", Max: "max", Avg: "avg", Func(99): "Func(?)"} {
		if got := f.String(); got != want {
			t.Errorf("%d = %q, want %q", f, got, want)
		}
	}
	_ = fmt.Sprint
}

// The int64 and float64 fast paths of compare must order exactly as the exact
// (big.Rat) comparison does, including at the extremes.
func TestCompareFastPathsAgreeWithExactComparison(t *testing.T) {
	exact := func(a, b any) int { return toRat(a).Cmp(toRat(b)) }
	ints := []any{int64(math.MinInt64), int64(-1), int64(0), int64(1), int64(math.MaxInt64), int64(math.MaxInt64 - 1)}
	floats := []any{math.SmallestNonzeroFloat64, -0.5, 0.0, 0.5, 1e300, -1e300, math.MaxFloat64}
	for name, vals := range map[string][]any{"int64": ints, "float64": floats} {
		for _, a := range vals {
			for _, b := range vals {
				got, err := compare(a, b, false)
				if err != nil || got != exact(a, b) {
					t.Errorf("%s: compare(%v, %v) = %d, %v; want %d", name, a, b, got, err, exact(a, b))
				}
			}
		}
	}
	// mixed kinds still use the exact comparison
	if got, err := compare(int64(2), 2.5, false); err != nil || got != -1 {
		t.Errorf("compare(int64 2, 2.5) = %d, %v", got, err)
	}
	// NaN and infinities are still refused, as before
	if _, err := compare(math.NaN(), 1.0, false); err == nil {
		t.Error("NaN should not order")
	}
	if _, err := compare(math.Inf(1), 1.0, false); err == nil {
		t.Error("Inf should not order")
	}
}
