package merge

import (
	"container/heap"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Source is the rows of one shard. *sql.Rows satisfies it.
type Source interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	Err() error
	Close() error
}

// typeNamer is implemented by sources that know their column types.
// *sql.Rows does, through ColumnTypes; tests can implement TypeNames.
type typeNamer interface {
	TypeNames() []string
}

type sqlTypes interface {
	ColumnTypes() ([]*sql.ColumnType, error)
}

// Rows is the merged result. It satisfies the same interface as *sql.Rows.
// Always Close it.
type Rows struct {
	cols   []string
	next   func() ([]any, error) // nil row means the end
	srcs   []Source
	cur    []any
	err    error
	closed bool
}

// Next advances to the next row.
func (r *Rows) Next() bool {
	if r.closed || r.err != nil {
		return false
	}
	row, err := r.next()
	switch {
	case err != nil:
		r.err = err
		_ = r.release()
		return false
	case row == nil:
		r.cur = nil
		r.closed = true
		_ = r.release()
		return false
	default:
		r.cur = row
		return true
	}
}

// Scan copies the current row into dest.
func (r *Rows) Scan(dest ...any) error {
	switch {
	case r.cur == nil:
		return errors.New("shard: Scan called without a current row; call Next first")
	case len(dest) != len(r.cur):
		return fmt.Errorf("shard: Scan got %d destinations for %d columns", len(dest), len(r.cur))
	default:
		// the right shape
	}
	for i, d := range dest {
		if err := assign(d, r.cur[i]); err != nil {
			return fmt.Errorf("column %d (%s): %w", i, r.cols[i], err)
		}
	}
	return nil
}

// Columns returns the result's column names.
func (r *Rows) Columns() ([]string, error) { return append([]string(nil), r.cols...), nil }

// Err returns the error that stopped iteration, if any.
func (r *Rows) Err() error { return r.err }

// Close releases every shard's rows. It is safe to call more than once.
func (r *Rows) Close() error {
	r.closed = true
	return r.release()
}

// release closes the shards' rows as soon as they are no longer needed.
func (r *Rows) release() error {
	var errs []error
	for _, s := range r.srcs {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	r.srcs = nil
	return errors.Join(errs...)
}

// Merge combines the shards' rows. It takes ownership of srcs and closes them.
func Merge(spec Spec, srcs []Source, opts Options) (*Rows, error) {
	m, err := newMerger(spec, srcs, opts)
	if err != nil {
		for _, s := range srcs {
			_ = s.Close()
		}
		return nil, err
	}

	out := &Rows{cols: m.names[:m.width], srcs: srcs}
	switch {
	case spec.Aggregate:
		rows, err := m.aggregate()
		if err != nil {
			_ = out.release()
			return nil, err
		}
		_ = out.release() // everything was read; free the connections now
		out.next = m.fromSlice(rows)
	default:
		if err := m.prime(); err != nil {
			_ = out.release()
			return nil, err
		}
		out.next = m.stream
	}
	return out, nil
}

// merger holds the state of one merge.
type merger struct {
	spec    Spec
	opts    Options
	cols    []Column // per-shard columns; all Pass if the spec does not list them
	order   []Order  // spec.Order with negative columns resolved
	names   []string
	numeric []bool // per column: strings are decimal numbers
	width   int    // visible columns
	cursors []*cursor

	// streaming state
	ordered bool
	h       *cursorHeap
	at      int // sequential mode: the cursor being read
	seen    map[string]struct{}
	skipped int64
	emitted int64
}

func newMerger(spec Spec, srcs []Source, opts Options) (*merger, error) {
	if len(srcs) == 0 {
		return nil, errors.New("shard: nothing to merge")
	}
	m := &merger{spec: spec, opts: opts}

	for i, s := range srcs {
		names, err := s.Columns()
		if err != nil {
			return nil, err
		}
		if i == 0 {
			m.names = names
			m.numeric = numericColumns(s, len(names))
		}
		if len(names) != len(m.names) {
			return nil, fmt.Errorf("shard: shards returned %d and %d columns for one statement", len(m.names), len(names))
		}
		m.cursors = append(m.cursors, &cursor{src: s, idx: i, width: len(names)})
	}

	n := len(m.names)
	switch {
	case spec.Columns == nil:
		if spec.Aggregate {
			return nil, errors.New("shard: an aggregate merge needs its columns listed")
		}
		m.cols = make([]Column, n)
	case len(spec.Columns) != n:
		return nil, fmt.Errorf("shard: a shard returned %d columns, the merge expects %d", n, len(spec.Columns))
	default:
		m.cols = spec.Columns
	}
	if spec.Hidden < 0 || spec.Hidden > n {
		return nil, fmt.Errorf("shard: merge hides %d of %d columns", spec.Hidden, n)
	}
	m.width = n - spec.Hidden
	for i, c := range m.cols {
		if c.Func == Avg {
			m.numeric[i] = true // an average is a decimal number, however its sum arrived
		}
	}

	for _, o := range spec.Order {
		if o.Col < 0 {
			o.Col += n // counted from the end: the statement may select * ahead of it
		}
		if o.Col < 0 || o.Col >= n {
			return nil, fmt.Errorf("shard: merge orders by column %d of %d", o.Col, n)
		}
		m.order = append(m.order, o)
	}
	return m, nil
}

func numericColumns(s Source, n int) []bool {
	out := make([]bool, n)
	var names []string
	switch t := s.(type) {
	case typeNamer:
		names = t.TypeNames()
	case sqlTypes:
		types, err := t.ColumnTypes()
		if err != nil {
			return out
		}
		for _, ct := range types {
			names = append(names, ct.DatabaseTypeName())
		}
	default:
		// types unknown: values are compared by their Go type
	}
	for i := 0; i < n && i < len(names); i++ {
		switch strings.ToUpper(names[i]) {
		case "NUMERIC", "DECIMAL":
			out[i] = true
		default:
			// not a decimal text
		}
	}
	return out
}

// cursor reads one source a row at a time.
type cursor struct {
	src   Source
	idx   int
	width int
	row   []any // nil when exhausted
	ptrs  []any // scan destinations, reused: they are rewritten for every row
}

func (c *cursor) advance() error {
	if !c.src.Next() {
		c.row = nil
		return c.src.Err()
	}
	vals := make([]any, c.width)
	if c.ptrs == nil {
		c.ptrs = make([]any, c.width)
	}
	for i := range vals {
		c.ptrs[i] = &vals[i]
	}
	if err := c.src.Scan(c.ptrs...); err != nil {
		return err
	}
	c.row = vals
	return nil
}

// ---- streaming -------------------------------------------------------------

// prime reads the first row of each source.
func (m *merger) prime() error {
	m.ordered = len(m.order) > 0
	if m.spec.Distinct {
		m.seen = map[string]struct{}{}
	}
	if !m.ordered {
		return nil
	}
	m.h = &cursorHeap{m: m}
	for _, c := range m.cursors {
		if err := c.advance(); err != nil {
			return err
		}
		if c.row != nil {
			m.h.items = append(m.h.items, c)
		}
	}
	heap.Init(m.h)
	return m.h.err
}

// raw returns the next row in merged order, or nil at the end.
func (m *merger) raw() ([]any, error) {
	if m.ordered {
		if m.h.Len() == 0 {
			return nil, nil
		}
		c := m.h.items[0]
		row := c.row
		if err := c.advance(); err != nil {
			return nil, err
		}
		switch {
		case c.row == nil:
			heap.Pop(m.h)
		default:
			heap.Fix(m.h, 0)
		}
		return row, m.h.err
	}
	for m.at < len(m.cursors) {
		c := m.cursors[m.at]
		if c.row == nil {
			if err := c.advance(); err != nil {
				return nil, err
			}
		}
		if c.row == nil {
			m.at++
			continue
		}
		row := c.row
		c.row = nil
		return row, nil
	}
	return nil, nil
}

// stream returns the next row of the result: merged order, then DISTINCT,
// OFFSET and LIMIT.
func (m *merger) stream() ([]any, error) {
	for {
		if m.spec.Limit != nil && m.emitted >= *m.spec.Limit {
			return nil, nil
		}
		row, err := m.raw()
		if err != nil || row == nil {
			return nil, err
		}
		row = row[:m.width]
		if m.seen != nil {
			k := rowKey(row, nil)
			if _, dup := m.seen[k]; dup {
				continue
			}
			if m.opts.MaxRows > 0 && len(m.seen) >= m.opts.MaxRows {
				return nil, fmt.Errorf("%w: DISTINCT would hold more than %d distinct rows (MaxMergeRows); narrow the query", ErrLimitExceeded, m.opts.MaxRows)
			}
			m.seen[k] = struct{}{}
		}
		if m.skipped < m.spec.Offset {
			m.skipped++
			continue
		}
		m.emitted++
		return row, nil
	}
}

type cursorHeap struct {
	m     *merger
	items []*cursor
	err   error
}

func (h *cursorHeap) Len() int { return len(h.items) }
func (h *cursorHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}
func (h *cursorHeap) Push(x any) { h.items = append(h.items, x.(*cursor)) }
func (h *cursorHeap) Pop() any {
	n := len(h.items)
	c := h.items[n-1]
	h.items = h.items[:n-1]
	return c
}
func (h *cursorHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	n, err := h.m.compareRows(a.row, b.row)
	if err != nil && h.err == nil {
		h.err = err
	}
	if n == 0 {
		return a.idx < b.idx // stable across shards
	}
	return n < 0
}

// compareRows orders two rows by the spec's ORDER BY.
func (m *merger) compareRows(a, b []any) (int, error) {
	for _, o := range m.order {
		x, y := a[o.Col], b[o.Col]
		var n int
		switch {
		case x == nil && y == nil:
			continue
		case x == nil:
			n = 1
			if o.NullsFirst {
				n = -1
			}
		case y == nil:
			n = -1
			if o.NullsFirst {
				n = 1
			}
		default:
			c, err := compare(x, y, m.numeric[o.Col])
			if err != nil {
				return 0, err
			}
			n = c
			if o.Desc {
				n = -n
			}
		}
		if n != 0 {
			return n, nil
		}
	}
	return 0, nil
}

func (m *merger) fromSlice(rows [][]any) func() ([]any, error) {
	i := 0
	return func() ([]any, error) {
		if i >= len(rows) {
			return nil, nil
		}
		i++
		return rows[i-1], nil
	}
}

// ---- aggregation -----------------------------------------------------------

type group struct{ row []any }

// aggregate reads every row, combines the groups, and returns the final rows:
// filtered by HAVING, sorted, de-duplicated and cut by OFFSET and LIMIT.
func (m *merger) aggregate() ([][]any, error) {
	keys := []int{} // non-nil: with no key columns, every row is in the one group
	for i, c := range m.cols {
		if c.Key {
			keys = append(keys, i)
		}
	}

	groups := map[string]*group{}
	var order []*group // first-seen order, so the result is deterministic
	for _, c := range m.cursors {
		for {
			if err := c.advance(); err != nil {
				return nil, err
			}
			if c.row == nil {
				break
			}
			k := rowKey(c.row, keys)
			g, ok := groups[k]
			switch {
			case !ok:
				if m.opts.MaxRows > 0 && len(groups) >= m.opts.MaxRows {
					return nil, fmt.Errorf("%w: more than %d groups (MaxMergeRows); narrow the query or add a more selective condition", ErrLimitExceeded, m.opts.MaxRows)
				}
				g = &group{row: c.row}
				groups[k] = g
				order = append(order, g)
			default:
				if err := m.combine(g.row, c.row); err != nil {
					return nil, err
				}
			}
		}
	}

	rows := make([][]any, 0, len(order))
	for _, g := range order {
		for i, col := range m.cols {
			if col.Func != Avg {
				continue
			}
			v, err := average(g.row[i], g.row[col.Count])
			if err != nil {
				return nil, err
			}
			g.row[i] = v
		}
		if m.spec.Having != nil {
			ok, err := m.spec.Having.holds(g.row, m.numeric)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		rows = append(rows, g.row)
	}

	if len(m.order) > 0 {
		var sortErr error
		sort.SliceStable(rows, func(i, j int) bool {
			n, err := m.compareRows(rows[i], rows[j])
			if err != nil && sortErr == nil {
				sortErr = err
			}
			return n < 0
		})
		if sortErr != nil {
			return nil, sortErr
		}
	}

	out := rows[:0]
	var seen map[string]struct{}
	if m.spec.Distinct {
		seen = map[string]struct{}{}
	}
	var skipped int64
	for _, row := range rows {
		row = row[:m.width]
		if seen != nil {
			k := rowKey(row, nil)
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
		}
		if skipped < m.spec.Offset {
			skipped++
			continue
		}
		if m.spec.Limit != nil && int64(len(out)) >= *m.spec.Limit {
			break
		}
		out = append(out, row)
	}
	return out, nil
}

// combine folds one shard's row into the group's running row.
func (m *merger) combine(into, row []any) error {
	for i, c := range m.cols {
		a, b := into[i], row[i]
		switch c.Func {
		case Pass:
			// every shard agrees on a group's key and dependent columns
		case Count, Sum, Avg:
			switch {
			case b == nil:
				// nothing to add
			case a == nil:
				into[i] = b
			default:
				v, err := add(a, b)
				if err != nil {
					return fmt.Errorf("column %d (%s): %w", i, m.names[i], err)
				}
				into[i] = v
			}
		case Min, Max:
			switch {
			case b == nil:
				// nothing to compare
			case a == nil:
				into[i] = b
			default:
				n, err := compare(b, a, m.numeric[i])
				if err != nil {
					return fmt.Errorf("column %d (%s): %w", i, m.names[i], err)
				}
				if (c.Func == Min && n < 0) || (c.Func == Max && n > 0) {
					into[i] = b
				}
			}
		default:
			return fmt.Errorf("shard: unknown merge function %v for column %d", c.Func, i)
		}
	}
	return nil
}

// truth is SQL's three-valued logic: a comparison with NULL is neither true nor
// false, and NOT of it is still neither.
type truth int8

const (
	isFalse truth = iota
	isTrue
	isUnknown
)

// holds reports whether the merged row satisfies the condition. Like WHERE and
// HAVING in SQL, only true keeps a row.
func (c *Cond) holds(row []any, numeric []bool) (bool, error) {
	t, err := c.eval(row, numeric)
	return t == isTrue, err
}

func (c *Cond) eval(row []any, numeric []bool) (truth, error) {
	switch c.Op {
	case "and", "or":
		// AND is false if any operand is; OR is true if any operand is.
		decisive, other := isFalse, isTrue
		if c.Op == "or" {
			decisive, other = isTrue, isFalse
		}
		result := other
		for i := range c.Args {
			t, err := c.Args[i].eval(row, numeric)
			switch {
			case err != nil:
				return isUnknown, err
			case t == decisive:
				return decisive, nil
			case t == isUnknown:
				result = isUnknown
			default:
				// the neutral value: keep looking
			}
		}
		return result, nil
	case "not":
		if len(c.Args) != 1 {
			return isUnknown, errors.New("shard: HAVING NOT needs one operand")
		}
		t, err := c.Args[0].eval(row, numeric)
		switch t {
		case isTrue:
			return isFalse, err
		case isFalse:
			return isTrue, err
		default:
			return isUnknown, err
		}
	case "=", "<>", "<", "<=", ">", ">=":
		v := row[c.Col]
		if v == nil || c.Value == nil {
			return isUnknown, nil
		}
		n, err := compare(v, c.Value, numeric[c.Col])
		if err != nil {
			return isUnknown, err
		}
		var ok bool
		switch c.Op {
		case "=":
			ok = n == 0
		case "<>":
			ok = n != 0
		case "<":
			ok = n < 0
		case "<=":
			ok = n <= 0
		case ">":
			ok = n > 0
		default:
			ok = n >= 0
		}
		if ok {
			return isTrue, nil
		}
		return isFalse, nil
	default:
		return isUnknown, fmt.Errorf("shard: unknown HAVING operator %q", c.Op)
	}
}

// rowKey encodes the chosen columns of a row (all of them if cols is nil) so
// that equal values give equal keys and different values never do.
func rowKey(row []any, cols []int) string {
	var b strings.Builder
	put := func(v any) {
		switch x := v.(type) {
		case nil:
			b.WriteString("n;")
		case string:
			b.WriteString("s" + strconv.Itoa(len(x)) + ":" + x)
		case []byte:
			b.WriteString("b" + strconv.Itoa(len(x)) + ":" + string(x))
		case int64:
			b.WriteString("i" + strconv.FormatInt(x, 10) + ";")
		default:
			s := fmt.Sprintf("%T:%v", v, v)
			b.WriteString("o" + strconv.Itoa(len(s)) + ":" + s)
		}
	}
	if cols == nil {
		for _, v := range row {
			put(v)
		}
		return b.String()
	}
	for _, i := range cols {
		put(row[i])
	}
	return b.String()
}
