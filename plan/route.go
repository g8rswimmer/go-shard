package plan

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
)

var (
	// ErrShardKeyRequired is returned when a statement cannot be routed
	// because it does not say which shard it belongs to.
	ErrShardKeyRequired = errors.New("shard: no shard key")

	// ErrCrossShardJoin is returned when a statement combines tables that are
	// not guaranteed to be on the same shard.
	ErrCrossShardJoin = errors.New("shard: cross-shard join")

	// ErrUnknownTable is returned for a table that is not in the registry.
	ErrUnknownTable = errors.New("shard: table not in the registry")
)

// Options adjust routing.
type Options struct {
	// AnyShard chooses the shard for a statement that every shard can answer
	// equally well, such as a read of global tables. The default is the first
	// shard. Spread the load by returning a different shard each call.
	AnyShard func() router.ShardID
}

func (o Options) anyShard(r router.Router) router.ShardID {
	if o.AnyShard != nil {
		return o.AnyShard()
	}
	return r.All()[0]
}

// Route decides which shards an analyzed statement runs on.
//
// The rules, in order:
//
//   - Statements that are not reads or writes of tables (DDL) are refused.
//   - Reads of global tables only, or of no tables, go to any one shard.
//   - Writes to a global table go to every shard.
//   - All other tables must be in one colocation group.
//   - The tables are joined into classes through equality on their shard key
//     (JOIN ... ON a.key = b.key, USING (key), or a correlated subquery). A
//     class with conditions like key = 5 or key IN (...) is limited to the
//     shards those keys hash to. A statement with one class goes to its
//     shards; one with several classes is allowed only if every class is
//     pinned to the same single shard.
//   - Anything else cannot be proven to find all its rows on the chosen
//     shard(s), so it is refused rather than guessed.
func Route(a analyze.Analysis, reg *registry.Registry, r router.Router, opts Options) (Plan, error) {
	if a.Op == analyze.OpOther {
		return Plan{}, fmt.Errorf("%w: %s statements are not routed automatically; run schema changes with db.WithAllShards() or the migrations runner", analyze.ErrUnsupportedQuery, a.Kind)
	}

	rt := &routing{a: a, reg: reg, r: r}
	if err := rt.resolveTables(); err != nil {
		return Plan{}, err
	}

	// Statements that touch no sharded table.
	if len(rt.sharded) == 0 {
		return rt.globalOnly(opts)
	}
	if a.Op.IsWrite() && rt.insts[a.Target].tbl.Kind == registry.KindGlobal {
		return Plan{}, fmt.Errorf("%w: %s to global table %q also uses sharded tables; split it into separate statements",
			analyze.ErrUnsupportedQuery, a.Op, rt.insts[a.Target].tbl.Name)
	}
	if a.Op == analyze.OpInsert {
		t := rt.insts[a.Target].tbl
		return Plan{}, fmt.Errorf("%w: INSERT into %q cannot be routed from the SQL yet; use db.WithShardKey(<%s value>)", ErrShardKeyRequired, t.Name, t.KeyCol)
	}

	if err := rt.checkGroup(); err != nil {
		return Plan{}, err
	}
	if err := rt.link(); err != nil {
		return Plan{}, err
	}
	return rt.decide(opts)
}

// inst is one table use with its registry entry.
type inst struct {
	ref    analyze.TableRef
	tbl    registry.Table
	system bool // pg_catalog / information_schema: ignored
}

type routing struct {
	a   analyze.Analysis
	reg *registry.Registry
	r   router.Router

	insts   []inst // indexed by TableRef.ID
	sharded []int  // IDs of sharded and colocated uses
	parent  map[int]int
	sets    map[int]*shardSet // class root -> allowed shards
}

// shardSet is a set of shards a class is limited to.
type shardSet struct {
	ids  []router.ShardID // sorted
	from string           // description for Explain
}

func (rt *routing) resolveTables() error {
	rt.insts = make([]inst, len(rt.a.Tables))
	for _, ref := range rt.a.Tables {
		in := inst{ref: ref}
		switch ref.Schema {
		case "pg_catalog", "information_schema":
			in.system = true
		default:
			t, ok := lookup(rt.reg, ref)
			if !ok {
				return fmt.Errorf("%w: %q is not registered; declare it with registry.Sharded, registry.Colocated or registry.Global", ErrUnknownTable, qualified(ref))
			}
			in.tbl = t
			if t.Kind != registry.KindGlobal {
				rt.sharded = append(rt.sharded, ref.ID)
			}
		}
		rt.insts[ref.ID] = in
	}
	return nil
}

// lookup finds a table in the registry. A schema-qualified name is tried as
// written ("sales.orders"); the public schema may also be omitted.
func lookup(reg *registry.Registry, ref analyze.TableRef) (registry.Table, bool) {
	if ref.Schema != "" {
		if t, ok := reg.Table(ref.Schema + "." + ref.Name); ok {
			return t, true
		}
		if ref.Schema != "public" {
			return registry.Table{}, false
		}
	}
	return reg.Table(ref.Name)
}

func qualified(ref analyze.TableRef) string {
	if ref.Schema != "" {
		return ref.Schema + "." + ref.Name
	}
	return ref.Name
}

// globalOnly routes statements that use no sharded table.
func (rt *routing) globalOnly(opts Options) (Plan, error) {
	if rt.a.Op.IsWrite() {
		t := rt.insts[rt.a.Target]
		switch {
		case t.system:
			return Plan{}, fmt.Errorf("%w: writes to %s are not routed", analyze.ErrUnsupportedQuery, qualified(t.ref))
		default:
			all := rt.r.All()
			return Plan{
				Targets:  all,
				Strategy: All,
				Reason:   fmt.Sprintf("%s global table %q: every shard holds a copy", rt.a.Op, t.tbl.Name),
			}, nil
		}
	}
	return Plan{
		Targets:  []router.ShardID{opts.anyShard(rt.r)},
		Strategy: Single,
		Reason:   "only global tables (or none) are read: every shard can answer",
	}, nil
}

// checkGroup requires all sharded tables to be in one colocation group.
func (rt *routing) checkGroup() error {
	first := rt.insts[rt.sharded[0]].tbl
	for _, id := range rt.sharded[1:] {
		if t := rt.insts[id].tbl; t.Group != first.Group {
			return fmt.Errorf("%w: %q (group %q) and %q (group %q) are not colocated, so their rows may be on different shards; "+
				"colocate them with registry.Colocated, make one global, or run two queries and join in your application",
				ErrCrossShardJoin, first.Name, first.Group, t.Name, t.Group)
		}
	}
	return nil
}

// link joins table uses into classes through shard-key equality and collects
// the shards each class is limited to.
func (rt *routing) link() error {
	rt.parent = map[int]int{}
	for _, id := range rt.sharded {
		rt.parent[id] = id
	}

	for _, eq := range rt.a.Equalities {
		l, lok := rt.keyColumn(eq.Left, false)
		rr, rok := rt.keyColumn(eq.Right, false)
		if lok && rok {
			rt.union(l, rr)
		}
	}
	for _, u := range rt.a.Usings {
		for _, col := range u.Columns {
			left, right := rt.withKey(u.Left, col), rt.withKey(u.Right, col)
			if len(left) > 0 && len(right) > 0 {
				for _, id := range append(left[1:], right...) {
					rt.union(left[0], id)
				}
			}
		}
	}

	type pending struct {
		inst int
		set  *shardSet
	}
	var bound []pending
	for _, b := range rt.a.Bindings {
		id, ok := rt.keyColumn(b.Column, true)
		if !ok {
			continue
		}
		t := rt.insts[id].tbl
		vals := make([]any, len(b.Values))
		for i, v := range b.Values {
			c, err := coerceKey(v, t.KeyType)
			if err != nil {
				return fmt.Errorf("shard key %s.%s: %w", t.Name, t.KeyCol, err)
			}
			vals[i] = c
		}
		ids, err := rt.r.ShardsFor(vals)
		if err != nil {
			return fmt.Errorf("shard key %s.%s: %w", t.Name, t.KeyCol, err)
		}
		bound = append(bound, pending{id, &shardSet{
			ids:  ids,
			from: fmt.Sprintf("%s.%s has %d key value(s)", t.Name, t.KeyCol, len(vals)),
		}})
	}

	rt.sets = map[int]*shardSet{}
	for _, p := range bound {
		root := rt.find(p.inst)
		if cur, ok := rt.sets[root]; ok {
			rt.sets[root] = &shardSet{ids: intersect(cur.ids, p.set.ids), from: cur.from + " and " + p.set.from}
			continue
		}
		rt.sets[root] = p.set
	}
	return nil
}

func (rt *routing) decide(opts Options) (Plan, error) {
	classes := rt.classes()

	// Conflicting key conditions (id = 1 AND id = 2) match nothing, whichever
	// shard runs it.
	for _, root := range classes {
		if s, ok := rt.sets[root]; ok && len(s.ids) == 0 {
			return Plan{
				Targets:  []router.ShardID{opts.anyShard(rt.r)},
				Strategy: Single,
				Reason:   "the shard key conditions match no rows (" + s.from + "), so any shard gives the same empty answer",
			}, nil
		}
	}

	if len(classes) == 1 {
		root := classes[0]
		s, ok := rt.sets[root]
		if !ok {
			return Plan{}, rt.keyRequired(root)
		}
		return rt.plan(s.ids, "routed by key: "+s.from), nil
	}

	// Several unconnected groups of tables: only safe if all are pinned to the
	// same single shard.
	var pinned router.ShardID
	for i, root := range classes {
		s, ok := rt.sets[root]
		if !ok || len(s.ids) != 1 || (i > 0 && s.ids[0] != pinned) {
			return Plan{}, rt.notConnected(classes)
		}
		pinned = s.ids[0]
	}
	return rt.plan([]router.ShardID{pinned}, "every group of tables is pinned to the same shard"), nil
}

func (rt *routing) plan(ids []router.ShardID, reason string) Plan {
	s := Multi
	switch {
	case len(ids) == 1:
		s = Single
	case len(ids) == len(rt.r.All()):
		s = All
	default:
		// some, but not all, shards
	}
	return Plan{Targets: ids, Strategy: s, Reason: fmt.Sprintf("%s -> %v", reason, ids)}
}

func (rt *routing) keyRequired(root int) error {
	t := rt.insts[root].tbl
	hint := ""
	if len(rt.a.Notes) > 0 {
		hint = " (ignored for routing: " + strings.Join(rt.a.Notes, "; ") + ")"
	}
	return fmt.Errorf("%w: the query on %q does not say which shard it belongs to; add `%s = $n` or `%s IN (...)` as an AND-ed condition, "+
		"or use db.WithShardKey / db.WithAllShards%s", ErrShardKeyRequired, t.Name, t.KeyCol, t.KeyCol, hint)
}

func (rt *routing) notConnected(classes []int) error {
	var groups []string
	for _, root := range classes {
		var names []string
		for _, id := range rt.sharded {
			if rt.find(id) == root {
				names = append(names, qualified(rt.insts[id].ref))
			}
		}
		groups = append(groups, "{"+strings.Join(names, ", ")+"}")
	}
	return fmt.Errorf("%w: these tables are not tied together by their shard key, so matching rows may be on different shards: %s; "+
		"join on the shard key (ON a.<key> = b.<key>), pin them to the same key, or run separate queries and combine in your application",
		ErrCrossShardJoin, strings.Join(groups, " and "))
}

// classes returns the class roots, ordered by their first table use.
func (rt *routing) classes() []int {
	var roots []int
	seen := map[int]bool{}
	for _, id := range rt.sharded {
		if root := rt.find(id); !seen[root] {
			seen[root] = true
			roots = append(roots, root)
		}
	}
	return roots
}

func (rt *routing) find(id int) int {
	for rt.parent[id] != id {
		rt.parent[id] = rt.parent[rt.parent[id]]
		id = rt.parent[id]
	}
	return id
}

func (rt *routing) union(a, b int) {
	ra, rb := rt.find(a), rt.find(b)
	if ra != rb {
		if ra > rb {
			ra, rb = rb, ra
		}
		rt.parent[rb] = ra
	}
}

// withKey returns the sharded table uses among ids whose shard key column is col.
func (rt *routing) withKey(ids []int, col string) []int {
	var out []int
	for _, id := range ids {
		if in := rt.insts[id]; !in.system && in.tbl.Kind != registry.KindGlobal && in.tbl.KeyCol == col {
			out = append(out, id)
		}
	}
	return out
}

// keyColumn resolves a column reference to the sharded table use it is the
// shard key of. It returns false for any other column.
//
// With merged set, an unqualified name that several tables use as their shard
// key is accepted if those tables are already tied together: after
// JOIN ... USING (id), "id" names the single merged column. Call it with merged
// only once all links have been made.
func (rt *routing) keyColumn(ref analyze.ColumnRef, merged bool) (int, bool) {
	for scope := ref.Scope; scope >= 0; scope = rt.a.Scopes[scope] {
		var here []inst
		for _, in := range rt.insts {
			if in.ref.Scope == scope {
				here = append(here, in)
			}
		}

		if ref.Qualifier != "" {
			var match []inst
			for _, in := range here {
				if matches(in.ref, ref.Qualifier) {
					match = append(match, in)
				}
			}
			switch len(match) {
			case 0:
				continue // not defined at this level; look at the enclosing query
			case 1:
				in := match[0]
				if in.system || in.tbl.Kind == registry.KindGlobal || in.tbl.KeyCol != ref.Name {
					return 0, false
				}
				return in.ref.ID, true
			default:
				return 0, false // ambiguous
			}
		}

		// Unqualified: if this level has a table keyed by this column name it
		// must be that one (otherwise the SQL would be ambiguous). If the level
		// has tables but none is keyed by it, the column may belong to one of
		// them, and we cannot see their columns, so we must not look further out.
		var cands []inst
		for _, in := range here {
			if !in.system && in.tbl.Kind != registry.KindGlobal && in.tbl.KeyCol == ref.Name {
				cands = append(cands, in)
			}
		}
		switch {
		case len(cands) == 1:
			return cands[0].ref.ID, true
		case len(cands) > 1 && merged && rt.sameClass(cands):
			return cands[0].ref.ID, true
		case len(cands) > 1 || len(here) > 0:
			return 0, false
		default:
			continue // no tables at this level (SELECT without FROM)
		}
	}
	return 0, false
}

func (rt *routing) sameClass(cands []inst) bool {
	root := rt.find(cands[0].ref.ID)
	for _, c := range cands[1:] {
		if rt.find(c.ref.ID) != root {
			return false
		}
	}
	return true
}

func matches(ref analyze.TableRef, qualifier string) bool {
	if ref.Alias != "" {
		return qualifier == ref.Alias
	}
	return qualifier == ref.Name || (ref.Schema != "" && qualifier == ref.Schema+"."+ref.Name)
}

func intersect(a, b []router.ShardID) []router.ShardID {
	in := map[router.ShardID]bool{}
	for _, id := range b {
		in[id] = true
	}
	var out []router.ShardID
	for _, id := range a {
		if in[id] {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// coerceKey converts a value found in SQL or arguments to the type of the key
// column, so that the same key always hashes the same way. A bigint key
// compared with the text '42' must route like the number 42.
func coerceKey(v any, kt registry.KeyType) (any, error) {
	if v == nil {
		return nil, router.ErrNilKey
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, router.ErrNilKey
		}
		rv = rv.Elem()
	}
	v = rv.Interface()

	switch kt {
	case registry.KeyInt:
		switch rv.Kind() {
		case reflect.String:
			n, err := strconv.ParseInt(rv.String(), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%q is not an integer, but the key column is declared int", rv.String())
			}
			return n, nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return v, nil
		default:
			return nil, fmt.Errorf("%w: %T for a key column declared int", router.ErrUnsupportedKey, v)
		}
	case registry.KeyString:
		if rv.Kind() != reflect.String {
			return nil, fmt.Errorf("%w: %T for a key column declared string", router.ErrUnsupportedKey, v)
		}
		return v, nil
	case registry.KeyUUID:
		switch {
		case rv.Kind() == reflect.String && router.IsUUIDString(rv.String()):
			return v, nil
		case rv.Kind() == reflect.Array && rv.Len() == 16:
			return v, nil
		default:
			return nil, fmt.Errorf("%v is not a UUID, but the key column is declared uuid", v)
		}
	default:
		// registry.New requires a key type on every sharded table, so this is
		// not reached for a registered table. If it ever were, the key is
		// hashed as given.
		return v, nil
	}
}
