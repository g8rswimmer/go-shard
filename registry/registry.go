// Package registry holds table metadata: which tables are sharded, which are
// colocated with another table, and which are global.
//
// A registry is built once at startup with New and is immutable afterwards, so
// it is safe for concurrent use without locks.
//
//	reg, err := registry.New(
//	    registry.Sharded("profiles", registry.Key("id")),
//	    registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
//	    registry.Global("countries"),
//	)
//
// Table and column names are matched exactly as declared. PostgreSQL folds
// unquoted identifiers to lower case, so declare them in lower case.
package registry

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrInvalid is wrapped by every error returned from New. The message lists
// every problem found, not just the first.
var ErrInvalid = errors.New("invalid registry")

// Kind says how a table is placed across shards.
type Kind int

const (
	// KindSharded tables are partitioned across shards by their shard key.
	KindSharded Kind = iota + 1
	// KindColocated tables live on the same shard as their parent row, because
	// they carry the parent's shard key.
	KindColocated
	// KindGlobal tables are replicated to every shard.
	KindGlobal
)

func (k Kind) String() string {
	switch k {
	case KindSharded:
		return "sharded"
	case KindColocated:
		return "colocated"
	case KindGlobal:
		return "global"
	default:
		return fmt.Sprintf("Kind(%d)", int(k))
	}
}

// KeyType is the declared type of a shard key column. It is optional; a
// colocated table must not declare a type that differs from its root table's.
type KeyType int

const (
	// KeyTypeUnset means no type was declared.
	KeyTypeUnset KeyType = iota
	KeyInt
	KeyUUID
	KeyString
)

func (t KeyType) String() string {
	switch t {
	case KeyTypeUnset:
		return "unset"
	case KeyInt:
		return "int"
	case KeyUUID:
		return "uuid"
	case KeyString:
		return "string"
	default:
		return fmt.Sprintf("KeyType(%d)", int(t))
	}
}

// Table is the resolved metadata for one table.
type Table struct {
	Name string
	Kind Kind
	// KeyCol is the shard key column. Empty for global tables.
	KeyCol string
	// KeyType is the declared key type, inherited from the root table when a
	// colocated table does not declare its own. May be KeyTypeUnset.
	KeyType KeyType
	// Parent is the table this one is colocated with. Empty unless colocated.
	Parent string
	// Group is the name of the root sharded table. Tables with the same group
	// are colocated: rows with equal shard keys are on the same shard. Empty
	// for global tables.
	Group string
}

// Decl declares one table. Build it with Sharded, Colocated or Global.
type Decl struct {
	name    string
	kind    Kind
	key     string
	keyType KeyType
	parent  string
}

// Option customizes a table declaration.
type Option func(*Decl)

// Key sets the shard key column of a sharded or colocated table.
func Key(column string) Option { return func(d *Decl) { d.key = column } }

// Type declares the type of the shard key column.
func Type(t KeyType) Option { return func(d *Decl) { d.keyType = t } }

// With names the parent table a colocated table is placed with.
func With(parent string) Option { return func(d *Decl) { d.parent = parent } }

// Sharded declares a table partitioned across shards. Requires Key.
func Sharded(name string, opts ...Option) Decl { return newDecl(name, KindSharded, opts) }

// Colocated declares a table stored on the same shard as its parent. Requires
// With and Key.
func Colocated(name string, opts ...Option) Decl { return newDecl(name, KindColocated, opts) }

// Global declares a table replicated to every shard. Takes no options.
func Global(name string, opts ...Option) Decl { return newDecl(name, KindGlobal, opts) }

func newDecl(name string, kind Kind, opts []Option) Decl {
	d := Decl{name: name, kind: kind}
	for _, o := range opts {
		o(&d)
	}
	return d
}

// Registry is an immutable set of validated table declarations.
type Registry struct {
	tables map[string]Table
}

// New validates the declarations and returns a registry. The error wraps
// ErrInvalid and lists every problem found.
func New(decls ...Decl) (*Registry, error) {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	byName := make(map[string]Decl, len(decls))
	for _, d := range decls {
		switch {
		case strings.TrimSpace(d.name) == "":
			add("a table has an empty name")
			continue
		case d.kind == 0:
			add("table %q: not declared with Sharded, Colocated or Global", d.name)
			continue
		default:
			// a usable declaration; checked below
		}
		if _, dup := byName[d.name]; dup {
			add("table %q is declared more than once", d.name)
			continue
		}
		byName[d.name] = d
	}

	for _, d := range decls {
		if byName[d.name].kind != d.kind { // skipped duplicate or invalid declaration
			continue
		}
		validateShape(d, add)
	}

	tables := make(map[string]Table, len(byName))
	for _, d := range decls {
		if _, ok := byName[d.name]; !ok || tables[d.name].Name != "" {
			continue
		}
		t := Table{Name: d.name, Kind: d.kind, KeyCol: d.key, KeyType: d.keyType, Parent: d.parent}
		if d.kind != KindGlobal {
			root, ok := resolveRoot(d, byName, add)
			if ok {
				t.Group = root.name
				switch {
				case d.keyType == KeyTypeUnset:
					t.KeyType = root.keyType
				case root.keyType != KeyTypeUnset && root.keyType != d.keyType:
					add("table %q: key type %s differs from root table %q (%s)", d.name, d.keyType, root.name, root.keyType)
				default:
					// declared type matches the root's, or the root declares none
				}
			}
		}
		tables[d.name] = t
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		return nil, fmt.Errorf("%w:\n  - %s", ErrInvalid, strings.Join(problems, "\n  - "))
	}
	return &Registry{tables: tables}, nil
}

// validateShape checks the options given to a single declaration.
func validateShape(d Decl, add func(string, ...any)) {
	switch d.kind {
	case KindSharded:
		if d.key == "" {
			add("sharded table %q needs a shard key: use registry.Key", d.name)
		}
		if d.parent != "" {
			add("sharded table %q cannot have a parent; use Colocated for %q", d.name, d.parent)
		}
	case KindColocated:
		if d.parent == "" {
			add("colocated table %q needs a parent: use registry.With", d.name)
		}
		if d.key == "" {
			add("colocated table %q needs a shard key: use registry.Key", d.name)
		}
		if d.parent == d.name {
			add("colocated table %q cannot be colocated with itself", d.name)
		}
	case KindGlobal:
		if d.key != "" || d.parent != "" || d.keyType != KeyTypeUnset {
			add("global table %q takes no options: it is replicated to every shard", d.name)
		}
	default:
		// kind was checked in New
	}
}

// resolveRoot follows parents to the root sharded table. It reports a problem
// and returns false for a missing parent, a global parent or a cycle.
func resolveRoot(d Decl, byName map[string]Decl, add func(string, ...any)) (Decl, bool) {
	seen := map[string]bool{d.name: true}
	cur := d
	for cur.kind == KindColocated {
		parent, ok := byName[cur.parent]
		switch {
		case cur.parent == "":
			return Decl{}, false // already reported by validateShape
		case !ok:
			add("colocated table %q: parent %q is not declared", cur.name, cur.parent)
			return Decl{}, false
		case parent.kind == KindGlobal:
			add("colocated table %q: parent %q is global; global tables have no shard key", cur.name, cur.parent)
			return Decl{}, false
		case seen[parent.name]:
			add("colocated table %q is part of a cycle through %q", d.name, parent.name)
			return Decl{}, false
		default:
			// parent is a sharded or colocated table; keep walking up
		}
		seen[parent.name] = true
		cur = parent
	}
	return cur, cur.kind == KindSharded && cur.key != ""
}

// Table returns the metadata for a table.
func (r *Registry) Table(name string) (Table, bool) {
	t, ok := r.tables[name]
	return t, ok
}

// Tables returns all tables sorted by name.
func (r *Registry) Tables() []Table {
	out := make([]Table, 0, len(r.tables))
	for _, t := range r.tables {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
