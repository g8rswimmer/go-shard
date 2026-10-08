package registry

import (
	"errors"
	"strings"
	"testing"
)

func TestNewResolvesTables(t *testing.T) {
	reg, err := New(
		Sharded("profiles", Key("id"), Type(KeyUUID)),
		Colocated("addresses", With("profiles"), Key("profile_id")),
		Colocated("address_notes", With("addresses"), Key("profile_id")),
		Global("countries"),
	)
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]Table{
		"profiles":      {Name: "profiles", Kind: KindSharded, KeyCol: "id", KeyType: KeyUUID, Group: "profiles"},
		"addresses":     {Name: "addresses", Kind: KindColocated, KeyCol: "profile_id", KeyType: KeyUUID, Parent: "profiles", Group: "profiles"},
		"address_notes": {Name: "address_notes", Kind: KindColocated, KeyCol: "profile_id", KeyType: KeyUUID, Parent: "addresses", Group: "profiles"},
		"countries":     {Name: "countries", Kind: KindGlobal},
	}
	for name, w := range want {
		got, ok := reg.Table(name)
		if !ok {
			t.Errorf("Table(%q) not found", name)
			continue
		}
		if got != w {
			t.Errorf("Table(%q) = %+v, want %+v", name, got, w)
		}
	}
	if _, ok := reg.Table("missing"); ok {
		t.Error("Table(missing) should not be found")
	}
}

func TestChildCanDeclareMatchingType(t *testing.T) {
	_, err := New(
		Sharded("profiles", Key("id"), Type(KeyInt)),
		Colocated("addresses", With("profiles"), Key("profile_id"), Type(KeyInt)),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTablesSortedByName(t *testing.T) {
	reg, err := New(Global("zebra"), Sharded("mango", Key("id"), Type(KeyInt)), Global("apple"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tb := range reg.Tables() {
		names = append(names, tb.Name)
	}
	if got := strings.Join(names, ","); got != "apple,mango,zebra" {
		t.Errorf("Tables() order = %s", got)
	}
}

func TestEmptyRegistryIsValid(t *testing.T) {
	reg, err := New()
	if err != nil || len(reg.Tables()) != 0 {
		t.Fatalf("New() = %v, %v", reg, err)
	}
}

func TestNewRejectsInvalid(t *testing.T) {
	tests := []struct {
		name  string
		decls []Decl
		want  string
	}{
		{"empty name", []Decl{Global("")}, "empty name"},
		{"blank name", []Decl{Global("  ")}, "empty name"},
		{"decl without kind", []Decl{{name: "x"}}, `table "x": not declared with`},
		{"duplicate", []Decl{Global("a"), Global("a")}, `table "a" is declared more than once`},
		{"duplicate different kind", []Decl{Global("a"), Sharded("a", Key("id"), Type(KeyInt))}, `table "a" is declared more than once`},

		{"sharded without key", []Decl{Sharded("p", Type(KeyInt))}, `sharded table "p" needs a shard key`},
		{"sharded without key type", []Decl{Sharded("p", Key("id"))}, `sharded table "p" needs a key type`},
		{"sharded with an invalid key type", []Decl{Sharded("p", Key("id"), Type(KeyType(99)))}, `invalid key type 99`},
		{"colocated with an invalid key type", []Decl{Sharded("p", Key("id"), Type(KeyInt)), Colocated("a", With("p"), Key("k"), Type(KeyType(-1)))}, `invalid key type -1`},
		{"sharded with parent", []Decl{Sharded("p", Key("id"), Type(KeyInt), With("x"))}, `sharded table "p" cannot have a parent`},

		{"colocated without parent", []Decl{Colocated("a", Key("pid"))}, `colocated table "a" needs a parent`},
		{"colocated without key", []Decl{Sharded("p", Key("id"), Type(KeyInt)), Colocated("a", With("p"))}, `colocated table "a" needs a shard key`},
		{"colocated with itself", []Decl{Colocated("a", With("a"), Key("pid"))}, "cannot be colocated with itself"},
		{"missing parent", []Decl{Colocated("a", With("nope"), Key("pid"))}, `parent "nope" is not declared`},
		{"global parent", []Decl{Global("c"), Colocated("a", With("c"), Key("pid"))}, `parent "c" is global`},
		{"cycle", []Decl{
			Colocated("a", With("b"), Key("k")),
			Colocated("b", With("a"), Key("k")),
		}, "part of a cycle"},
		{"longer cycle", []Decl{
			Colocated("a", With("b"), Key("k")),
			Colocated("b", With("c"), Key("k")),
			Colocated("c", With("a"), Key("k")),
		}, "part of a cycle"},

		{"global with key", []Decl{Global("c", Key("id"))}, `global table "c" takes no options`},
		{"global with parent", []Decl{Sharded("p", Key("id"), Type(KeyInt)), Global("c", With("p"))}, `global table "c" takes no options`},
		{"global with type", []Decl{Global("c", Type(KeyInt))}, `global table "c" takes no options`},

		{"type mismatch", []Decl{
			Sharded("p", Key("id"), Type(KeyUUID)),
			Colocated("a", With("p"), Key("pid"), Type(KeyInt)),
		}, `key type int differs from root table "p" (uuid)`},
		{"type mismatch through chain", []Decl{
			Sharded("p", Key("id"), Type(KeyUUID)),
			Colocated("a", With("p"), Key("pid")),
			Colocated("b", With("a"), Key("pid"), Type(KeyString)),
		}, `key type string differs from root table "p" (uuid)`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg, err := New(tc.decls...)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("error = %v, want ErrInvalid", err)
			}
			if reg != nil {
				t.Error("registry should be nil on error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not mention %q:\n%v", tc.want, err)
			}
		})
	}
}

func TestNewReportsEveryProblem(t *testing.T) {
	_, err := New(
		Sharded("p"),
		Colocated("a", With("nope"), Key("pid")),
		Global("c", Key("id")),
	)
	if !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, w := range []string{"needs a shard key", "needs a key type", "is not declared", "takes no options"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("error does not mention %q:\n%v", w, err)
		}
	}
}

func TestStrings(t *testing.T) {
	if KindColocated.String() != "colocated" || KeyUUID.String() != "uuid" || KeyTypeUnset.String() != "unset" {
		t.Error("unexpected String() output")
	}
	if !strings.HasPrefix(Kind(99).String(), "Kind(") || !strings.HasPrefix(KeyType(99).String(), "KeyType(") {
		t.Error("unknown values should print their number")
	}
}

func TestMissingKeyTypeExplainsWhy(t *testing.T) {
	_, err := New(Sharded("profiles", Key("id")))
	for _, w := range []string{`"profiles"`, "registry.Type", "KeyInt", `42 and "42"`} {
		if err == nil || !strings.Contains(err.Error(), w) {
			t.Errorf("error should mention %q: %v", w, err)
		}
	}
}

func TestColocatedTablesNeedNoKeyTypeOfTheirOwn(t *testing.T) {
	reg, err := New(
		Sharded("p", Key("id"), Type(KeyString)),
		Colocated("a", With("p"), Key("pid")),
		Colocated("b", With("a"), Key("pid")),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"p", "a", "b"} {
		if tb, _ := reg.Table(n); tb.KeyType != KeyString {
			t.Errorf("%s key type = %v, want string (inherited from the root)", n, tb.KeyType)
		}
	}
	if tb, _ := mustGlobal(t).Table("g"); tb.KeyType != KeyTypeUnset {
		t.Errorf("a global table has no key type, got %v", tb.KeyType)
	}
}

func mustGlobal(t *testing.T) *Registry {
	t.Helper()
	reg, err := New(Global("g"))
	if err != nil {
		t.Fatal(err)
	}
	return reg
}
