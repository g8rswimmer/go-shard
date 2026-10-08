package exec

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestValidTableName(t *testing.T) {
	for name, want := range map[string]bool{
		"go_shard_idempotency_keys": true,
		"keys":                      true,
		"_keys$1":                   true,
		"app.idempotency":           true,
		"":                          false,
		"1keys":                     false,
		"a b":                       false,
		`a"; DROP TABLE x; --`:      false,
		"a.b.c":                     false,
		".a":                        false,
		"a.":                        false,
	} {
		if got := ValidTableName(name); got != want {
			t.Errorf("ValidTableName(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestQuoteTable(t *testing.T) {
	if got := quoteTable("keys"); got != `"keys"` {
		t.Errorf("got %s", got)
	}
	if got := quoteTable("app.keys"); got != `"app"."keys"` {
		t.Errorf("got %s", got)
	}
}

func TestIdempotencyDDL(t *testing.T) {
	ddl := IdempotencyDDL("app.keys")
	for _, w := range []string{
		`CREATE TABLE IF NOT EXISTS "app"."keys"`, `"key"`, "PRIMARY KEY", "request_hash", "rows_affected",
		"created_at", `CREATE INDEX IF NOT EXISTS "keys_created_at_idx" ON "app"."keys" (created_at)`,
	} {
		if !strings.Contains(ddl, w) {
			t.Errorf("DDL is missing %q:\n%s", w, ddl)
		}
	}
}

func TestRequestHash(t *testing.T) {
	base := requestHash("INSERT x", []any{1, "a"})
	if base != requestHash("INSERT x", []any{1, "a"}) {
		t.Error("the same statement must hash the same")
	}
	for name, other := range map[string]string{
		"different SQL":      requestHash("INSERT y", []any{1, "a"}),
		"different argument": requestHash("INSERT x", []any{2, "a"}),
		"different order":    requestHash("INSERT x", []any{"a", 1}),
		"different type":     requestHash("INSERT x", []any{"1", "a"}),
		"fewer arguments":    requestHash("INSERT x", []any{1}),
		"arguments run on":   requestHash("INSERT x", []any{"1a"}),
	} {
		if other == base {
			t.Errorf("%s must change the hash", name)
		}
	}
}

func TestRequestHashIgnoresTimeRepresentation(t *testing.T) {
	loc := time.FixedZone("x", 3600)
	a := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	b := a.In(loc)
	withMonotonic := time.Now()
	if requestHash("q", []any{a}) != requestHash("q", []any{b}) {
		t.Error("the same instant in another zone must hash the same")
	}
	if requestHash("q", []any{withMonotonic}) != requestHash("q", []any{withMonotonic.Round(0)}) {
		t.Error("the monotonic clock reading must not matter")
	}
	if requestHash("q", []any{a}) == requestHash("q", []any{a.Add(time.Nanosecond)}) {
		t.Error("different instants must hash differently")
	}
	if requestHash("q", []any{[]byte("ab")}) == requestHash("q", []any{"ab"}) {
		t.Error("bytes and text are different values")
	}
}

func TestTableMissingNamesTheFix(t *testing.T) {
	err := tableMissing(&pgconn.PgError{Code: "42P01", Message: `relation "keys" does not exist`}, "keys")
	if !errors.Is(err, ErrIdempotencyTableMissing) {
		t.Fatalf("error = %v", err)
	}
	for _, w := range []string{"EnsureIdempotencyTable", "CREATE TABLE IF NOT EXISTS"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("error should mention %q: %v", w, err)
		}
	}

	other := errors.New("boom")
	if got := tableMissing(other, "keys"); !errors.Is(got, other) || errors.Is(got, ErrIdempotencyTableMissing) {
		t.Errorf("other errors pass through unchanged, got %v", got)
	}
	if got := tableMissing(&pgconn.PgError{Code: "23505"}, "keys"); errors.Is(got, ErrIdempotencyTableMissing) {
		t.Error("only undefined_table means the table is missing")
	}
}
