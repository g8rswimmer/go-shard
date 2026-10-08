package shardtest

import (
	"reflect"
	"strings"
	"testing"
)

func TestInsertSQLSortsColumnsAndQuotes(t *testing.T) {
	row := map[string]any{"name": "Ann", "id": 7, `we"ird`: true}
	want := `INSERT INTO "profiles" ("id", "name", "we""ird") VALUES ($1, $2, $3)`
	if got := insertSQL("profiles", row); got != want {
		t.Errorf("insertSQL = %s\nwant        %s", got, want)
	}
	if got := insertArgs(row); !reflect.DeepEqual(got, []any{7, "Ann", true}) {
		t.Errorf("insertArgs = %v, want values in column order", got)
	}
}

func TestInsertSQLQuotesTableName(t *testing.T) {
	got := insertSQL(`a"b`, map[string]any{"x": 1})
	if !strings.HasPrefix(got, `INSERT INTO "a""b" `) {
		t.Errorf("table name not quoted: %s", got)
	}
}

func TestExternalDSNs(t *testing.T) {
	got, err := externalDSNs(" a , b,,c ", 2)
	if err != nil || !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("externalDSNs = %v, %v; want the first two, trimmed", got, err)
	}
	if _, err := externalDSNs("a,b", 3); err == nil || !strings.Contains(err.Error(), "lists 2 databases but the test needs 3") {
		t.Errorf("error = %v, want a count mismatch", err)
	}
	if _, err := externalDSNs(" , ", 1); err == nil {
		t.Error("blank entries should not count as databases")
	}
}
