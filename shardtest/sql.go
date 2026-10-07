package shardtest

import (
	"fmt"
	"sort"
	"strings"

	// Registers the "pgx" driver used by Cluster.Direct.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// quoteIdent quotes a SQL identifier.
func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// columns returns a row's column names in a stable order.
func columns(row map[string]any) []string {
	cols := make([]string, 0, len(row))
	for c := range row {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

// insertSQL builds INSERT INTO t (cols...) VALUES ($1...) with columns sorted.
func insertSQL(table string, row map[string]any) string {
	cols := columns(row)
	names := make([]string, len(cols))
	params := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quoteIdent(c)
		params[i] = fmt.Sprintf("$%d", i+1)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quoteIdent(table), strings.Join(names, ", "), strings.Join(params, ", "))
}

// insertArgs returns the row's values in the same order as insertSQL.
func insertArgs(row map[string]any) []any {
	cols := columns(row)
	args := make([]any, len(cols))
	for i, c := range cols {
		args[i] = row[c]
	}
	return args
}
