//go:build cgo

package shard

import (
	"errors"
	"strings"
	"testing"
)

// Raw SQL inside a transaction is routed by the same parser and rules, then
// checked against the transaction's shard.
func TestTxRoutesRawSQL(t *testing.T) {
	db := autoDB(t, defaultAnalyzer(), "a", "b", "c")
	tx := txOn(db, "b")
	mine := keyOwnedBy(t, db, "b")
	other := keyOwnedBy(t, db, "c")
	plan := func(sql string, args ...any) error {
		_, err := tx.plan((&scoped{db: db}).sqlRequest(sql, args))
		return err
	}

	for name, tc := range map[string]struct {
		sql  string
		args []any
	}{
		"select by key":    {"SELECT * FROM profiles WHERE id = $1", []any{mine}},
		"insert":           {"INSERT INTO profiles (id, name) VALUES ($1, 'x')", []any{mine}},
		"update":           {"UPDATE profiles SET name = 'x' WHERE id = $1", []any{mine}},
		"global read":      {"SELECT * FROM countries", nil},
		"no tables":        {"SELECT 1", nil},
		"conflicting keys": {"SELECT * FROM profiles WHERE id = $1 AND id = $2", []any{mine, other}},
	} {
		if err := plan(tc.sql, tc.args...); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	for name, tc := range map[string]struct {
		sql  string
		args []any
		want error
	}{
		"select of another shard":  {"SELECT * FROM profiles WHERE id = $1", []any{other}, ErrCrossShardTx},
		"insert for another shard": {"INSERT INTO profiles (id, name) VALUES ($1, 'x')", []any{other}, ErrCrossShardTx},
		"write to a global table":  {"INSERT INTO countries (code) VALUES ('US')", nil, ErrCrossShardTx},
		"no shard key":             {"SELECT * FROM profiles WHERE name = 'x'", nil, ErrShardKeyRequired},
		"unsupported construct":    {"WITH x AS (SELECT 1) SELECT * FROM x", nil, ErrUnsupportedQuery},
		"key that cannot change":   {"UPDATE profiles SET id = 9 WHERE id = $1", []any{mine}, ErrShardKeyImmutable},
	} {
		err := plan(tc.sql, tc.args...)
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: error = %v, want %v", name, err, tc.want)
		}
	}

	// A refusal that Unchecked can resolve says so.
	err := plan("WITH x AS (SELECT 1) SELECT * FROM x")
	if err == nil || !strings.Contains(err.Error(), "tx.Unchecked()") {
		t.Errorf("error should point to tx.Unchecked(): %v", err)
	}
}
