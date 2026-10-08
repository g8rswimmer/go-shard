package shard

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/query"
)

// keyOwnedBy finds an id the router assigns to the given shard.
func keyOwnedBy(t *testing.T, db *DB, id ShardID, not ...int) int {
	t.Helper()
	for k := 1; k < 1000; k++ {
		if s, _ := db.router.ShardFor(k); s == id {
			skip := false
			for _, n := range not {
				skip = skip || n == k
			}
			if !skip {
				return k
			}
		}
	}
	t.Fatalf("no key found for %s", id)
	return 0
}

func txOn(db *DB, id ShardID) *tx { return &tx{db: db, id: id} }

func TestTxAcceptsStatementsOfItsShard(t *testing.T) {
	db := autoDB(t, nil, "a", "b", "c")
	tx := txOn(db, "b")
	mine := keyOwnedBy(t, db, "b")

	for name, b := range map[string]interface {
		Build() (query.Statement, error)
	}{
		"read":   query.From("profiles").Where(query.Eq("id", mine)),
		"update": query.Update("profiles").Set("name", "x").Where(query.Eq("id", mine)),
		"delete": query.DeleteFrom("profiles").Where(query.Eq("id", mine)),
		"insert": query.InsertInto("profiles").Columns("id", "name").Row(mine, "x"),
	} {
		st, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		p, err := tx.plan(statementRequest(st))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if len(p.Targets) != 1 || p.Targets[0] != "b" {
			t.Errorf("%s: targets %v, want [b]", name, p.Targets)
		}
		if p.SQL != st.SQL() || len(p.Args) != len(st.Args()) {
			t.Errorf("%s: the plan must keep the SQL and arguments", name)
		}
	}
}

func TestTxRefusesStatementsOfOtherShards(t *testing.T) {
	db := autoDB(t, nil, "a", "b", "c")
	tx := txOn(db, "b")
	other := keyOwnedBy(t, db, "c")
	mine := keyOwnedBy(t, db, "b")
	otherToo := keyOwnedBy(t, db, "a")

	cases := map[string]interface {
		Build() (query.Statement, error)
	}{
		"read of another shard":    query.From("profiles").Where(query.Eq("id", other)),
		"write to another shard":   query.Update("profiles").Set("name", "x").Where(query.Eq("id", other)),
		"keys on several shards":   query.From("profiles").Where(query.In("id", mine, other, otherToo)),
		"insert for another":       query.InsertInto("profiles").Columns("id", "name").Row(other, "x"),
		"insert split across":      query.InsertInto("profiles").Columns("id", "name").Row(mine, "x").Row(other, "y"),
		"delete on every shard":    query.DeleteFrom("countries"),
		"update of a global table": query.Update("countries").Set("name", "x").Where(query.Eq("code", "US")),
	}
	for name, b := range cases {
		st, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.plan(statementRequest(st))
		if !errors.Is(err, ErrCrossShardTx) {
			t.Errorf("%s: error = %v, want ErrCrossShardTx", name, err)
			continue
		}
		for _, w := range []string{"transaction is on b", "colocation", "outside the transaction"} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%s: the error should mention %q: %v", name, w, err)
			}
		}
	}

	st, _ := query.DeleteFrom("countries").Build()
	if _, err := tx.plan(statementRequest(st)); err == nil || !strings.Contains(err.Error(), "global table") {
		t.Errorf("a global write should say why: %v", err)
	}
}

func TestTxRunsGlobalReadsOnItsOwnShard(t *testing.T) {
	db := autoDB(t, nil, "a", "b", "c")
	for _, id := range []ShardID{"a", "b", "c"} {
		st, _ := query.From("countries").Build()
		p, err := txOn(db, id).plan(statementRequest(st))
		if err != nil || len(p.Targets) != 1 || p.Targets[0] != id {
			t.Errorf("transaction on %s: plan %+v, err %v; a global read must stay on the transaction's shard", id, p.Targets, err)
		}
	}
}

func TestTxOnASingleShardClusterMayWriteGlobalTables(t *testing.T) {
	db := autoDB(t, nil, "only")
	st, _ := query.DeleteFrom("countries").Build()
	if _, err := txOn(db, "only").plan(statementRequest(st)); err != nil {
		t.Errorf("with one shard, a global write is a write to that shard: %v", err)
	}
}

func TestTxExplainsUnroutableStatements(t *testing.T) {
	db := autoDB(t, nil, "a", "b")
	tx := txOn(db, "a")

	st, _ := query.From("profiles").Build() // no shard key condition
	_, err := tx.plan(statementRequest(st))
	if !errors.Is(err, ErrShardKeyRequired) || !strings.Contains(err.Error(), "tx.Unchecked()") {
		t.Errorf("error = %v, want ErrShardKeyRequired pointing to tx.Unchecked()", err)
	}

	// Without an analyzer, raw SQL cannot be routed either; the hint still applies.
	_, err = tx.plan((&scoped{db: db}).sqlRequest("SELECT 1", nil))
	if !errors.Is(err, ErrShardKeyRequired) || !strings.Contains(err.Error(), "tx.Unchecked()") {
		t.Errorf("error = %v", err)
	}
}

func TestTxRefusesIdempotencyKeys(t *testing.T) {
	ctx := WithIdempotencyKey(context.Background(), "k")
	if err := rejectKey(ctx); !errors.Is(err, ErrUnsupportedQuery) || !strings.Contains(err.Error(), "already atomic") {
		t.Errorf("error = %v", err)
	}
	if err := rejectKey(context.Background()); err != nil {
		t.Errorf("no key, no problem: %v", err)
	}

	db := autoDB(t, nil, "a")
	tx := txOn(db, "a")
	st, _ := query.Update("profiles").Set("name", "x").Where(query.Eq("id", 1)).Build()
	if _, err := tx.ExecStatement(ctx, st); !errors.Is(err, ErrUnsupportedQuery) {
		t.Errorf("ExecStatement: error = %v", err)
	}
	if _, err := tx.Unchecked().Exec(ctx, "UPDATE profiles SET name = 'x'"); !errors.Is(err, ErrUnsupportedQuery) {
		t.Errorf("Unchecked().Exec: error = %v", err)
	}
}

func TestResolveTarget(t *testing.T) {
	db := autoDB(t, nil, "a", "b", "c")
	owner42, _ := db.router.ShardFor(42)

	t.Run("by key", func(t *testing.T) {
		if id, err := db.resolveTarget(ForKey(42)); err != nil || id != owner42 {
			t.Errorf("ForKey(42) = %q, %v; want %q", id, err, owner42)
		}
		if _, err := db.resolveTarget(ForKey(nil)); err == nil || !strings.Contains(err.Error(), "transaction key") {
			t.Errorf("a nil key: %v", err)
		}
	})

	t.Run("by table converts the key to its declared type", func(t *testing.T) {
		for _, key := range []any{42, "42", int64(42), uint8(42)} {
			if id, err := db.resolveTarget(ForTable("profiles", key)); err != nil || id != owner42 {
				t.Errorf("ForTable(profiles, %T %v) = %q, %v; want %q", key, key, id, err, owner42)
			}
		}
		if _, err := db.resolveTarget(ForTable("profiles", "abc")); err == nil || !strings.Contains(err.Error(), "not an integer") {
			t.Errorf("a key that is not a number: %v", err)
		}
	})

	t.Run("by table, errors", func(t *testing.T) {
		if _, err := db.resolveTarget(ForTable("mystery", 1)); !errors.Is(err, ErrUnknownTable) {
			t.Errorf("unknown table: %v", err)
		}
		if _, err := db.resolveTarget(ForTable("countries", 1)); !errors.Is(err, ErrUnsupportedQuery) || !strings.Contains(err.Error(), "ForShard") {
			t.Errorf("global table: %v", err)
		}
	})

	t.Run("by shard", func(t *testing.T) {
		if id, err := db.resolveTarget(ForShard("c")); err != nil || id != "c" {
			t.Errorf("ForShard(c) = %q, %v", id, err)
		}
		if _, err := db.resolveTarget(ForShard("zzz")); !errors.Is(err, ErrUnknownShard) || !strings.Contains(err.Error(), "[a b c]") {
			t.Errorf("unknown shard: %v", err)
		}
	})

	t.Run("no target", func(t *testing.T) {
		if _, err := db.resolveTarget(TxTarget{}); err == nil || !strings.Contains(err.Error(), "ForKey, shard.ForTable or shard.ForShard") {
			t.Errorf("a zero target: %v", err)
		}
	})
}

func TestBeginRejectsABadTargetBeforeConnecting(t *testing.T) {
	db := autoDB(t, nil, "a")
	if _, err := db.Begin(context.Background(), ForShard("nope")); !errors.Is(err, ErrUnknownShard) {
		t.Errorf("error = %v", err)
	}
	if err := db.InTx(context.Background(), TxTarget{}, func(Tx) error { t.Error("fn must not run"); return nil }); err == nil {
		t.Error("InTx with no target should fail")
	}
}

func TestTxOptions(t *testing.T) {
	var o sql.TxOptions
	ReadOnly()(&o)
	Isolation(sql.LevelSerializable)(&o)
	if !o.ReadOnly || o.Isolation != sql.LevelSerializable {
		t.Errorf("options = %+v", o)
	}
}

func TestErrTxDoneIsDatabaseSQLs(t *testing.T) {
	if !errors.Is(ErrTxDone, sql.ErrTxDone) {
		t.Error("ErrTxDone must be database/sql's, so existing code that checks it keeps working")
	}
}
