//go:build cgo

package shard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/query"
)

func TestDefaultAnalyzerRoutesRawSQL(t *testing.T) {
	if defaultAnalyzer() == nil {
		t.Fatal("with cgo, the default analyzer should be the SQL parser")
	}
	db := autoDB(t, defaultAnalyzer(), "a", "b", "c")
	run := func(sql string, args ...any) (plan.Plan, error) {
		return (&scoped{db: db}).plan((&scoped{db: db}).sqlRequest(sql, args))
	}

	t.Run("by key", func(t *testing.T) {
		for _, id := range []int{1, 2, 3, 42} {
			p, err := run("SELECT * FROM profiles WHERE id = $1", id)
			want, _ := db.router.ShardFor(id)
			if err != nil || len(p.Targets) != 1 || p.Targets[0] != want {
				t.Errorf("id %d: plan %+v, err %v; want [%s]", id, p.Targets, err, want)
			}
			if p.SQL == "" || len(p.Args) != 1 {
				t.Errorf("the plan must carry the SQL and arguments: %+v", p)
			}
		}
	})

	t.Run("a global read takes turns", func(t *testing.T) {
		var got []ShardID
		for i := 0; i < 4; i++ {
			p, err := run("SELECT * FROM countries")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, p.Targets[0])
		}
		if got[0] == got[1] || got[3] != got[0] {
			t.Errorf("shards chosen = %v, want them to rotate", got)
		}
	})

	t.Run("a global write goes everywhere", func(t *testing.T) {
		p, err := run("DELETE FROM countries WHERE code = 'US'")
		if err != nil || len(p.Targets) != 3 {
			t.Errorf("plan = %+v, err %v; want all three shards", p, err)
		}
	})

	t.Run("an unplaceable statement is refused", func(t *testing.T) {
		_, err := run("SELECT * FROM profiles WHERE id = $1 OR id = $2", 1, 2)
		if !errors.Is(err, ErrShardKeyRequired) {
			t.Errorf("error = %v, want ErrShardKeyRequired", err)
		}
	})

	t.Run("an explicit route overrides the SQL", func(t *testing.T) {
		sql := "SELECT * FROM profiles WHERE id = $1 OR id = $2"
		p, err := (&scoped{db: db, route: route{kind: routeByShard, shard: "c"}}).plan((&scoped{db: db}).sqlRequest(sql, []any{1, 2}))
		if err != nil || p.Targets[0] != "c" {
			t.Errorf("plan = %+v, err %v; want shard c", p, err)
		}
	})

	t.Run("DDL says what to do", func(t *testing.T) {
		_, err := run("CREATE TABLE x (id int)")
		if !errors.Is(err, ErrUnsupportedQuery) || !strings.Contains(err.Error(), "WithAllShards") {
			t.Errorf("error = %v", err)
		}
	})

	t.Run("an unregistered table is named", func(t *testing.T) {
		_, err := run("SELECT * FROM mystery WHERE id = 1")
		if !errors.Is(err, ErrUnknownTable) || !strings.Contains(err.Error(), "mystery") {
			t.Errorf("error = %v", err)
		}
	})
}

// A multi-row INSERT is routed by row and each shard gets only its own rows,
// whether it arrives as SQL or as a built statement.
func TestMultiRowInsertIsSplitPerShard(t *testing.T) {
	db := autoDB(t, defaultAnalyzer(), "a", "b", "c")
	owner := func(id int) ShardID { s, _ := db.router.ShardFor(id); return s }

	// Find ids on different shards.
	ids := map[ShardID]int{}
	for id := 1; len(ids) < 3; id++ {
		if _, seen := ids[owner(id)]; !seen {
			ids[owner(id)] = id
		}
	}
	first, second := ids["a"], ids["b"]

	sql := "INSERT INTO profiles (id, name) VALUES ($1, $2), ($3, $4), ($5, $6) RETURNING id"
	args := []any{first, "x", second, "y", first, "z"}
	p, err := (&scoped{db: db}).plan((&scoped{db: db}).sqlRequest(sql, args))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Targets) != 2 || len(p.PerShard) != 2 {
		t.Fatalf("plan = %+v", p)
	}

	for shardID, rows := range map[ShardID][]string{"a": {"x", "z"}, "b": {"y"}} {
		stmt := p.PerShard[shardID]
		an, err := defaultAnalyzer().FromSQL(stmt.SQL, stmt.Args)
		if err != nil {
			t.Fatalf("shard %s: %v\n%s", shardID, err, stmt.SQL)
		}
		var names []string
		for _, row := range an.Insert.Rows {
			names = append(names, row[1].Value.(string))
		}
		if fmt.Sprint(names) != fmt.Sprint(rows) {
			t.Errorf("shard %s receives rows %v, want %v", shardID, names, rows)
		}
		if !strings.Contains(stmt.SQL, "RETURNING") {
			t.Errorf("shard %s lost its RETURNING clause: %s", shardID, stmt.SQL)
		}
	}
	if fmt.Sprint(p.Rows["a"]) != "[0 2]" || fmt.Sprint(p.Rows["b"]) != "[1]" {
		t.Errorf("rows = %v", p.Rows)
	}

	// The same through the builder.
	st, err := query.InsertInto("profiles").Columns("id", "name").
		Row(first, "x").Row(second, "y").Row(first, "z").Build()
	if err != nil {
		t.Fatal(err)
	}
	bp, err := (&scoped{db: db}).plan(statementRequest(st))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(bp.Rows) != fmt.Sprint(p.Rows) || len(bp.PerShard) != 2 {
		t.Errorf("builder plan rows %v, want %v", bp.Rows, p.Rows)
	}
}

func TestInsertThatChangesTheKeyIsRefusedBeforeRunning(t *testing.T) {
	db := autoDB(t, defaultAnalyzer(), "a", "b")
	for _, sql := range []string{
		"UPDATE profiles SET id = 9 WHERE id = 1",
		"INSERT INTO profiles (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET id = 2",
	} {
		_, err := db.Exec(context.Background(), sql)
		if !errors.Is(err, ErrShardKeyImmutable) {
			t.Errorf("%s: error = %v, want ErrShardKeyImmutable", sql, err)
		}
	}
}
