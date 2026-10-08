//go:build cgo

package shard

import (
	"errors"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/plan"
)

func TestDefaultAnalyzerRoutesRawSQL(t *testing.T) {
	if defaultAnalyzer() == nil {
		t.Fatal("with cgo, the default analyzer should be the SQL parser")
	}
	db := autoDB(t, defaultAnalyzer(), "a", "b", "c")
	run := func(sql string, args ...any) (plan.Plan, error) {
		return (&scoped{db: db}).plan(sql, args, db.analyzeNow(sql, args))
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
		p, err := (&scoped{db: db, route: route{kind: routeByShard, shard: "c"}}).plan(sql, []any{1, 2}, db.analyzeNow(sql, []any{1, 2}))
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
