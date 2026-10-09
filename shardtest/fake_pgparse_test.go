//go:build cgo

package shardtest

import (
	"context"
	"testing"
)

// Raw SQL is routed by the real parser and rules.
func TestFakeRoutesRawSQL(t *testing.T) {
	ctx := context.Background()
	f := NewFake(t, fakeRegistry(t), shards...)
	f.StubRows("", []string{"n"}, []any{int64(7)})

	rows, err := f.Query(ctx, "SELECT count(*) FROM profiles WHERE id = $1", 42)
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	f.AssertSingleShard("count(*)")

	if _, err := f.Query(ctx, "SELECT name FROM profiles WHERE id IN (1, 2, 3, 4, 5, 6)"); err != nil {
		t.Fatal(err)
	}
	f.AssertFanout("id IN")

	if _, err := f.Exec(ctx, "INSERT INTO countries (code) VALUES ('US')"); err != nil {
		t.Fatal(err)
	}
	f.AssertRoutes("INSERT INTO countries", shards...)
}
