//go:build cgo

package shard

import (
	"context"
	"testing"
)

// Raw SQL is read by PostgreSQL's parser to find the shard key; this is the
// cost of that. A prepared Statement from package query pays none of it.
func BenchmarkSingleShardRawSQL(b *testing.B) {
	db := benchDB(b, 1, nil)
	ctx := context.Background()
	for b.Loop() {
		rows, err := db.Query(ctx, "SELECT name FROM profiles WHERE id = $1", 42)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
		_ = rows.Close()
	}
}

func BenchmarkFanOutRawSQL(b *testing.B) {
	db := benchDB(b, 100, nil)
	ctx := context.Background()
	for b.Loop() {
		rows, err := db.WithAllShards().Query(ctx, "SELECT id FROM profiles ORDER BY id")
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			n++
		}
		_ = rows.Close()
		if n != 300 {
			b.Fatalf("read %d rows", n)
		}
	}
}
