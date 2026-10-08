package shard

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
)

func testRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg, err := registry.New(registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)))
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func shards(ids ...ShardID) []ShardConfig {
	out := make([]ShardConfig, len(ids))
	for i, id := range ids {
		out[i] = ShardConfig{ID: id, DSN: "postgres://x@localhost/" + string(id)}
	}
	return out
}

func TestConfigValidateBuildsEvenRouterByDefault(t *testing.T) {
	cfg := Config{Shards: shards("a", "b", "c"), Registry: testRegistry(t)}
	r, err := cfg.validate()
	if err != nil {
		t.Fatal(err)
	}
	if got := r.All(); len(got) != 3 {
		t.Fatalf("router has shards %v, want 3", got)
	}
	// Bucket 0 is the first bucket of the first listed shard.
	first, _ := router.New(router.Even("a", "b", "c")...)
	for k := 0; k < 200; k++ {
		want, _ := first.ShardFor(k)
		if got, _ := r.ShardFor(k); got != want {
			t.Fatalf("key %d routed to %s, want %s (even split in listed order)", k, got, want)
		}
	}
}

func TestConfigValidateUsesExplicitBuckets(t *testing.T) {
	s := shards("a", "b")
	s[0].Buckets = []router.BucketRange{Range(0, 99)}
	s[1].Buckets = []router.BucketRange{Range(100, 1023)}
	r, err := Config{Shards: s, Registry: testRegistry(t)}.validate()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[ShardID]int{}
	for k := 0; k < 5000; k++ {
		id, _ := r.ShardFor(k)
		counts[id]++
	}
	if counts["a"] > 800 { // owns about 10% of buckets
		t.Errorf("shard a got %d of 5000 keys; explicit buckets were ignored", counts["a"])
	}
}

func TestConfigValidateRejects(t *testing.T) {
	reg := testRegistry(t)
	partial := shards("a", "b")
	partial[0].Buckets = []router.BucketRange{Range(0, 1023)}
	gap := shards("a")
	gap[0].Buckets = []router.BucketRange{Range(0, 10)}

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"no shards", Config{Registry: reg}, "at least one shard"},
		{"no registry", Config{Shards: shards("a")}, "Registry is required"},
		{"empty id", Config{Shards: shards(""), Registry: reg}, "has no ID"},
		{"duplicate id", Config{Shards: shards("a", "a"), Registry: reg}, `"a" is listed more than once`},
		{"no dsn", Config{Shards: []ShardConfig{{ID: "a"}}, Registry: reg}, `shard "a" has no DSN`},
		{"negative conns", Config{Shards: []ShardConfig{{ID: "a", DSN: "x", MaxConns: -1}}, Registry: reg}, "MaxConns cannot be negative"},
		{"negative fanout", Config{Shards: shards("a"), Registry: reg, MaxFanout: -1}, "MaxFanout cannot be negative"},
		{"negative timeout", Config{Shards: shards("a"), Registry: reg, ShardTimeout: -time.Second}, "ShardTimeout cannot be negative"},
		{"buckets on some shards", Config{Shards: partial, Registry: reg}, "1 of 2 shards list Buckets"},
		{"bucket gap", Config{Shards: gap, Registry: reg}, "buckets 11-1023 are not assigned"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.cfg.validate()
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error = %v, want ErrInvalidConfig", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not mention %q:\n%v", tc.want, err)
			}
		})
	}
}

func TestBucketMapErrorKeepsItsCause(t *testing.T) {
	s := shards("a")
	s[0].Buckets = []router.BucketRange{Range(0, 10)}
	_, err := Config{Shards: s, Registry: testRegistry(t)}.validate()
	if !errors.Is(err, router.ErrInvalidBucketMap) {
		t.Errorf("error = %v, want it to also wrap router.ErrInvalidBucketMap", err)
	}
}

func TestConfigReportsEveryProblem(t *testing.T) {
	_, err := Config{Shards: []ShardConfig{{ID: "a"}}, MaxFanout: -1}.validate()
	for _, w := range []string{"Registry is required", "no DSN", "MaxFanout"} {
		if err == nil || !strings.Contains(err.Error(), w) {
			t.Errorf("error does not mention %q:\n%v", w, err)
		}
	}
}

func TestOpenRejectsBadConfigWithoutConnecting(t *testing.T) {
	_, err := Open(context.Background(), Config{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v, want ErrInvalidConfig", err)
	}
}

func TestOpenNamesTheUnreachableShard(t *testing.T) {
	s := []ShardConfig{{ID: "shard-09", DSN: "postgres://shard:pw@127.0.0.1:1/shard?sslmode=disable&connect_timeout=2"}}
	_, err := Open(context.Background(), Config{Shards: s, Registry: testRegistry(t)})
	var se *ShardError
	if !errors.As(err, &se) || se.Shard != "shard-09" {
		t.Fatalf("error = %v, want a *ShardError for shard-09", err)
	}
}
