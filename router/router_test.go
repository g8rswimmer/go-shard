package router

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func mustNew(t *testing.T, a ...Assignment) *HashRouter {
	t.Helper()
	r, err := New(a...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func TestEven(t *testing.T) {
	tests := []struct {
		shards int
		sizes  []int
	}{
		{1, []int{1024}},
		{2, []int{512, 512}},
		{3, []int{342, 341, 341}},
		{5, []int{205, 205, 205, 205, 204}},
		{1024, nil}, // every shard has exactly one bucket
	}
	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.shards), func(t *testing.T) {
			ids := make([]ShardID, tc.shards)
			for i := range ids {
				ids[i] = ShardID(fmt.Sprintf("shard-%04d", i))
			}
			a := Even(ids...)
			if _, err := New(a...); err != nil {
				t.Fatalf("Even assignments are not a valid bucket map: %v", err)
			}
			for i, want := range tc.sizes {
				b := a[i].Buckets[0]
				if got := b.To - b.From + 1; got != want {
					t.Errorf("shard %d has %d buckets, want %d", i, got, want)
				}
			}
		})
	}
	if Even() != nil {
		t.Error("Even() with no shards should return nil")
	}
}

func TestNewRejectsInvalidMaps(t *testing.T) {
	tests := []struct {
		name string
		in   []Assignment
		want []string // substrings that must all appear
	}{
		{"nothing", nil, []string{"buckets 0-1023 are not assigned"}},
		{"gap at end", []Assignment{{"a", []BucketRange{Buckets(0, 599)}}},
			[]string{"buckets 600-1023 are not assigned"}},
		{"gap in middle", []Assignment{{"a", []BucketRange{Buckets(0, 99), Buckets(200, 1023)}}},
			[]string{"buckets 100-199 are not assigned"}},
		{"single bucket gap", []Assignment{{"a", []BucketRange{Buckets(0, 9), Buckets(11, 1023)}}},
			[]string{"buckets 10 are not assigned"}},
		{"overlap", []Assignment{
			{"a", []BucketRange{Buckets(0, 599)}},
			{"b", []BucketRange{Buckets(500, 1023)}}},
			[]string{`buckets 500-599 assigned to both "a" and "b"`}},
		{"out of range high", []Assignment{{"a", []BucketRange{Buckets(0, 1024)}}},
			[]string{"bucket range 0-1024 is invalid"}},
		{"out of range negative", []Assignment{{"a", []BucketRange{Buckets(-1, 1023)}}},
			[]string{"bucket range -1-1023 is invalid"}},
		{"reversed range", []Assignment{{"a", []BucketRange{Buckets(10, 5)}}},
			[]string{"bucket range 10-5 is invalid"}},
		{"empty shard id", []Assignment{{"", []BucketRange{Buckets(0, 1023)}}},
			[]string{"empty shard ID"}},
		{"shard without buckets", []Assignment{{"a", nil}, {"b", []BucketRange{Buckets(0, 1023)}}},
			[]string{`shard "a" has no buckets`}},
		{"duplicate shard", []Assignment{
			{"a", []BucketRange{Buckets(0, 511)}},
			{"a", []BucketRange{Buckets(512, 1023)}}},
			[]string{`shard "a" has more than one assignment`}},
		{"reports every problem", []Assignment{
			{"a", []BucketRange{Buckets(0, 99)}},
			{"b", []BucketRange{Buckets(50, 149)}}},
			[]string{"assigned to both", "buckets 150-1023 are not assigned"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.in...)
			if !errors.Is(err, ErrInvalidBucketMap) {
				t.Fatalf("error = %v, want ErrInvalidBucketMap", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error does not mention %q:\n%v", w, err)
				}
			}
		})
	}
}

func TestSingleShardOwnsEverything(t *testing.T) {
	r := mustNew(t, Even("only")...)
	for i := 0; i < 100; i++ {
		if s, err := r.ShardFor(i); err != nil || s != "only" {
			t.Fatalf("ShardFor(%d) = %q, %v", i, s, err)
		}
	}
}

func TestShardForUsesBucketOwner(t *testing.T) {
	// "profile-42" is bucket 368 (see TestBucketVectors).
	r := mustNew(t,
		Assignment{"low", []BucketRange{Buckets(0, 367)}},
		Assignment{"high", []BucketRange{Buckets(368, 1023)}})
	if s, _ := r.ShardFor("profile-42"); s != "high" {
		t.Errorf("ShardFor = %q, want high (bucket 368 is the first of its range)", s)
	}
	r = mustNew(t,
		Assignment{"low", []BucketRange{Buckets(0, 368)}},
		Assignment{"high", []BucketRange{Buckets(369, 1023)}})
	if s, _ := r.ShardFor("profile-42"); s != "low" {
		t.Errorf("ShardFor = %q, want low (bucket 368 is the last of its range)", s)
	}
}

func TestShardForIsDeterministicAndUsesAllShards(t *testing.T) {
	r1 := mustNew(t, Even("a", "b", "c")...)
	r2 := mustNew(t, Even("a", "b", "c")...)
	counts := map[ShardID]int{}
	const n = 30000
	for i := 0; i < n; i++ {
		s1, _ := r1.ShardFor(i)
		s2, _ := r2.ShardFor(i)
		if s1 != s2 {
			t.Fatalf("key %d: routers disagree: %q vs %q", i, s1, s2)
		}
		counts[s1]++
	}
	for _, id := range r1.All() {
		share := float64(counts[id]) / n
		if share < 0.30 || share > 0.37 { // ideal is 1/3
			t.Errorf("shard %q got %.1f%% of sequential keys, want about 33%%", id, share*100)
		}
	}
}

func TestShardForError(t *testing.T) {
	r := mustNew(t, Even("a")...)
	if _, err := r.ShardFor(nil); !errors.Is(err, ErrNilKey) {
		t.Errorf("error = %v, want ErrNilKey", err)
	}
	if _, err := r.ShardFor(1.5); !errors.Is(err, ErrUnsupportedKey) {
		t.Errorf("error = %v, want ErrUnsupportedKey", err)
	}
}

func TestShardsFor(t *testing.T) {
	r := mustNew(t, Even("c", "a", "b")...)

	got, err := r.ShardsFor([]any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Errorf("result is not sorted and distinct: %v", got)
		}
	}

	one, _ := r.ShardFor(7)
	same, _ := r.ShardsFor([]any{7, 7, 7})
	if len(same) != 1 || same[0] != one {
		t.Errorf("ShardsFor([7 7 7]) = %v, want [%s]", same, one)
	}

	none, err := r.ShardsFor(nil)
	if err != nil || len(none) != 0 {
		t.Errorf("ShardsFor(nil) = %v, %v; want empty, nil", none, err)
	}

	if _, err := r.ShardsFor([]any{1, nil}); !errors.Is(err, ErrNilKey) || !strings.Contains(err.Error(), "key 1") {
		t.Errorf("error = %v, want ErrNilKey naming key 1", err)
	}
}

func TestAllIsSortedCopy(t *testing.T) {
	r := mustNew(t, Even("c", "a", "b")...)
	all := r.All()
	if fmt.Sprint(all) != "[a b c]" {
		t.Errorf("All() = %v, want [a b c]", all)
	}
	all[0] = "mutated"
	if r.All()[0] != "a" {
		t.Error("All() must return a copy")
	}
}
