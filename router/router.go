// Package router maps shard keys to shards.
//
// A key is canonicalized to stable bytes (see Canonical), hashed with xxhash64
// and reduced to one of BucketCount virtual buckets. Each bucket belongs to
// exactly one shard. Because the bucket count is fixed, moving data between
// shards later means moving buckets, not rehashing every key.
package router

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// BucketCount is the number of virtual buckets. It is part of the on-disk
// layout of every deployment and must never change.
const BucketCount = 1024

// ErrInvalidBucketMap is wrapped by every error returned from New. The message
// lists every problem found.
var ErrInvalidBucketMap = errors.New("router: invalid bucket map")

// ShardID identifies a shard. IDs are stable names such as "shard-01".
type ShardID string

// BucketRange is an inclusive range of buckets.
type BucketRange struct{ From, To int }

// Buckets returns the inclusive bucket range [from, to].
func Buckets(from, to int) BucketRange { return BucketRange{From: from, To: to} }

// Assignment gives a shard ownership of one or more bucket ranges.
type Assignment struct {
	Shard   ShardID
	Buckets []BucketRange
}

// Router picks the shard(s) that own shard keys.
type Router interface {
	// ShardFor returns the shard that owns key.
	ShardFor(key any) (ShardID, error)
	// ShardsFor returns the distinct shards that own the keys, sorted by ID.
	ShardsFor(keys []any) ([]ShardID, error)
	// All returns every shard, sorted by ID.
	All() []ShardID
}

// HashRouter is the default Router: xxhash64 over canonical key bytes, modulo
// BucketCount, mapped to shards by a static bucket map. It is immutable and
// safe for concurrent use.
type HashRouter struct {
	owner  [BucketCount]ShardID
	shards []ShardID
}

var _ Router = (*HashRouter)(nil)

// New builds a router from a bucket map. Every bucket in [0, BucketCount) must
// be assigned to exactly one shard.
func New(assignments ...Assignment) (*HashRouter, error) {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	r := &HashRouter{}
	seenShard := map[ShardID]bool{}
	conflicts := runs{}

	for _, a := range assignments {
		if a.Shard == "" {
			add("an assignment has an empty shard ID")
			continue
		}
		if seenShard[a.Shard] {
			add("shard %q has more than one assignment; list all its ranges in one", a.Shard)
			continue
		}
		seenShard[a.Shard] = true
		r.shards = append(r.shards, a.Shard)
		if len(a.Buckets) == 0 {
			add("shard %q has no buckets", a.Shard)
		}
		for _, br := range a.Buckets {
			if br.From > br.To || br.From < 0 || br.To >= BucketCount {
				add("shard %q: bucket range %d-%d is invalid (valid buckets are 0-%d)", a.Shard, br.From, br.To, BucketCount-1)
				continue
			}
			for b := br.From; b <= br.To; b++ {
				if prev := r.owner[b]; prev != "" {
					conflicts.add(b, fmt.Sprintf("assigned to both %q and %q", prev, a.Shard))
					continue
				}
				r.owner[b] = a.Shard
			}
		}
	}

	for _, c := range conflicts.list() {
		add("buckets %s %s", c.span(), c.label)
	}
	unassigned := runs{}
	for b, s := range r.owner {
		if s == "" {
			unassigned.add(b, "")
		}
	}
	for _, c := range unassigned.list() {
		add("buckets %s are not assigned to any shard", c.span())
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("%w:\n  - %s", ErrInvalidBucketMap, strings.Join(problems, "\n  - "))
	}
	sort.Slice(r.shards, func(i, j int) bool { return r.shards[i] < r.shards[j] })
	return r, nil
}

// Even splits all buckets into contiguous, near-equal ranges, one per shard in
// the order given. With a remainder, the first shards get one extra bucket.
func Even(ids ...ShardID) []Assignment {
	if len(ids) == 0 {
		return nil
	}
	base, extra := BucketCount/len(ids), BucketCount%len(ids)
	out := make([]Assignment, len(ids))
	next := 0
	for i, id := range ids {
		n := base
		if i < extra {
			n++
		}
		out[i] = Assignment{Shard: id, Buckets: []BucketRange{Buckets(next, next+n-1)}}
		next += n
	}
	return out
}

// Bucket returns the virtual bucket for a key, in [0, BucketCount).
func (r *HashRouter) Bucket(key any) (int, error) {
	c, err := Canonical(key)
	if err != nil {
		return 0, err
	}
	return int(xxhash.Sum64(c) % BucketCount), nil
}

// ShardFor returns the shard that owns key.
func (r *HashRouter) ShardFor(key any) (ShardID, error) {
	b, err := r.Bucket(key)
	if err != nil {
		return "", err
	}
	return r.owner[b], nil
}

// ShardsFor returns the distinct shards that own the keys, sorted by ID. An
// empty key list returns no shards.
func (r *HashRouter) ShardsFor(keys []any) ([]ShardID, error) {
	seen := map[ShardID]bool{}
	var out []ShardID
	for i, k := range keys {
		s, err := r.ShardFor(k)
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", i, err)
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// All returns every shard, sorted by ID.
func (r *HashRouter) All() []ShardID { return append([]ShardID(nil), r.shards...) }

// runs groups consecutive integers that share a label, for readable messages.
type runs struct{ items []run }

type run struct {
	from, to int
	label    string
}

func (rs *runs) add(n int, label string) {
	if last := len(rs.items) - 1; last >= 0 && rs.items[last].to == n-1 && rs.items[last].label == label {
		rs.items[last].to = n
		return
	}
	rs.items = append(rs.items, run{from: n, to: n, label: label})
}

func (rs *runs) list() []run { return rs.items }

func (r run) span() string {
	if r.from == r.to {
		return fmt.Sprint(r.from)
	}
	return fmt.Sprintf("%d-%d", r.from, r.to)
}
