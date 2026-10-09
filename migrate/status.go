package migrate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

// ShardStatus is one shard's place in the migrations.
type ShardStatus struct {
	State
	// Behind says the shard has not applied the newest migration in the source.
	Behind bool
	// Err is set when the shard could not be read; the other fields are then
	// meaningless.
	Err error
}

// Status is where every shard is.
type Status struct {
	// Latest is the newest migration in the source. LatestOK is false if the
	// source has none.
	Latest   uint
	LatestOK bool
	Shards   map[ShardID]ShardStatus
}

// Status reads every shard's version. It always returns the Status; the error
// is non-nil when a shard could not be read.
func (r *Runner) Status(ctx context.Context) (Status, error) {
	st := Status{Shards: make(map[ShardID]ShardStatus, len(r.shards))}
	var mu sync.Mutex
	var latestSet bool

	g := new(errgroup.Group)
	g.SetLimit(r.concurrency)
	for _, sh := range r.shards {
		g.Go(func() error {
			ss, latest, ok := r.read(ctx, sh)
			mu.Lock()
			defer mu.Unlock()
			st.Shards[sh.ID] = ss
			if ok && !latestSet {
				st.Latest, st.LatestOK, latestSet = latest, true, true
			}
			return nil
		})
	}
	_ = g.Wait()

	var errs []error
	for _, sh := range r.shards {
		ss := st.Shards[sh.ID]
		if ss.Err == nil && st.LatestOK {
			ss.Behind = !ss.Applied || ss.Version < st.Latest
			st.Shards[sh.ID] = ss
		}
		if ss.Err != nil {
			errs = append(errs, &ShardError{sh.ID, ss.Err})
		}
	}
	return st, errors.Join(errs...)
}

func (r *Runner) read(ctx context.Context, sh Shard) (ss ShardStatus, latest uint, ok bool) {
	m, err := r.open(ctx, sh)
	if err != nil {
		ss.Err = err
		return ss, 0, false
	}
	defer m.Close()
	if ss.State, err = m.State(ctx); err != nil {
		ss.Err = err
		return ss, 0, false
	}
	latest, ok, err = m.Latest()
	if err != nil {
		ss.Err = err
	}
	return ss, latest, ok
}

// Drift reports whether the shards disagree: they are not all at the same
// version, or one is dirty. A shard that could not be read counts, since its
// version is unknown.
func (s Status) Drift() bool {
	var first *ShardStatus
	for _, id := range s.ids() {
		ss := s.Shards[id]
		switch {
		case ss.Err != nil, ss.Dirty:
			return true
		case first == nil:
			first = &ss
		case ss.State != first.State:
			return true
		default:
			// same as the first
		}
	}
	return false
}

// Behind lists the shards that have not applied the newest migration.
func (s Status) Behind() []ShardID {
	return s.pick(func(ss ShardStatus) bool { return ss.Err == nil && ss.Behind })
}

// Dirty lists the shards with a migration of unknown outcome.
func (s Status) Dirty() []ShardID {
	return s.pick(func(ss ShardStatus) bool { return ss.Err == nil && ss.Dirty })
}

// Unreadable lists the shards that could not be read.
func (s Status) Unreadable() []ShardID {
	return s.pick(func(ss ShardStatus) bool { return ss.Err != nil })
}

// UpToDate reports whether every shard is at the newest migration, with no
// drift.
func (s Status) UpToDate() bool {
	return !s.Drift() && len(s.Behind()) == 0
}

func (s Status) ids() []ShardID {
	ids := make([]ShardID, 0, len(s.Shards))
	for id := range s.Shards {
		ids = append(ids, id)
	}
	sortIDs(ids)
	return ids
}

func (s Status) pick(keep func(ShardStatus) bool) []ShardID {
	var ids []ShardID
	for _, id := range s.ids() {
		if keep(s.Shards[id]) {
			ids = append(ids, id)
		}
	}
	return ids
}

// String renders the status for people.
func (s Status) String() string {
	var b strings.Builder
	latest := "none"
	if s.LatestOK {
		latest = fmt.Sprint(s.Latest)
	}
	fmt.Fprintf(&b, "latest: %s\n", latest)
	for _, id := range s.ids() {
		ss := s.Shards[id]
		switch {
		case ss.Err != nil:
			fmt.Fprintf(&b, "%s: unreadable: %v\n", id, ss.Err)
		case ss.Behind:
			fmt.Fprintf(&b, "%s: %s (behind)\n", id, ss.State)
		default:
			fmt.Fprintf(&b, "%s: %s\n", id, ss.State)
		}
	}
	switch {
	case s.Drift():
		b.WriteString("drift: yes\n")
	default:
		b.WriteString("drift: no\n")
	}
	return b.String()
}

func sortIDs(ids []ShardID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
}
