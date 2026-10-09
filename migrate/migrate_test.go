package migrate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"
)

// cluster is a fake set of databases that keeps its state between Runner calls,
// like real shards do.
type cluster struct {
	mu      sync.Mutex
	state   map[ShardID]State
	latest  uint
	failAt  map[ShardID]uint // a migration that fails on this shard
	down    map[ShardID]bool // shard cannot be reached
	running atomic.Int32
	peak    atomic.Int32
	delay   time.Duration
}

func newCluster(latest uint, ids ...ShardID) *cluster {
	c := &cluster{state: map[ShardID]State{}, latest: latest, failAt: map[ShardID]uint{}, down: map[ShardID]bool{}}
	for _, id := range ids {
		c.state[id] = State{}
	}
	return c
}

func (c *cluster) shards() []Shard {
	var out []Shard
	for _, id := range []ShardID{"shard-01", "shard-02", "shard-03"} {
		if _, ok := c.state[id]; ok {
			out = append(out, Shard{ID: id, DSN: "unused"})
		}
	}
	return out
}

func (c *cluster) runner(t *testing.T, opts ...Option) *Runner {
	t.Helper()
	r, err := New(c.shards(), FromURL("file://unused"), append([]Option{WithFactory(c.factory)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (c *cluster) factory(_ context.Context, sh Shard, _ Config) (Migrator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.down[sh.ID] {
		return nil, errors.New("connection refused")
	}
	return &fakeMigrator{c: c, id: sh.ID}, nil
}

type fakeMigrator struct {
	c  *cluster
	id ShardID
}

func (f *fakeMigrator) State(context.Context) (State, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	return f.c.state[f.id], nil
}

func (f *fakeMigrator) UpTo(_ context.Context, version uint) error {
	n := f.c.running.Add(1)
	defer f.c.running.Add(-1)
	for {
		p := f.c.peak.Load()
		if n <= p || f.c.peak.CompareAndSwap(p, n) {
			break
		}
	}
	time.Sleep(f.c.delay)

	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	st := f.c.state[f.id]
	for v := st.Version + 1; v <= version; v++ {
		if st.Applied && v <= st.Version {
			continue
		}
		if f.c.failAt[f.id] == v {
			f.c.state[f.id] = st // left at the last good version
			return errors.New("syntax error")
		}
		st = State{Applied: true, Version: v}
	}
	f.c.state[f.id] = st
	return nil
}

func (f *fakeMigrator) Latest() (uint, bool, error) { return f.c.latest, f.c.latest > 0, nil }

func (f *fakeMigrator) Force(_ context.Context, version int) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	f.c.state[f.id] = State{Applied: version >= 0, Version: uint(max(version, 0))}
	return nil
}

func (f *fakeMigrator) Close() error { return nil }

func TestNewValidates(t *testing.T) {
	src := FromURL("file://x")
	one := []Shard{{ID: "a"}}
	for name, tc := range map[string]struct {
		shards []Shard
		src    Source
		opts   []Option
		want   string
	}{
		"no shards":      {nil, src, nil, "no shards"},
		"no id":          {[]Shard{{}}, src, nil, "no ID"},
		"duplicate":      {[]Shard{{ID: "a"}, {ID: "a"}}, src, nil, "twice"},
		"no source":      {one, Source{}, nil, "no migration source"},
		"concurrency":    {one, src, []Option{WithConcurrency(0)}, "concurrency"},
		"unknown policy": {one, src, []Option{WithPolicy(7)}, "policy"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(tc.shards, tc.src, tc.opts...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want one containing %q", err, tc.want)
			}
		})
	}
	if _, err := New(nil, src); !errors.Is(err, ErrNoShards) {
		t.Errorf("error = %v, want ErrNoShards", err)
	}
}

func TestUpMigratesEveryShard(t *testing.T) {
	c := newCluster(3, "shard-01", "shard-02", "shard-03")
	res, err := c.runner(t).Up(context.Background())
	if err != nil || !res.OK() {
		t.Fatalf("Up = %+v, %v", res, err)
	}
	for id, sr := range res.PerShard {
		if sr.Before.Applied || sr.After != (State{Applied: true, Version: 3}) {
			t.Errorf("%s: before %v after %v", id, sr.Before, sr.After)
		}
	}
}

func TestUpTo(t *testing.T) {
	c := newCluster(5, "shard-01", "shard-02")
	r := c.runner(t)
	if _, err := r.UpTo(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	res, err := r.UpTo(context.Background(), 1) // already past it
	if err != nil || res.PerShard["shard-01"].After.Version != 2 {
		t.Errorf("UpTo behind the shard moved it: %+v, %v", res, err)
	}
	st, _ := r.Status(context.Background())
	if st.Drift() || len(st.Behind()) != 2 || st.UpToDate() {
		t.Errorf("both shards at 2 of 5: %s", st)
	}
}

func TestHaltOnFailureStopsStartingShards(t *testing.T) {
	c := newCluster(2, "shard-01", "shard-02", "shard-03")
	c.failAt["shard-02"] = 2
	res, err := c.runner(t, WithConcurrency(1)).Up(context.Background())

	var se *ShardError
	switch {
	case !errors.Is(err, ErrFailed) || !errors.As(err, &se) || se.Shard != "shard-02":
		t.Fatalf("error = %v, want ErrFailed with a ShardError for shard-02", err)
	case !strings.Contains(err.Error(), "1 not started"):
		t.Errorf("error does not say a shard was not started: %v", err)
	case res.OK():
		t.Error("result is OK")
	}
	if got := res.PerShard["shard-01"].After.Version; got != 2 {
		t.Errorf("shard-01 at %d, want 2 (it ran before the failure)", got)
	}
	if got := res.PerShard["shard-02"].After; got != (State{Applied: true, Version: 1}) {
		t.Errorf("shard-02 at %v, want its last good version 1", got)
	}
	if !res.PerShard["shard-03"].Skipped || c.state["shard-03"].Applied {
		t.Errorf("shard-03 = %+v, want skipped and untouched", res.PerShard["shard-03"])
	}
	if f, s := res.Failed(), res.Skipped(); len(f) != 1 || f[0] != "shard-02" || len(s) != 1 || s[0] != "shard-03" {
		t.Errorf("Failed = %v, Skipped = %v", f, s)
	}
}

func TestContinueOnFailureMigratesTheRest(t *testing.T) {
	c := newCluster(2, "shard-01", "shard-02", "shard-03")
	c.failAt["shard-01"] = 1
	res, err := c.runner(t, WithConcurrency(1), WithPolicy(ContinueOnFailure)).Up(context.Background())
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("error = %v", err)
	}
	if len(res.Skipped()) != 0 || res.PerShard["shard-03"].After.Version != 2 || res.PerShard["shard-02"].After.Version != 2 {
		t.Errorf("the other shards should be migrated: %+v", res)
	}
}

func TestFailedShardIsRerunnable(t *testing.T) {
	c := newCluster(3, "shard-01", "shard-02", "shard-03")
	c.failAt["shard-02"] = 3
	r := c.runner(t, WithPolicy(ContinueOnFailure))
	ctx := context.Background()

	if _, err := r.Up(ctx); err == nil {
		t.Fatal("expected a failure")
	}
	st, err := r.Status(ctx)
	if err != nil || !st.Drift() || st.UpToDate() {
		t.Fatalf("after the failure: %v, drift %v\n%s", err, st.Drift(), st)
	}
	if b := st.Behind(); len(b) != 1 || b[0] != "shard-02" {
		t.Errorf("Behind = %v, want [shard-02]", b)
	}

	delete(c.failAt, "shard-02") // the migration is fixed
	if _, err := r.Up(ctx); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if st, _ := r.Status(ctx); st.Drift() || !st.UpToDate() {
		t.Errorf("after the re-run:\n%s", st)
	}
}

func TestShardThatCannotBeReached(t *testing.T) {
	c := newCluster(1, "shard-01", "shard-02")
	c.down["shard-02"] = true
	r := c.runner(t, WithPolicy(ContinueOnFailure))
	ctx := context.Background()

	res, err := r.Up(ctx)
	if !errors.Is(err, ErrFailed) || res.PerShard["shard-01"].After.Version != 1 {
		t.Errorf("Up = %+v, %v", res, err)
	}
	st, err := r.Status(ctx)
	var se *ShardError
	if !errors.As(err, &se) || se.Shard != "shard-02" {
		t.Errorf("Status error = %v, want a ShardError for shard-02", err)
	}
	if u := st.Unreadable(); len(u) != 1 || u[0] != "shard-02" || !st.Drift() {
		t.Errorf("unreadable = %v, drift = %v", u, st.Drift())
	}
	if !strings.Contains(st.String(), "shard-02: unreadable: connection refused") {
		t.Errorf("String:\n%s", st)
	}
}

func TestStatusDriftRules(t *testing.T) {
	applied := func(v uint) State { return State{Applied: true, Version: v} }
	for name, tc := range map[string]struct {
		states     [3]State
		drift      bool
		upToDate   bool
		behind, di int
	}{
		"all at the newest":       {[3]State{applied(3), applied(3), applied(3)}, false, true, 0, 0},
		"all at the same, behind": {[3]State{applied(2), applied(2), applied(2)}, false, false, 3, 0},
		"one behind":              {[3]State{applied(3), applied(2), applied(3)}, true, false, 1, 0},
		"one never migrated":      {[3]State{applied(3), {}, applied(3)}, true, false, 1, 0},
		"none migrated":           {[3]State{{}, {}, {}}, false, false, 3, 0},
		"all dirty":               {[3]State{{Applied: true, Version: 3, Dirty: true}, {Applied: true, Version: 3, Dirty: true}, {Applied: true, Version: 3, Dirty: true}}, true, false, 0, 3},
		"version 0 is not none":   {[3]State{{Applied: true}, {}, {}}, true, false, 3, 0},
		"one dirty":               {[3]State{applied(3), {Applied: true, Version: 3, Dirty: true}, applied(3)}, true, false, 0, 1},
	} {
		t.Run(name, func(t *testing.T) {
			c := newCluster(3, "shard-01", "shard-02", "shard-03")
			for i, id := range []ShardID{"shard-01", "shard-02", "shard-03"} {
				c.state[id] = tc.states[i]
			}
			st, err := c.runner(t).Status(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if st.Drift() != tc.drift || st.UpToDate() != tc.upToDate || len(st.Behind()) != tc.behind || len(st.Dirty()) != tc.di {
				t.Errorf("drift %v upToDate %v behind %v dirty %v\n%s", st.Drift(), st.UpToDate(), st.Behind(), st.Dirty(), st)
			}
		})
	}
}

func TestStatusString(t *testing.T) {
	c := newCluster(3, "shard-01", "shard-02", "shard-03")
	c.state["shard-01"] = State{Applied: true, Version: 3}
	c.state["shard-02"] = State{Applied: true, Version: 2}
	st, _ := c.runner(t).Status(context.Background())
	want := "latest: 3\nshard-01: 3\nshard-02: 2 (behind)\nshard-03: none (behind)\ndrift: yes\n"
	if st.String() != want {
		t.Errorf("String:\n%s\nwant:\n%s", st, want)
	}
}

func TestNothingToMigrate(t *testing.T) {
	c := newCluster(0, "shard-01")
	res, err := c.runner(t).Up(context.Background())
	if err != nil || res.PerShard["shard-01"].After.Applied {
		t.Errorf("an empty source should do nothing: %+v, %v", res, err)
	}
	if st, _ := c.runner(t).Status(context.Background()); st.LatestOK || st.Drift() || !st.UpToDate() {
		t.Errorf("status of an empty source:\n%s", st)
	}
}

func TestConcurrencyIsBounded(t *testing.T) {
	for _, n := range []int{1, 2} {
		c := newCluster(1, "shard-01", "shard-02", "shard-03")
		c.delay = 30 * time.Millisecond
		if _, err := c.runner(t, WithConcurrency(n)).Up(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := int(c.peak.Load()); got != n {
			t.Errorf("concurrency %d: %d shards ran at once", n, got)
		}
	}
}

func TestForce(t *testing.T) {
	c := newCluster(3, "shard-01")
	c.state["shard-01"] = State{Applied: true, Version: 2, Dirty: true}
	r := c.runner(t)
	if err := r.Force(context.Background(), "shard-01", 1); err != nil {
		t.Fatal(err)
	}
	if got := c.state["shard-01"]; got != (State{Applied: true, Version: 1}) {
		t.Errorf("state = %v", got)
	}
	if err := r.Force(context.Background(), "nope", 1); err == nil {
		t.Error("an unknown shard should be an error")
	}
}

func TestStateString(t *testing.T) {
	for st, want := range map[State]string{
		{}:                                       "none",
		{Dirty: true}:                            "none (dirty)",
		{Applied: true, Version: 4}:              "4",
		{Applied: true, Version: 4, Dirty: true}: "4 (dirty)",
	} {
		if got := st.String(); got != want {
			t.Errorf("%+v = %q, want %q", st, got, want)
		}
	}
}

func TestLatestReadsTheSource(t *testing.T) {
	for name, tc := range map[string]struct {
		files  []string
		want   uint
		wantOK bool
	}{
		"empty":     {nil, 0, false},
		"one":       {[]string{"1_a.up.sql"}, 1, true},
		"gaps":      {[]string{"1_a.up.sql", "5_b.up.sql", "9_c.up.sql", "9_c.down.sql"}, 9, true},
		"down only": {[]string{"1_a.down.sql"}, 1, true},
	} {
		t.Run(name, func(t *testing.T) {
			fsys := fstest.MapFS{"migrations/README.md": &fstest.MapFile{}} // not a migration; makes the directory exist
			for _, f := range tc.files {
				fsys["migrations/"+f] = &fstest.MapFile{Data: []byte("SELECT 1;")}
			}
			src, err := openSource(FromFS(fsys, "migrations"))
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			got, ok, err := (&gmMigrator{src: src}).Latest()
			if err != nil || got != tc.want || ok != tc.wantOK {
				t.Errorf("Latest = %d, %v, %v; want %d, %v", got, ok, err, tc.want, tc.wantOK)
			}
		})
	}
}
