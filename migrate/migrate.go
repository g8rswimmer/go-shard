package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/g8rswimmer/go-shard/router"
)

// ShardID names a shard; it is the same type as shard.ShardID.
type ShardID = router.ShardID

// Shard is a database to migrate.
type Shard struct {
	ID  ShardID
	DSN string
}

// State is where one database is in the migrations.
type State struct {
	// Applied says whether any migration has been applied. When false, Version
	// is 0.
	Applied bool
	// Version is the newest migration applied.
	Version uint
	// Dirty says a migration started and its outcome is unknown, typically
	// because the process died during it. Up refuses to run on a dirty
	// database; see Runner.Force.
	Dirty bool
}

// String renders the state for people: "none", "3" or "3 (dirty)".
func (s State) String() string {
	switch {
	case !s.Applied && s.Dirty:
		return "none (dirty)"
	case !s.Applied:
		return "none"
	case s.Dirty:
		return fmt.Sprintf("%d (dirty)", s.Version)
	default:
		return fmt.Sprint(s.Version)
	}
}

// Migrator migrates one database. A Runner uses one per shard and closes it
// when it is done.
type Migrator interface {
	// State reports where the database is.
	State(ctx context.Context) (State, error)
	// UpTo applies, in order, every migration newer than the database's version
	// up to and including version. A database already at or past version is left
	// alone. If a migration fails, the database must be left at the version
	// before it, not dirty, so that calling UpTo again retries it.
	UpTo(ctx context.Context, version uint) error
	// Latest is the newest migration in the source. ok is false when the source
	// has none.
	Latest() (version uint, ok bool, err error)
	// Force records version as the database's version without running anything
	// (-1 for none), and clears Dirty. It is for repairing a database a crash
	// left dirty.
	Force(ctx context.Context, version int) error
	Close() error
}

// Factory opens the Migrator for one shard.
type Factory func(ctx context.Context, sh Shard, cfg Config) (Migrator, error)

// Config is what a Factory is given besides the shard.
type Config struct {
	Source Source
	// Table is where the version is kept; empty means the engine's default
	// (schema_migrations).
	Table string
}

// Source says where the migrations are.
type Source struct {
	url  string
	fsys fs.FS
	dir  string
}

// FromURL reads migrations from a golang-migrate source URL, such as
// "file://./migrations".
func FromURL(url string) Source { return Source{url: url} }

// FromFS reads migrations from dir inside fsys, for example an embed.FS.
func FromFS(fsys fs.FS, dir string) Source { return Source{fsys: fsys, dir: dir} }

// Policy says what happens to the other shards when one fails.
type Policy int

const (
	// HaltOnFailure stops starting new shards after the first failure. Shards
	// already running finish; the rest are reported as Skipped. This is the
	// default: a bad migration usually fails everywhere, and it is better to
	// find out on one shard than on all of them.
	HaltOnFailure Policy = iota
	// ContinueOnFailure migrates every shard it can and reports each failure.
	ContinueOnFailure
)

// String returns the name of the policy.
func (p Policy) String() string {
	switch p {
	case HaltOnFailure:
		return "HaltOnFailure"
	case ContinueOnFailure:
		return "ContinueOnFailure"
	default:
		return fmt.Sprintf("Policy(%d)", int(p))
	}
}

// DefaultConcurrency is how many shards are migrated at once.
const DefaultConcurrency = 4

// Option customizes New.
type Option func(*Runner)

// WithPolicy sets what happens to the other shards when one fails.
func WithPolicy(p Policy) Option { return func(r *Runner) { r.policy = p } }

// WithConcurrency limits how many shards are migrated at once. With
// HaltOnFailure, the shards already running when one fails still finish, so
// a lower number means fewer shards touched by a bad migration.
func WithConcurrency(n int) Option { return func(r *Runner) { r.concurrency = n } }

// WithTable sets the table that records the version on each shard (default
// schema_migrations). It may be schema-qualified.
func WithTable(name string) Option { return func(r *Runner) { r.table = name } }

// WithFactory replaces the golang-migrate engine.
func WithFactory(f Factory) Option { return func(r *Runner) { r.factory = f } }

// Runner migrates a fixed set of shards. It is safe for concurrent use; every
// operation opens its own connections and closes them before it returns.
type Runner struct {
	shards      []Shard
	source      Source
	policy      Policy
	concurrency int
	table       string
	factory     Factory
}

// ErrFailed is wrapped by the error Up and UpTo return when a shard failed.
var ErrFailed = errors.New("migrate: migration failed")

// ErrNoShards is returned by New when it is given no shards.
var ErrNoShards = errors.New("migrate: no shards")

// New builds a Runner for the shards. It does not connect to them.
func New(shards []Shard, src Source, opts ...Option) (*Runner, error) {
	if len(shards) == 0 {
		return nil, ErrNoShards
	}
	seen := map[ShardID]bool{}
	for _, s := range shards {
		switch {
		case s.ID == "":
			return nil, errors.New("migrate: a shard has no ID")
		case seen[s.ID]:
			return nil, fmt.Errorf("migrate: shard %q is listed twice", s.ID)
		default:
			seen[s.ID] = true
		}
	}
	if src.url == "" && src.fsys == nil {
		return nil, errors.New("migrate: no migration source; use FromURL or FromFS")
	}
	r := &Runner{
		shards:      append([]Shard(nil), shards...),
		source:      src,
		concurrency: DefaultConcurrency,
		factory:     golangMigrate,
	}
	for _, o := range opts {
		o(r)
	}
	switch {
	case r.concurrency < 1:
		return nil, fmt.Errorf("migrate: concurrency must be at least 1, got %d", r.concurrency)
	case r.policy != HaltOnFailure && r.policy != ContinueOnFailure:
		return nil, fmt.Errorf("migrate: unknown policy %v", r.policy)
	default:
		return r, nil
	}
}

// ShardError is a migration failure on one shard.
type ShardError struct {
	Shard ShardID
	Err   error
}

// Error names the shard and gives the underlying error.
func (e *ShardError) Error() string { return fmt.Sprintf("shard %s: %v", e.Shard, e.Err) }

// Unwrap returns the underlying error, for errors.Is and errors.As.
func (e *ShardError) Unwrap() error { return e.Err }

// ShardResult is what happened to one shard.
type ShardResult struct {
	// Before and After are the shard's state around the migration. After is
	// the zero State when the shard could not be read.
	Before, After State
	// Err is why the shard failed, if it did.
	Err error
	// Skipped is true for a shard HaltOnFailure never started.
	Skipped bool
}

// Result reports every shard, like shard.WriteResult.
type Result struct {
	PerShard map[ShardID]ShardResult
}

// Failed lists the shards that failed, in the order given to New.
func (r Result) Failed() []ShardID { return r.pick(func(s ShardResult) bool { return s.Err != nil }) }

// Skipped lists the shards HaltOnFailure did not start.
func (r Result) Skipped() []ShardID { return r.pick(func(s ShardResult) bool { return s.Skipped }) }

// OK reports whether every shard was migrated.
func (r Result) OK() bool {
	for _, s := range r.PerShard {
		if s.Err != nil || s.Skipped {
			return false
		}
	}
	return true
}

func (r Result) pick(keep func(ShardResult) bool) []ShardID {
	var ids []ShardID
	for id, s := range r.PerShard {
		if keep(s) {
			ids = append(ids, id)
		}
	}
	sortIDs(ids)
	return ids
}

// Up applies every pending migration to every shard. The Result always lists
// every shard; the error is non-nil, wraps ErrFailed and one *ShardError per
// failed shard, when any shard failed or was skipped.
func (r *Runner) Up(ctx context.Context) (Result, error) { return r.run(ctx, nil) }

// UpTo is Up, stopping at version. Shards already at or past it are left alone.
func (r *Runner) UpTo(ctx context.Context, version uint) (Result, error) {
	return r.run(ctx, &version)
}

func (r *Runner) run(ctx context.Context, target *uint) (Result, error) {
	res := Result{PerShard: make(map[ShardID]ShardResult, len(r.shards))}
	var mu sync.Mutex
	var halted bool

	// Not WithContext: one shard failing must not cancel the others mid-migration.
	g := new(errgroup.Group)
	g.SetLimit(r.concurrency)
	for _, sh := range r.shards {
		// Go blocks while all slots are busy, so by the time this shard starts,
		// failures of the shards ahead of it are known.
		g.Go(func() error {
			mu.Lock()
			if halted {
				res.PerShard[sh.ID] = ShardResult{Skipped: true}
				mu.Unlock()
				return nil
			}
			mu.Unlock()

			sr := r.migrate(ctx, sh, target)
			mu.Lock()
			res.PerShard[sh.ID] = sr
			if sr.Err != nil && r.policy == HaltOnFailure {
				halted = true
			}
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	return res, r.failure(res)
}

func (r *Runner) migrate(ctx context.Context, sh Shard, target *uint) (sr ShardResult) {
	m, err := r.open(ctx, sh)
	if err != nil {
		sr.Err = &ShardError{sh.ID, err}
		return sr
	}
	defer func() {
		if err := m.Close(); err != nil && sr.Err == nil {
			sr.Err = &ShardError{sh.ID, fmt.Errorf("closing: %w", err)}
		}
	}()

	if sr.Before, err = m.State(ctx); err != nil {
		sr.Err = &ShardError{sh.ID, err}
		return sr
	}
	version := uint(0)
	switch {
	case target != nil:
		version = *target
	default:
		latest, ok, err := m.Latest()
		if err != nil {
			sr.Err = &ShardError{sh.ID, err}
			return sr
		}
		if !ok {
			sr.After = sr.Before
			return sr
		}
		version = latest
	}

	upErr := m.UpTo(ctx, version)
	// Read the state even after a failure: it shows the shard stayed put.
	after, stateErr := m.State(ctx)
	sr.After = after
	switch {
	case upErr != nil:
		sr.Err = &ShardError{sh.ID, upErr}
	case stateErr != nil:
		sr.Err = &ShardError{sh.ID, stateErr}
	default:
		// migrated
	}
	return sr
}

func (r *Runner) open(ctx context.Context, sh Shard) (Migrator, error) {
	return r.factory(ctx, sh, Config{Source: r.source, Table: r.table})
}

// failure summarizes a Result as an error.
func (r *Runner) failure(res Result) error {
	var errs []error
	skipped := 0
	for _, sh := range r.shards {
		sr := res.PerShard[sh.ID]
		switch {
		case sr.Err != nil:
			errs = append(errs, sr.Err)
		case sr.Skipped:
			skipped++
		default:
			// migrated
		}
	}
	switch {
	case len(errs) == 0 && skipped == 0:
		return nil
	case len(errs) == 0:
		return fmt.Errorf("%w: %d shards were not started", ErrFailed, skipped)
	default:
		msg := fmt.Sprintf("%d of %d shards failed", len(errs), len(r.shards))
		if skipped > 0 {
			msg += fmt.Sprintf(", %d not started (%v)", skipped, r.policy)
		}
		return fmt.Errorf("%w: %s: %w", ErrFailed, msg, errors.Join(errs...))
	}
}

// Force records version as shard id's version without running any migration
// (-1 for none) and clears its dirty flag. Use it only to repair a shard a
// crash left dirty, after checking by hand what that migration did.
func (r *Runner) Force(ctx context.Context, id ShardID, version int) error {
	for _, sh := range r.shards {
		if sh.ID != id {
			continue
		}
		m, err := r.open(ctx, sh)
		if err != nil {
			return &ShardError{id, err}
		}
		defer m.Close()
		if err := m.Force(ctx, version); err != nil {
			return &ShardError{id, err}
		}
		return nil
	}
	return fmt.Errorf("migrate: unknown shard %q", id)
}
