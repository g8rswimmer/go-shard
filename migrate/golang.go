package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	gm "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	_ "github.com/golang-migrate/migrate/v4/source/file" // file:// sources
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// golangMigrate is the default Factory: one golang-migrate instance for the
// shard, over its own connection. golang-migrate holds a PostgreSQL advisory
// lock for the whole run, so two runners (or two processes) migrating the same
// shard take turns.
func golangMigrate(ctx context.Context, sh Shard, cfg Config) (Migrator, error) {
	src, err := openSource(cfg.Source)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", sh.DSN)
	if err != nil {
		_ = src.Close()
		return nil, fmt.Errorf("opening: %w", err)
	}
	// The driver pings with its own context, so check reachability first to
	// honor ctx.
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		_ = src.Close()
		return nil, fmt.Errorf("connecting: %w", err)
	}
	drv, err := pgx.WithInstance(db, &pgx.Config{MigrationsTable: cfg.Table})
	if err != nil {
		_ = db.Close()
		_ = src.Close()
		return nil, fmt.Errorf("preparing the migrations table: %w", err)
	}
	m, err := gm.NewWithInstance("migrations", src, "pgx", drv)
	if err != nil {
		_ = drv.Close()
		_ = src.Close()
		return nil, err
	}
	return &gmMigrator{m: m, src: src}, nil
}

func openSource(s Source) (source.Driver, error) {
	switch {
	case s.fsys != nil:
		return iofs.New(s.fsys, s.dir)
	default:
		return source.Open(s.url)
	}
}

type gmMigrator struct {
	m   *gm.Migrate
	src source.Driver
}

var _ Migrator = (*gmMigrator)(nil)

func (g *gmMigrator) State(context.Context) (State, error) {
	v, dirty, err := g.m.Version()
	switch {
	case errors.Is(err, gm.ErrNilVersion):
		return State{}, nil
	case err != nil:
		return State{}, err
	default:
		return State{Applied: true, Version: v, Dirty: dirty}, nil
	}
}

func (g *gmMigrator) UpTo(ctx context.Context, version uint) error {
	before, err := g.State(ctx)
	if err != nil {
		return err
	}
	if before.Applied && before.Version >= version {
		return nil
	}

	// golang-migrate does not look at a context. Ask it to stop between
	// migrations when ctx ends (GracefulStop has room for one signal).
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			select {
			case g.m.GracefulStop <- true:
			default:
				// already asked
			}
		case <-done:
		}
	}()

	runErr := g.m.Migrate(version)
	switch {
	case errors.Is(runErr, gm.ErrNoChange):
		// another runner, holding the shard's lock while this one waited, applied
		// them first
		return nil
	case runErr != nil:
		runErr = g.describe(ctx, runErr)
		// golang-migrate marks a migration dirty before running it and leaves it
		// so on failure. Ours run in one transaction, so nothing of it remains:
		// put the shard back at its last good version so it can be retried.
		if err := g.settle(ctx); err != nil {
			return fmt.Errorf("%w (and the shard could not be reset to its last good version: %w)", runErr, err)
		}
		return runErr
	default:
		// stopped early by a cancelled context returns no error
		return ctx.Err()
	}
}

// describe says which migration failed, and keeps the message short:
// golang-migrate's own error includes the whole migration file.
func (g *gmMigrator) describe(ctx context.Context, err error) error {
	st, stateErr := g.State(ctx)
	var dbErr database.Error
	if !errors.As(err, &dbErr) || stateErr != nil || !st.Dirty {
		return err
	}
	msg := strings.TrimPrefix(dbErr.Err, "migration failed: ")
	msg = strings.ReplaceAll(msg, " (column 0)", "")
	if dbErr.Line > 0 {
		msg = fmt.Sprintf("%s (line %d)", msg, dbErr.Line)
	}
	return &migrationError{version: st.Version, msg: msg, err: err}
}

// migrationError is a failed migration, shown without its SQL.
type migrationError struct {
	version uint
	msg     string
	err     error
}

func (e *migrationError) Error() string { return fmt.Sprintf("migration %d: %s", e.version, e.msg) }
func (e *migrationError) Unwrap() error { return e.err }

// settle clears a dirty flag a failed migration left behind.
func (g *gmMigrator) settle(ctx context.Context) error {
	st, err := g.State(ctx)
	if err != nil || !st.Dirty {
		return err
	}
	prev, err := g.src.Prev(st.Version)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return g.m.Force(-1)
	case err != nil:
		return err
	default:
		return g.m.Force(int(prev))
	}
}

func (g *gmMigrator) Latest() (uint, bool, error) {
	v, err := g.src.First()
	switch {
	case errors.Is(err, os.ErrNotExist):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	default:
		// walk to the end
	}
	for {
		next, err := g.src.Next(v)
		switch {
		case errors.Is(err, os.ErrNotExist):
			return v, true, nil
		case err != nil:
			return 0, false, err
		default:
			v = next
		}
	}
}

func (g *gmMigrator) Force(_ context.Context, version int) error { return g.m.Force(version) }

func (g *gmMigrator) Close() error {
	srcErr, dbErr := g.m.Close()
	return errors.Join(srcErr, dbErr)
}
