// Package migrate applies versioned migrations to every shard, in parallel,
// and reports what version each shard is at and whether they have drifted
// apart.
//
//	runner, err := migrate.New(shards, migrate.FromURL("file://./migrations"))
//	res, err := runner.Up(ctx)       // every shard to the newest version
//	st, err := runner.Status(ctx)    // per-shard version, st.Drift()
//
// Every shard gets the same migrations, so the sharded tables, the colocated
// tables and the global tables all exist everywhere; a migration that changes
// global-table data is like any other. Migrations are forward-only: files are
// named <version>_<name>.up.sql as in golang-migrate, and .down.sql files are
// not used.
//
// A migration runs as one statement batch, which PostgreSQL executes as a
// single transaction: it either happens completely or not at all. If it
// fails, the shard is left at its last good version and the next Up retries
// it. Keep a migration that cannot run in a transaction (CREATE INDEX
// CONCURRENTLY) alone in its file.
//
// The engine behind a Runner is a Migrator, one per shard. The default wraps
// golang-migrate; see WithFactory to use another.
package migrate
