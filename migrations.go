package shard

import "github.com/g8rswimmer/go-shard/migrate"

// Migrations returns a migrate.Runner for this DB's shards, using the DSNs in
// Config. The Runner opens its own connections, separate from the pool, and
// can be used while the DB is serving queries.
func (db *DB) Migrations(src migrate.Source, opts ...migrate.Option) (*migrate.Runner, error) {
	return migrate.New(db.shards, src, opts...)
}
