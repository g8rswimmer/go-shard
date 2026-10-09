// Package profiles is the code under test in the testing example: a small
// repository that depends on shard.Querier, not on a database.
//
// Because it takes the interface, a unit test can pass shardtest.NewFake (no
// Docker, no Postgres) and an integration test can pass a real *shard.DB.
package profiles

import (
	"context"
	"errors"
	"fmt"

	"github.com/g8rswimmer/go-shard"
)

// ErrNotFound is returned when a profile does not exist.
var ErrNotFound = errors.New("profiles: not found")

// DB is what the repository needs: a Querier, and the way to say "every
// shard". Both *shard.DB and *shardtest.Fake provide them.
type DB interface {
	shard.Querier
	WithAllShards() shard.Querier
}

// Repo reads and writes profiles.
type Repo struct {
	// DB is a *shard.DB in production and a *shardtest.Fake in unit tests.
	DB DB
}

// Profile is a row of the profiles table.
type Profile struct {
	ID      int64
	Name    string
	Country string
}

// Get finds one profile by its id, which is the shard key, so the query goes
// to one shard.
func (r Repo) Get(ctx context.Context, id int64) (Profile, error) {
	rows, err := r.DB.Query(ctx, "SELECT id, name, country FROM profiles WHERE id = $1", id)
	if err != nil {
		return Profile{}, fmt.Errorf("getting profile %d: %w", id, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Profile{}, err
		}
		return Profile{}, ErrNotFound
	}
	var p Profile
	if err := rows.Scan(&p.ID, &p.Name, &p.Country); err != nil {
		return Profile{}, err
	}
	return p, nil
}

// Rename changes a profile's name. It returns ErrNotFound if no row changed.
func (r Repo) Rename(ctx context.Context, id int64, name string) error {
	res, err := r.DB.Exec(ctx, "UPDATE profiles SET name = $1 WHERE id = $2", name, id)
	if err != nil {
		return fmt.Errorf("renaming profile %d: %w", id, err)
	}
	if res.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// CountByCountry totals the profiles per country, across all shards. The
// per-shard counts are added up by the library.
func (r Repo) CountByCountry(ctx context.Context) (map[string]int64, error) {
	rows, err := r.DB.WithAllShards().Query(ctx, "SELECT country, count(*) FROM profiles GROUP BY country")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var country string
		var n int64
		if err := rows.Scan(&country, &n); err != nil {
			return nil, err
		}
		out[country] = n
	}
	return out, rows.Err()
}

// FindByName looks profiles up by name. A name is not the shard key, so this
// has to ask every shard, and says so with WithAllShards.
func (r Repo) FindByName(ctx context.Context, name string) ([]Profile, error) {
	rows, err := r.DB.WithAllShards().Query(ctx, "SELECT id, name, country FROM profiles WHERE name = $1 ORDER BY id", name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Profile
	for rows.Next() {
		var p Profile
		if err := rows.Scan(&p.ID, &p.Name, &p.Country); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
