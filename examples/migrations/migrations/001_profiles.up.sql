-- A sharded table: it exists on every shard, each shard holds its own rows.
CREATE TABLE ex_profiles (
    id   bigint PRIMARY KEY,
    name text NOT NULL
);
