-- demo_profiles are sharded by id; demo_addresses follow their profile
-- (colocated); demo_countries are the same on every shard (global). The demo_
-- prefix keeps the demo away from tables of your own.
CREATE TABLE demo_profiles (
    id    bigint PRIMARY KEY,
    name  text   NOT NULL,
    tier  text   NOT NULL,
    score int    NOT NULL
);
CREATE TABLE demo_addresses (
    id         bigint PRIMARY KEY,
    profile_id bigint NOT NULL,
    city       text   NOT NULL
);
CREATE TABLE demo_countries (
    code text PRIMARY KEY,
    name text NOT NULL
);
INSERT INTO demo_countries (code, name) VALUES
    ('NZ', 'New Zealand'),
    ('PT', 'Portugal'),
    ('US', 'United States');
