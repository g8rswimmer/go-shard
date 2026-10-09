-- profiles are sharded by id; addresses follow their profile (colocated);
-- countries are the same on every shard (global).
CREATE TABLE profiles (
    id    bigint PRIMARY KEY,
    name  text   NOT NULL,
    tier  text   NOT NULL,
    score int    NOT NULL
);
CREATE TABLE addresses (
    id         bigint PRIMARY KEY,
    profile_id bigint NOT NULL,
    city       text   NOT NULL
);
CREATE TABLE countries (
    code text PRIMARY KEY,
    name text NOT NULL
);
INSERT INTO countries (code, name) VALUES
    ('NZ', 'New Zealand'),
    ('PT', 'Portugal'),
    ('US', 'United States');
