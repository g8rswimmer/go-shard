-- A global table: the same rows on every shard, loaded by the migration itself.
CREATE TABLE ex_countries (
    code text PRIMARY KEY,
    name text NOT NULL
);
INSERT INTO ex_countries (code, name) VALUES
    ('NZ', 'New Zealand'),
    ('PT', 'Portugal'),
    ('US', 'United States');
