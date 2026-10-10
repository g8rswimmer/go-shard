-- Fails on any shard that holds a name longer than 20 characters: adding a
-- constraint checks the rows already there. The demo uses this to show a
-- migration that fails on one shard only.
ALTER TABLE profiles ADD CONSTRAINT profiles_name_length CHECK (length(name) <= 20);
