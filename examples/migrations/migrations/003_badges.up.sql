-- Both statements happen or neither does: a migration is one transaction.
CREATE TABLE ex_badges (
    profile_id bigint NOT NULL,
    badge      text   NOT NULL
);
CREATE INDEX ex_badges_by_profile ON ex_badges (profile_id);
