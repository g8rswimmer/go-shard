//go:build cgo

package plan_test

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/analyze"
	"github.com/g8rswimmer/go-shard/analyze/pgparse"
	"github.com/g8rswimmer/go-shard/plan"
	"github.com/g8rswimmer/go-shard/registry"
	"github.com/g8rswimmer/go-shard/router"
)

// The suite runs real SQL through the real parser, registry and router, and
// checks where each statement would run. It needs no database.
//
// Tables:
//
//	profiles(id int)                        sharded
//	  addresses(profile_id)                 colocated with profiles
//	    address_notes(profile_id)           colocated with addresses
//	  settings(id)                          colocated with profiles, same key name
//	orders(customer_id int)                 sharded, a different group
//	sales.orders(customer_id int)           sharded, registered with its schema
//	events(tenant string)                   sharded by a string
//	sessions(uid uuid)                      sharded by a uuid
//	countries, codes                        global

func suiteRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
		registry.Colocated("address_notes", registry.With("addresses"), registry.Key("profile_id")),
		registry.Colocated("settings", registry.With("profiles"), registry.Key("id")),
		registry.Sharded("orders", registry.Key("customer_id"), registry.Type(registry.KeyInt)),
		registry.Sharded("sales.orders", registry.Key("customer_id"), registry.Type(registry.KeyInt)),
		registry.Sharded("events", registry.Key("tenant"), registry.Type(registry.KeyString)),
		registry.Sharded("sessions", registry.Key("uid"), registry.Type(registry.KeyUUID)),
		registry.Global("countries"),
		registry.Global("codes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

var shards = []router.ShardID{"a", "b", "c"}

func suiteRouter(t testing.TB) *router.HashRouter {
	t.Helper()
	r, err := router.New(router.Even(shards...)...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// anyShard is deliberately not the first shard, so a test notices whether the
// router used the AnyShard option.
const anyShard router.ShardID = "b"

// Keys chosen by where they land, so cases can state "two different shards"
// without hard-coding hash results.
type keys struct {
	k1, k2         int64 // different shards
	same1, same2   int64 // different keys, same shard
	three          []any // together cover all three shards
	two            []any // together cover exactly two shards
	ofShard        map[router.ShardID]int64
	twoShardsOfTwo []router.ShardID
}

func pickKeys(t testing.TB, r *router.HashRouter) keys {
	t.Helper()
	k := keys{ofShard: map[router.ShardID]int64{}}
	byShard := map[router.ShardID][]int64{}
	for i := int64(1); i <= 300; i++ {
		s, _ := r.ShardFor(i)
		byShard[s] = append(byShard[s], i)
		if _, ok := k.ofShard[s]; !ok {
			k.ofShard[s] = i
		}
	}
	k.k1, k.k2 = byShard["a"][0], byShard["b"][0]
	k.same1, k.same2 = byShard["a"][0], byShard["a"][1]
	k.three = []any{byShard["a"][0], byShard["b"][0], byShard["c"][0]}
	k.two = []any{byShard["a"][0], byShard["c"][0]}
	k.twoShardsOfTwo = []router.ShardID{"a", "c"}
	return k
}

func TestRoutingSuite(t *testing.T) {
	reg := suiteRegistry(t)
	r := suiteRouter(t)
	k := pickKeys(t, r)
	parser := pgparse.New()

	shardOf := func(key any) router.ShardID {
		id, err := r.ShardFor(key)
		if err != nil {
			t.Fatalf("shardOf(%v): %v", key, err)
		}
		return id
	}
	const uuid = "123e4567-e89b-12d3-a456-426614174000"
	var uuidBytes = [16]byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}
	n42 := int64(42)

	// Expectations. Exactly one of the fields in want is set.
	type want struct {
		keys     []any                    // routed by these keys: targets are the shards they hash to
		shards   []router.ShardID         // routed to exactly these shards
		any      bool                     // any one shard
		all      bool                     // every shard (strategy All)
		rows     map[router.ShardID][]int // for INSERT: which VALUES rows go to which shard
		strategy plan.Strategy            // checked when non-zero
		err      error                    // errors.Is
		errHas   string                   // substring of the error
	}
	cases := []struct {
		name string
		sql  string
		args []any
		want want
	}{
		// ---- a shard key predicate picks a shard -------------------------------
		{"key = param", "SELECT * FROM profiles WHERE id = $1", []any{42}, want{keys: []any{42}, strategy: plan.Single}},
		{"key = literal", "SELECT * FROM profiles WHERE id = 42", nil, want{keys: []any{42}}},
		{"literal = key", "SELECT * FROM profiles WHERE 42 = id", nil, want{keys: []any{42}}},
		{"alias qualifier", "SELECT * FROM profiles p WHERE p.id = $1", []any{42}, want{keys: []any{42}}},
		{"table qualifier", "SELECT * FROM profiles WHERE profiles.id = $1", []any{42}, want{keys: []any{42}}},
		{"schema-qualified table", "SELECT * FROM public.profiles WHERE public.profiles.id = $1", []any{42}, want{keys: []any{42}}},
		{"text literal for int key", "SELECT * FROM profiles WHERE id = '42'", nil, want{keys: []any{42}}},
		{"cast param", "SELECT * FROM profiles WHERE id = $1::bigint", []any{42}, want{keys: []any{42}}},
		{"cast literal", "SELECT * FROM profiles WHERE id = 42::bigint", nil, want{keys: []any{42}}},
		{"text arg for int key", "SELECT * FROM profiles WHERE id = $1", []any{"42"}, want{keys: []any{42}}},
		{"negative literal", "SELECT * FROM profiles WHERE id = -5", nil, want{keys: []any{-5}}},
		{"literal above int32", "SELECT * FROM profiles WHERE id = 3000000000", nil, want{keys: []any{int64(3000000000)}}},
		{"uint64 arg", "SELECT * FROM profiles WHERE id = $1", []any{uint64(42)}, want{keys: []any{42}}},
		{"pointer arg", "SELECT * FROM profiles WHERE id = $1", []any{&n42}, want{keys: []any{42}}},
		{"other conditions around the key", "SELECT * FROM profiles WHERE name = 'x' AND id = $1 AND age > 3", []any{42}, want{keys: []any{42}}},
		{"nested ANDs", "SELECT * FROM profiles WHERE (id = $1 AND (name = 'a' AND age > 1))", []any{42}, want{keys: []any{42}}},
		{"string key", "SELECT * FROM events WHERE tenant = 'acme'", nil, want{keys: []any{"acme"}}},
		{"uuid key text", "SELECT * FROM sessions WHERE uid = $1", []any{uuid}, want{keys: []any{uuid}}},
		{"uuid key bytes", "SELECT * FROM sessions WHERE uid = $1", []any{uuidBytes}, want{keys: []any{uuid}}},
		{"uuid key upper case", "SELECT * FROM sessions WHERE uid = $1", []any{strings.ToUpper(uuid)}, want{keys: []any{uuid}}},
		{"uuid literal with cast", "SELECT * FROM sessions WHERE uid = '" + uuid + "'::uuid", nil, want{keys: []any{uuid}}},
		{"schema-qualified registry name", "SELECT * FROM sales.orders WHERE customer_id = $1", []any{42}, want{keys: []any{42}}},
		{"same table name in another schema is unknown", "SELECT * FROM archive.profiles WHERE id = 1", nil, want{err: plan.ErrUnknownTable, errHas: "archive.profiles"}},
		{"negated parameter", "SELECT * FROM profiles WHERE id = -$1", []any{5}, want{keys: []any{-5}}},
		{"FOR UPDATE", "SELECT * FROM profiles WHERE id = $1 FOR UPDATE", []any{42}, want{keys: []any{42}}},
		{"ORDER BY LIMIT", "SELECT * FROM profiles WHERE id = $1 ORDER BY name LIMIT 5", []any{42}, want{keys: []any{42}}},

		// ---- sets of keys ------------------------------------------------------
		{"IN literals", "SELECT * FROM profiles WHERE id IN (1, 2, 3)", nil, want{keys: []any{1, 2, 3}}},
		{"IN params", "SELECT * FROM profiles WHERE id IN ($1, $2)", []any{k.k1, k.k2}, want{keys: []any{k.k1, k.k2}}},
		{"IN with one value", "SELECT * FROM profiles WHERE id IN (7)", nil, want{keys: []any{7}, strategy: plan.Single}},
		{"IN keys on the same shard", "SELECT * FROM profiles WHERE id IN ($1, $2)", []any{k.same1, k.same2}, want{keys: []any{k.same1}, strategy: plan.Single}},
		{"IN covering two shards", "SELECT * FROM profiles WHERE id IN ($1, $2)", k.two, want{shards: k.twoShardsOfTwo, strategy: plan.Multi}},
		{"IN covering every shard", "SELECT * FROM profiles WHERE id IN ($1, $2, $3)", k.three, want{shards: shards, strategy: plan.All}},
		{"= ANY slice param", "SELECT * FROM profiles WHERE id = ANY($1)", []any{[]int64{k.k1, k.k2}}, want{keys: []any{k.k1, k.k2}}},
		{"= ANY array literal", "SELECT * FROM profiles WHERE id = ANY(ARRAY[1, 2, 3])", nil, want{keys: []any{1, 2, 3}}},
		{"= ANY with cast", "SELECT * FROM profiles WHERE id = ANY($1::bigint[])", []any{[]int{1, 2}}, want{keys: []any{1, 2}}},
		{"IN and = intersect", "SELECT * FROM profiles WHERE id IN ($1, $2) AND id = $2", []any{k.k1, k.k2}, want{keys: []any{k.k2}}},
		{"conflicting keys match nothing", "SELECT * FROM profiles WHERE id = $1 AND id = $2", []any{k.k1, k.k2}, want{any: true}},
		{"empty ANY list matches nothing", "SELECT * FROM profiles WHERE id = ANY($1)", []any{[]int64{}}, want{any: true}},

		{"ANY with an argument that is not a slice", "SELECT * FROM profiles WHERE id = ANY($1)", []any{5}, want{err: plan.ErrShardKeyRequired, errHas: "not a slice"}},
		{"ANY not on a column", "SELECT * FROM profiles WHERE 1 = ANY($1)", []any{[]int{1}}, want{err: plan.ErrShardKeyRequired}},
		{"ANY(ARRAY) with a non-constant", "SELECT * FROM profiles WHERE id = ANY(ARRAY[1, other])", nil, want{err: plan.ErrShardKeyRequired}},
		{"IN not on a column", "SELECT * FROM profiles WHERE 1 IN (id, 2)", nil, want{err: plan.ErrShardKeyRequired}},
		{"ambiguous unqualified key between untied tables", "SELECT * FROM profiles p, settings s WHERE id = 5", nil, want{err: plan.ErrCrossShardJoin}},

		// ---- writes ------------------------------------------------------------
		{"UPDATE by key", "UPDATE profiles SET name = 'x' WHERE id = $1", []any{42}, want{keys: []any{42}}},
		{"UPDATE by key set", "UPDATE profiles SET name = 'x' WHERE id IN ($1, $2)", k.two, want{shards: k.twoShardsOfTwo}},
		{"DELETE by key", "DELETE FROM profiles WHERE id = $1", []any{42}, want{keys: []any{42}}},
		{"DELETE with USING", "DELETE FROM profiles p USING addresses a WHERE a.profile_id = p.id AND p.id = $1", []any{42}, want{keys: []any{42}}},
		{"UPDATE with FROM", "UPDATE addresses a SET city = p.city FROM profiles p WHERE a.profile_id = p.id AND p.id = $1", []any{42}, want{keys: []any{42}}},
		{"UPDATE RETURNING", "UPDATE profiles SET name = 'x' WHERE id = $1 RETURNING id", []any{42}, want{keys: []any{42}}},
		{"UPDATE without key", "UPDATE profiles SET name = 'x'", nil, want{err: plan.ErrShardKeyRequired}},
		{"DELETE without key", "DELETE FROM profiles WHERE name = 'x'", nil, want{err: plan.ErrShardKeyRequired}},
		// ---- INSERT ------------------------------------------------------------
		{"INSERT one row, literal key", "INSERT INTO profiles (id, name) VALUES (42, 'x')", nil, want{keys: []any{42}, strategy: plan.Single, rows: map[router.ShardID][]int{shardOf(42): {0}}}},
		{"INSERT one row, parameters", "INSERT INTO profiles (id, name) VALUES ($1, $2)", []any{42, "x"}, want{keys: []any{42}}},
		{"INSERT with the key column second", "INSERT INTO profiles (name, id) VALUES ($1, $2)", []any{"x", 42}, want{keys: []any{42}}},
		{"INSERT with a text key for an int column", "INSERT INTO profiles (id, name) VALUES ('42', 'x')", nil, want{keys: []any{42}}},
		{"INSERT with a cast key", "INSERT INTO profiles (id, name) VALUES ($1::bigint, 'x')", []any{42}, want{keys: []any{42}}},
		{"INSERT ... RETURNING", "INSERT INTO profiles (id, name) VALUES ($1, 'x') RETURNING id", []any{42}, want{keys: []any{42}}},
		{"INSERT ... ON CONFLICT DO NOTHING", "INSERT INTO profiles (id, name) VALUES ($1, 'x') ON CONFLICT DO NOTHING", []any{42}, want{keys: []any{42}}},
		{"INSERT ... ON CONFLICT DO UPDATE of other columns", "INSERT INTO profiles (id, name) VALUES ($1, 'x') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name", []any{42}, want{keys: []any{42}}},
		{"INSERT into a colocated table routes by its key column", "INSERT INTO addresses (id, profile_id, city) VALUES (1, $1, 'x')", []any{42}, want{keys: []any{42}}},
		{"INSERT with a string key", "INSERT INTO events (tenant, n) VALUES ('acme', 1)", nil, want{keys: []any{"acme"}}},
		{"INSERT with a uuid key", "INSERT INTO sessions (uid, n) VALUES ($1, 1)", []any{uuid}, want{keys: []any{uuid}}},
		{"INSERT rows that all belong to one shard", "INSERT INTO profiles (id, name) VALUES ($1, 'a'), ($2, 'b')", []any{k.same1, k.same2},
			want{keys: []any{k.same1}, strategy: plan.Single, rows: map[router.ShardID][]int{shardOf(k.same1): {0, 1}}}},
		{"INSERT rows for different shards", "INSERT INTO profiles (id, name) VALUES ($1, 'a'), ($2, 'b'), ($3, 'c')", []any{k.k1, k.k2, k.k1},
			want{keys: []any{k.k1, k.k2}, strategy: plan.Multi, rows: map[router.ShardID][]int{shardOf(k.k1): {0, 2}, shardOf(k.k2): {1}}}},
		{"INSERT rows for every shard", "INSERT INTO profiles (id, name) VALUES ($1, 'a'), ($2, 'b'), ($3, 'c')", k.three, want{shards: shards, strategy: plan.All}},
		{"INSERT without the key column", "INSERT INTO profiles (name) VALUES ('x')", nil, want{err: plan.ErrShardKeyRequired, errHas: "does not set its shard key column"}},
		{"INSERT without column names", "INSERT INTO profiles VALUES (1, 'x')", nil, want{err: plan.ErrShardKeyRequired, errHas: "must name its columns"}},
		{"INSERT with DEFAULT as the key", "INSERT INTO profiles (id, name) VALUES (DEFAULT, 'x')", nil, want{err: plan.ErrShardKeyRequired, errHas: "not a literal or a parameter"}},
		{"INSERT with a function as the key", "INSERT INTO profiles (id, name) VALUES (nextval('s'), 'x')", nil, want{err: plan.ErrShardKeyRequired, errHas: "compute the key in your application"}},
		{"INSERT with an expression as the key", "INSERT INTO profiles (id, name) VALUES (1 + 2, 'x')", nil, want{err: plan.ErrShardKeyRequired}},
		{"INSERT with a NULL key", "INSERT INTO profiles (id, name) VALUES (NULL, 'x')", nil, want{err: plan.ErrShardKeyRequired}},
		{"INSERT with one bad row", "INSERT INTO profiles (id, name) VALUES (1, 'a'), (now(), 'b')", nil, want{err: plan.ErrShardKeyRequired, errHas: "row 1"}},
		{"INSERT with a row of the wrong width", "INSERT INTO profiles (id, name) VALUES (1, 'a'), (2)", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "row 1"}},
		{"INSERT with a key that is not a number", "INSERT INTO profiles (id, name) VALUES ('abc', 'x')", nil, want{errHas: "not an integer"}},
		{"INSERT ... SELECT", "INSERT INTO profiles (id, name) SELECT id, name FROM archive", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "INSERT ... SELECT"}},
		{"INSERT ... DEFAULT VALUES", "INSERT INTO profiles DEFAULT VALUES", nil, want{err: analyze.ErrUnsupportedQuery}},
		{"INSERT with a subquery value", "INSERT INTO profiles (id, name) VALUES (1, (SELECT 'x'))", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "subquery inside VALUES"}},
		{"INSERT with WITH", "WITH x AS (SELECT 1) INSERT INTO profiles (id) VALUES (1)", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "WITH"}},
		{"INSERT into an unknown table", "INSERT INTO mystery (id) VALUES (1)", nil, want{err: plan.ErrUnknownTable}},
		{"INSERT with a missing argument", "INSERT INTO profiles (id, name) VALUES ($1, $2)", []any{1}, want{err: analyze.ErrMissingArgument}},
		{"INSERT ... ON CONFLICT DO UPDATE of the shard key", "INSERT INTO profiles (id, name) VALUES ($1, 'x') ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id + 1", []any{42}, want{err: plan.ErrShardKeyImmutable}},

		// ---- the shard key cannot be changed ------------------------------------
		{"UPDATE sets the shard key", "UPDATE profiles SET id = 5 WHERE id = 1", nil, want{err: plan.ErrShardKeyImmutable, errHas: "delete it and insert it again"}},
		{"UPDATE sets the shard key to itself", "UPDATE profiles SET id = id WHERE id = 1", nil, want{err: plan.ErrShardKeyImmutable}},
		{"UPDATE sets several columns including the key", "UPDATE profiles SET (name, id) = ('a', 5) WHERE id = 1", nil, want{err: plan.ErrShardKeyImmutable}},
		{"UPDATE sets the key of a colocated table", "UPDATE addresses SET profile_id = 5 WHERE profile_id = 1", nil, want{err: plan.ErrShardKeyImmutable}},
		{"UPDATE of a column that merely looks like the key", "UPDATE addresses SET id = 5 WHERE profile_id = 1", nil, want{keys: []any{1}}},
		{"UPDATE of a global table's id", "UPDATE countries SET id = 5 WHERE code = 'US'", nil, want{all: true}},
		{"UPDATE that sets the key without a WHERE", "UPDATE profiles SET id = 5", nil, want{err: plan.ErrShardKeyImmutable}},
		{"INSERT into global table", "INSERT INTO countries (code) VALUES ('US')", nil, want{all: true, strategy: plan.All}},
		{"UPDATE global table", "UPDATE countries SET name = 'x' WHERE code = 'US'", nil, want{all: true}},
		{"DELETE from global table", "DELETE FROM countries WHERE code = 'US'", nil, want{all: true}},
		{"write to global reading sharded", "UPDATE countries SET n = 1 FROM profiles p WHERE p.id = $1", []any{42}, want{err: analyze.ErrUnsupportedQuery}},

		// ---- global tables and no tables ---------------------------------------
		{"SELECT without tables", "SELECT 1", nil, want{any: true}},
		{"function without tables", "SELECT now()", nil, want{any: true}},
		{"global table", "SELECT * FROM countries", nil, want{any: true}},
		{"global table with conditions", "SELECT * FROM countries WHERE code = 'US' OR code = 'CA'", nil, want{any: true}},
		{"two global tables joined", "SELECT * FROM countries c JOIN codes d ON d.code = c.code", nil, want{any: true}},
		{"pg_catalog is ignored", "SELECT * FROM pg_catalog.pg_tables", nil, want{any: true}},
		{"information_schema is ignored", "SELECT * FROM information_schema.tables", nil, want{any: true}},

		// ---- joins -------------------------------------------------------------
		{"join on key, key on parent", "SELECT * FROM profiles p JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1", []any{42}, want{keys: []any{42}}},
		{"join on key, key on child", "SELECT * FROM profiles p JOIN addresses a ON a.profile_id = p.id WHERE a.profile_id = $1", []any{42}, want{keys: []any{42}}},
		{"key in the ON clause of an inner join", "SELECT * FROM profiles p JOIN addresses a ON a.profile_id = p.id AND p.id = $1", []any{42}, want{keys: []any{42}}},
		{"comma join with WHERE link", "SELECT * FROM profiles p, addresses a WHERE a.profile_id = p.id AND p.id = $1", []any{42}, want{keys: []any{42}}},
		{"left join", "SELECT * FROM profiles p LEFT JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1", []any{42}, want{keys: []any{42}}},
		{"right join", "SELECT * FROM profiles p RIGHT JOIN addresses a ON a.profile_id = p.id WHERE a.profile_id = $1", []any{42}, want{keys: []any{42}}},
		{"full join", "SELECT * FROM profiles p FULL JOIN addresses a ON a.profile_id = p.id WHERE p.id = $1", []any{42}, want{keys: []any{42}}},
		{"three tables in a chain", "SELECT * FROM profiles p JOIN addresses a ON a.profile_id = p.id JOIN address_notes n ON n.profile_id = a.profile_id WHERE p.id = $1", []any{42}, want{keys: []any{42}}},
		{"USING on the shared key name", "SELECT * FROM profiles JOIN settings USING (id) WHERE id = $1", []any{42}, want{keys: []any{42}}},
		{"self join on the key", "SELECT * FROM profiles p1 JOIN profiles p2 ON p1.id = p2.id WHERE p1.id = $1", []any{42}, want{keys: []any{42}}},
		{"join with a global table", "SELECT * FROM profiles p JOIN countries c ON c.code = p.country WHERE p.id = $1", []any{42}, want{keys: []any{42}}},
		{"join without a key condition", "SELECT * FROM profiles p JOIN addresses a ON a.profile_id = p.id", nil, want{err: plan.ErrShardKeyRequired}},
		{"sharded and global join without a key", "SELECT * FROM profiles p JOIN countries c ON c.code = p.country", nil, want{err: plan.ErrShardKeyRequired}},
		{"USING without a key condition", "SELECT * FROM profiles JOIN settings USING (id)", nil, want{err: plan.ErrShardKeyRequired}},
		{"join on a non-key column", "SELECT * FROM profiles p JOIN addresses a ON a.city = p.city WHERE p.id = $1", []any{42}, want{err: plan.ErrCrossShardJoin, errHas: "addresses"}},
		{"chain with a broken link", "SELECT * FROM profiles p JOIN addresses a ON a.profile_id = p.id JOIN address_notes n ON n.city = a.city WHERE p.id = $1", []any{42}, want{err: plan.ErrCrossShardJoin, errHas: "address_notes"}},
		{"join across colocation groups", "SELECT * FROM profiles p JOIN orders o ON o.customer_id = p.id WHERE p.id = $1", []any{42}, want{err: plan.ErrCrossShardJoin, errHas: "not colocated"}},
		{"self join without a link, different shards", "SELECT * FROM profiles p1, profiles p2 WHERE p1.id = $1 AND p2.id = $2", []any{k.k1, k.k2}, want{err: plan.ErrCrossShardJoin}},
		{"self join without a link, same shard", "SELECT * FROM profiles p1, profiles p2 WHERE p1.id = $1 AND p2.id = $2", []any{k.same1, k.same2}, want{keys: []any{k.same1}}},
		{"two groups of tables pinned to one shard", "SELECT * FROM profiles p, addresses a WHERE p.id = $1 AND a.profile_id = $1", []any{42}, want{keys: []any{42}}},
		{"two groups pinned to different shards", "SELECT * FROM profiles p, addresses a WHERE p.id = $1 AND a.profile_id = $2", []any{k.k1, k.k2}, want{err: plan.ErrCrossShardJoin}},
		{"key only in the ON clause of an outer join", "SELECT * FROM profiles p LEFT JOIN addresses a ON a.profile_id = p.id AND a.profile_id = $1", []any{42}, want{err: plan.ErrShardKeyRequired}},

		// ---- subqueries --------------------------------------------------------
		{"correlated EXISTS", "SELECT * FROM profiles p WHERE p.id = $1 AND EXISTS (SELECT 1 FROM addresses a WHERE a.profile_id = p.id)", []any{42}, want{keys: []any{42}}},
		{"subquery pinned to the same key", "SELECT * FROM profiles p WHERE p.id = $1 AND EXISTS (SELECT 1 FROM addresses a WHERE a.profile_id = $1)", []any{42}, want{keys: []any{42}}},
		{"subquery on a global table", "SELECT * FROM profiles WHERE id = $1 AND country IN (SELECT code FROM countries)", []any{42}, want{keys: []any{42}}},
		{"scalar subquery in the select list", "SELECT (SELECT count(*) FROM addresses a WHERE a.profile_id = p.id) FROM profiles p WHERE p.id = $1", []any{42}, want{keys: []any{42}}},
		{"uncorrelated subquery on a colocated table", "SELECT * FROM profiles p WHERE p.id = $1 AND p.x IN (SELECT y FROM addresses)", []any{42}, want{err: plan.ErrCrossShardJoin}},
		{"subquery on another colocation group", "SELECT * FROM profiles WHERE id = $1 AND id IN (SELECT customer_id FROM orders)", []any{42}, want{err: plan.ErrCrossShardJoin}},
		{"IN subquery used as the only key condition", "SELECT * FROM profiles WHERE id IN (SELECT profile_id FROM addresses)", nil, want{err: plan.ErrCrossShardJoin}},
		// The id in the subquery is countries.id, not profiles.id. Routing by 5
		// would silently look on the wrong shard.
		{"inner column shadows the shard key", "SELECT * FROM profiles WHERE EXISTS (SELECT 1 FROM countries WHERE id = 5)", nil, want{err: plan.ErrShardKeyRequired}},
		{"qualified inner column does not shadow", "SELECT * FROM profiles p WHERE p.id = $1 AND EXISTS (SELECT 1 FROM countries c WHERE c.id = 5)", []any{42}, want{keys: []any{42}}},

		// ---- conditions that cannot pick a shard -------------------------------
		{"no WHERE", "SELECT * FROM profiles", nil, want{err: plan.ErrShardKeyRequired, errHas: "WithAllShards"}},
		{"OR of keys", "SELECT * FROM profiles WHERE id = $1 OR id = $2", []any{1, 2}, want{err: plan.ErrShardKeyRequired, errHas: "OR"}},
		{"OR next to an AND", "SELECT * FROM profiles WHERE (id = $1 OR id = $2) AND name = 'x'", []any{1, 2}, want{err: plan.ErrShardKeyRequired}},
		{"greater than", "SELECT * FROM profiles WHERE id > 5", nil, want{err: plan.ErrShardKeyRequired, errHas: `">"`}},
		{"BETWEEN", "SELECT * FROM profiles WHERE id BETWEEN 1 AND 10", nil, want{err: plan.ErrShardKeyRequired, errHas: "between"}},
		{"not equal", "SELECT * FROM profiles WHERE id <> 5", nil, want{err: plan.ErrShardKeyRequired}},
		{"NOT IN", "SELECT * FROM profiles WHERE id NOT IN (1, 2)", nil, want{err: plan.ErrShardKeyRequired, errHas: "NOT IN"}},
		{"IS NULL", "SELECT * FROM profiles WHERE id IS NULL", nil, want{err: plan.ErrShardKeyRequired}},
		{"= NULL", "SELECT * FROM profiles WHERE id = NULL", nil, want{err: plan.ErrShardKeyRequired}},
		{"NOT", "SELECT * FROM profiles WHERE NOT (id = 5)", nil, want{err: plan.ErrShardKeyRequired, errHas: "NOT"}},
		{"function on the key", "SELECT * FROM profiles WHERE abs(id) = 5", nil, want{err: plan.ErrShardKeyRequired}},
		{"function on a string key", "SELECT * FROM events WHERE lower(tenant) = 'acme'", nil, want{err: plan.ErrShardKeyRequired}},
		{"LIKE on a string key", "SELECT * FROM events WHERE tenant LIKE 'ac%'", nil, want{err: plan.ErrShardKeyRequired}},
		{"key compared with a column", "SELECT * FROM profiles WHERE id = parent_id", nil, want{err: plan.ErrShardKeyRequired}},
		{"key compared with an expression", "SELECT * FROM profiles WHERE id = 1 + 2", nil, want{err: plan.ErrShardKeyRequired}},
		{"IN with a non-constant", "SELECT * FROM profiles WHERE id IN (1, other)", nil, want{err: plan.ErrShardKeyRequired}},
		{"conditions on other columns only", "SELECT * FROM profiles WHERE name = 'x'", nil, want{err: plan.ErrShardKeyRequired}},
		{"wrong column qualifier", "SELECT * FROM profiles p WHERE q.id = 5", nil, want{err: plan.ErrShardKeyRequired}},
		{"column of a global table", "SELECT * FROM countries c JOIN profiles p ON p.country = c.code WHERE c.id = 5", nil, want{err: plan.ErrShardKeyRequired}},

		// ---- refused ------------------------------------------------------------
		{"WITH", "WITH x AS (SELECT * FROM profiles WHERE id = 1) SELECT * FROM x", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "WITH"}},
		{"UNION", "SELECT id FROM profiles WHERE id = 1 UNION SELECT id FROM profiles WHERE id = 2", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "UNION"}},
		{"subquery in FROM", "SELECT * FROM (SELECT * FROM profiles WHERE id = 1) t", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "FROM"}},
		{"function in FROM", "SELECT * FROM generate_series(1, 3)", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "FROM"}},
		{"NATURAL JOIN", "SELECT * FROM profiles NATURAL JOIN addresses WHERE id = 1", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "NATURAL"}},
		{"VALUES", "VALUES (1), (2)", nil, want{err: analyze.ErrUnsupportedQuery}},
		{"two statements", "SELECT 1; SELECT 2", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "2 statements"}},
		{"empty statement", "", nil, want{err: analyze.ErrUnsupportedQuery}},
		{"CREATE TABLE", "CREATE TABLE x (id int)", nil, want{err: analyze.ErrUnsupportedQuery, errHas: "WithAllShards"}},
		{"DROP TABLE", "DROP TABLE profiles", nil, want{err: analyze.ErrUnsupportedQuery}},
		{"EXPLAIN", "EXPLAIN SELECT * FROM profiles WHERE id = 1", nil, want{err: analyze.ErrUnsupportedQuery}},
		{"unknown table", "SELECT * FROM mystery WHERE id = 1", nil, want{err: plan.ErrUnknownTable, errHas: "mystery"}},
		{"unknown table in a subquery", "SELECT * FROM profiles WHERE id = 1 AND x IN (SELECT y FROM mystery)", nil, want{err: plan.ErrUnknownTable}},
		{"missing argument", "SELECT * FROM profiles WHERE id = $2", []any{1}, want{err: analyze.ErrMissingArgument}},
		{"float key", "SELECT * FROM profiles WHERE id = 1.5", nil, want{err: router.ErrUnsupportedKey}},
		{"non-numeric text for an int key", "SELECT * FROM profiles WHERE id = 'abc'", nil, want{errHas: "not an integer"}},
		{"int for a string key", "SELECT * FROM events WHERE tenant = 5", nil, want{err: router.ErrUnsupportedKey}},
		{"non-uuid for a uuid key", "SELECT * FROM sessions WHERE uid = 'abc'", nil, want{errHas: "not a UUID"}},
		{"nil key arg", "SELECT * FROM profiles WHERE id = $1", []any{nil}, want{err: router.ErrNilKey}},
		{"syntax error", "SELEC * FROM profiles", nil, want{errHas: "cannot parse SQL"}},
	}

	if len(cases) < 60 {
		t.Fatalf("the suite has %d cases; it is meant to have at least 60", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := parser.FromSQL(tc.sql, tc.args)
			var p plan.Plan
			if err == nil {
				p, err = plan.Route(a, reg, r, plan.Options{AnyShard: func() router.ShardID { return anyShard }})
			}

			w := tc.want
			if w.err != nil || w.errHas != "" {
				if err == nil {
					t.Fatalf("want an error (%v %q), got plan %+v", w.err, w.errHas, p)
				}
				if w.err != nil && !errors.Is(err, w.err) {
					t.Errorf("error = %v\nwant it to wrap %v", err, w.err)
				}
				if w.errHas != "" && !strings.Contains(err.Error(), w.errHas) {
					t.Errorf("error = %v\nwant it to mention %q", err, w.errHas)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var wantIDs []router.ShardID
			switch {
			case w.any:
				wantIDs = []router.ShardID{anyShard}
			case w.all:
				wantIDs = shards
			case w.shards != nil:
				wantIDs = w.shards
			default:
				seen := map[router.ShardID]bool{}
				for _, key := range w.keys {
					seen[shardOf(key)] = true
				}
				for id := range seen {
					wantIDs = append(wantIDs, id)
				}
				sort.Slice(wantIDs, func(i, j int) bool { return wantIDs[i] < wantIDs[j] })
			}
			if fmt.Sprint(p.Targets) != fmt.Sprint(wantIDs) {
				t.Errorf("targets = %v, want %v\nreason: %s", p.Targets, wantIDs, p.Reason)
			}
			if w.rows != nil && fmt.Sprint(p.Rows) != fmt.Sprint(w.rows) {
				t.Errorf("rows = %v, want %v", p.Rows, w.rows)
			}
			if w.strategy != 0 && p.Strategy != w.strategy {
				t.Errorf("strategy = %v, want %v", p.Strategy, w.strategy)
			}
			if p.Strategy == 0 || p.Reason == "" {
				t.Errorf("plan is missing its strategy or reason: %+v", p)
			}
		})
	}
}

// Errors must say how to fix the problem.
func TestRoutingErrorsAreActionable(t *testing.T) {
	reg, r, parser := suiteRegistry(t), suiteRouter(t), pgparse.New()
	route := func(sql string, args ...any) error {
		a, err := parser.FromSQL(sql, args)
		if err != nil {
			return err
		}
		_, err = plan.Route(a, reg, r, plan.Options{})
		return err
	}

	err := route("SELECT * FROM profiles WHERE id = $1 OR id = $2", 1, 2)
	for _, w := range []string{`"profiles"`, "id = $n", "IN (...)", "WithAllShards", "WithShardKey", "OR expression"} {
		if err == nil || !strings.Contains(err.Error(), w) {
			t.Errorf("shard-key error does not mention %q: %v", w, err)
		}
	}

	err = route("SELECT * FROM profiles p JOIN orders o ON o.customer_id = p.id WHERE p.id = 1")
	for _, w := range []string{"colocate", "global", "application"} {
		if err == nil || !strings.Contains(err.Error(), w) {
			t.Errorf("cross-shard error does not mention %q: %v", w, err)
		}
	}
}

// Without options, global-only statements use the first shard.
func TestAnyShardDefaultsToFirst(t *testing.T) {
	reg, r := suiteRegistry(t), suiteRouter(t)
	a, _ := pgparse.New().FromSQL("SELECT 1", nil)
	p, err := plan.Route(a, reg, r, plan.Options{})
	if err != nil || len(p.Targets) != 1 || p.Targets[0] != "a" {
		t.Errorf("plan = %+v, %v; want shard a", p, err)
	}
}
