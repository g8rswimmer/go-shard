package shardtest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard"
	"github.com/g8rswimmer/go-shard/query"
	"github.com/g8rswimmer/go-shard/registry"
)

var shards = []shard.ShardID{"shard-01", "shard-02", "shard-03"}

func fakeRegistry(t testing.TB) *registry.Registry {
	t.Helper()
	reg, err := registry.New(
		registry.Sharded("profiles", registry.Key("id"), registry.Type(registry.KeyInt)),
		registry.Colocated("addresses", registry.With("profiles"), registry.Key("profile_id")),
		registry.Global("countries"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// recorder captures what an assertion reports instead of failing the test.
type recorder struct {
	testing.TB
	errors []string
	fatal  bool
}

func (r *recorder) Helper() {}
func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}
func (r *recorder) Fatalf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
	r.fatal = true
	panic(r) // stops the code under test, as FailNow would
}

// expectFailure runs fn against a Fake whose test is a recorder, and returns
// what was reported.
func expectFailure(t *testing.T, fn func(f *Fake)) (msgs []string) {
	t.Helper()
	rec := &recorder{TB: t}
	f := NewFake(rec, fakeRegistry(t), shards...)
	defer func() {
		if p := recover(); p != nil && p != any(rec) {
			panic(p)
		}
		msgs = rec.errors
	}()
	fn(f)
	return rec.errors
}

func mustBuild(t *testing.T, b interface {
	Build() (query.Statement, error)
}) query.Statement {
	t.Helper()
	st, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestFakeRoutesLikeARealDB(t *testing.T) {
	ctx := context.Background()
	f := NewFake(t, fakeRegistry(t), shards...)

	// The same key must land on the same shard as in a DB with this config.
	want, err := shard.NewPlanner(shard.Config{Registry: fakeRegistry(t), Shards: []shard.ShardConfig{{ID: "shard-01"}, {ID: "shard-02"}, {ID: "shard-03"}}})
	if err != nil {
		t.Fatal(err)
	}
	st := mustBuild(t, query.From("profiles").Where(query.Eq("id", 42)))
	exp, err := want.ExplainStatement(ctx, st)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := f.QueryStatement(ctx, st)
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	got := f.AssertSingleShard("FROM \"profiles\"")
	if got != exp.Targets[0] {
		t.Errorf("shard = %s, want %s", got, exp.Targets[0])
	}
	if p := f.LastPlan(); p.Strategy != shard.Single || p.Reason != exp.Reason {
		t.Errorf("LastPlan = %+v", p)
	}
}

func TestFakeStubbedRowsScan(t *testing.T) {
	ctx := context.Background()
	f := NewFake(t, fakeRegistry(t), shards...)
	f.StubRows("", []string{"id", "name"}, []any{int64(1), "Ada"}, []any{int64(2), nil})
	f.StubRows("FROM \"addresses\"", []string{"city"}, []any{"Paris"})

	rows, err := f.WithAllShards().QueryStatement(ctx, mustBuild(t, query.From("profiles")))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id int64
		var name *string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		n := "<nil>"
		if name != nil {
			n = *name
		}
		got = append(got, fmt.Sprintf("%d:%s", id, n))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "1:Ada,2:<nil>" {
		t.Errorf("rows = %v", got)
	}
	if cols, _ := rows.Columns(); strings.Join(cols, ",") != "id,name" {
		t.Errorf("columns = %v", cols)
	}

	// The later, more specific stub wins for its own statements.
	rows2, err := f.WithShardKey(1).QueryStatement(ctx, mustBuild(t, query.From("addresses").Where(query.Eq("profile_id", 1))))
	if err != nil {
		t.Fatal(err)
	}
	defer rows2.Close()
	if !rows2.Next() {
		t.Fatal("expected the addresses stub")
	}
	var city string
	if err := rows2.Scan(&city); err != nil || city != "Paris" {
		t.Errorf("city = %q, %v", city, err)
	}
}

func TestFakeWritesReportEachTarget(t *testing.T) {
	ctx := context.Background()
	f := NewFake(t, fakeRegistry(t), shards...)
	f.StubAffected("UPDATE", 2)

	res, err := f.WithAllShards().ExecStatement(ctx, mustBuild(t, query.Update("profiles").Set("name", "x").Where(query.Eq("name", "y"))))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.PerShard) != 3 || res.RowsAffected() != 6 {
		t.Errorf("result = %+v, want 2 rows on each of 3 shards", res)
	}
	f.AssertFanout("UPDATE", shards...)
	if c := f.Calls(); len(c) != 1 || c[0].Kind != ExecCall {
		t.Errorf("calls = %+v", c)
	}
}

func TestFakeErrors(t *testing.T) {
	ctx := context.Background()
	f := NewFake(t, fakeRegistry(t), shards...)

	// Routing refusals reach the code under test, and are recorded.
	_, err := f.QueryStatement(ctx, mustBuild(t, query.From("profiles").Where(query.Eq("name", "x"))))
	if !errors.Is(err, shard.ErrShardKeyRequired) {
		t.Errorf("error = %v, want ErrShardKeyRequired", err)
	}
	if c := f.Calls(); len(c) != 1 || !errors.Is(c[0].Err, shard.ErrShardKeyRequired) {
		t.Errorf("the refusal should be recorded: %+v", c)
	}

	// A stubbed error is returned once the statement has been routed.
	boom := errors.New("boom")
	f.StubError("profiles", boom)
	_, err = f.QueryStatement(ctx, mustBuild(t, query.From("profiles").Where(query.Eq("id", 1))))
	if !errors.Is(err, boom) {
		t.Errorf("error = %v, want the stubbed one", err)
	}
	f.AssertSingleShard("profiles")

	f.Reset()
	if len(f.Calls()) != 0 {
		t.Error("Reset should forget the calls")
	}
}

func TestFakeExplainIsNotACall(t *testing.T) {
	f := NewFake(t, fakeRegistry(t), shards...)
	if _, err := f.WithAllShards().ExplainStatement(context.Background(), mustBuild(t, query.From("profiles"))); err != nil {
		t.Fatal(err)
	}
	if n := len(f.Calls()); n != 0 {
		t.Errorf("Explain recorded %d calls", n)
	}
}

func TestFakeAssertionsFail(t *testing.T) {
	ctx := context.Background()
	send := func(f *Fake) {
		_, _ = f.WithShardKey(1).QueryStatement(ctx, mustBuild(t, query.From("profiles").Where(query.Eq("id", 1))))
		_, _ = f.WithAllShards().QueryStatement(ctx, mustBuild(t, query.From("addresses")))
	}
	tests := []struct {
		name string
		do   func(f *Fake)
		want string
	}{
		{"single but fanned out", func(f *Fake) { send(f); f.AssertSingleShard("addresses") }, "want one"},
		{"fan-out but single", func(f *Fake) { send(f); f.AssertFanout("profiles") }, "want a fan-out"},
		{"wrong shards", func(f *Fake) { send(f); f.AssertRoutes("addresses", "shard-01") }, "want [shard-01]"},
		{"no such statement", func(f *Fake) { send(f); f.AssertRoutes("orders", "shard-01") }, `no statement contains "orders"`},
		{"fan-out to the wrong shards", func(f *Fake) { send(f); f.AssertFanout("addresses", "shard-01", "shard-02") }, "want [shard-01 shard-02]"},
		{"last plan with nothing sent", func(f *Fake) { f.LastPlan() }, "no statement was sent"},
		{"asserting on a refused statement", func(f *Fake) {
			_, _ = f.QueryStatement(ctx, mustBuild(t, query.From("profiles").Where(query.Eq("name", "x"))))
			f.AssertSingleShard("profiles")
		}, "was refused"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msgs := expectFailure(t, tc.do)
			if len(msgs) == 0 || !strings.Contains(strings.Join(msgs, "\n"), tc.want) {
				t.Errorf("reported %q, want a failure containing %q", msgs, tc.want)
			}
		})
	}
}

// Naming the right number of shards is not enough: they must be the right ones.
func TestFakeAssertRoutesComparesTheShards(t *testing.T) {
	ctx := context.Background()
	probe := NewFake(t, fakeRegistry(t), shards...)
	_, _ = probe.QueryStatement(ctx, mustBuild(t, query.From("profiles").Where(query.Eq("id", 1))))
	actual := probe.LastPlan().Targets[0]
	wrong := shards[0]
	if wrong == actual {
		wrong = shards[1]
	}

	msgs := expectFailure(t, func(f *Fake) {
		_, _ = f.QueryStatement(ctx, mustBuild(t, query.From("profiles").Where(query.Eq("id", 1))))
		f.AssertRoutes("profiles", wrong)
	})
	if len(msgs) != 1 || !strings.Contains(msgs[0], "would run on") {
		t.Errorf("reported %q, want one failure naming the shards", msgs)
	}
}

func TestFakeAssertionsPass(t *testing.T) {
	ctx := context.Background()
	msgs := expectFailure(t, func(f *Fake) {
		_, _ = f.WithShardKey(1).QueryStatement(ctx, mustBuild(t, query.From("profiles").Where(query.Eq("id", 1))))
		_, _ = f.WithAllShards().QueryStatement(ctx, mustBuild(t, query.From("addresses")))
		f.AssertSingleShard("profiles")
		f.AssertFanout("addresses")
		f.AssertFanout("addresses", shards...)
		f.AssertRoutes("addresses", "shard-03", "shard-01", "shard-02")
	})
	if len(msgs) > 0 {
		t.Errorf("passing assertions reported %q", msgs)
	}
}
