package shard

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/g8rswimmer/go-shard/observe"
	"github.com/g8rswimmer/go-shard/query"
)

func builtSelect(t *testing.T, where ...query.Cond) Statement {
	t.Helper()
	b := query.From("profiles")
	for _, w := range where {
		b = b.Where(w)
	}
	st, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func assertOrder(t *testing.T, rec *recorder, want ...string) {
	t.Helper()
	if got := rec.kinds(); !reflect.DeepEqual(got, want) {
		t.Errorf("events = %v, want %v", got, want)
	}
	for _, p := range rec.ctxErr {
		t.Error(p)
	}
}

func TestHooksForAQueryOnOneShard(t *testing.T) {
	rec := &recorder{}
	db := fakeDB(t, rec, &fakeShard{rows: 1}, &fakeShard{rows: 1}, &fakeShard{rows: 1})

	rows, err := db.QueryStatement(context.Background(), builtSelect(t, query.Eq("id", 42)))
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	assertOrder(t, rec, "plan", "start", "shard-done", "done")
	p, d := rec.plan[0], rec.ends[0]
	switch {
	case p.Kind != observe.Query || p.Strategy != "single" || len(p.Targets) != 1 || p.Err != nil:
		t.Errorf("plan event = %+v", p)
	case !strings.Contains(p.Reason, "profiles.id") || !strings.HasPrefix(p.SQL, "SELECT"):
		t.Errorf("plan event should carry the reason and the SQL: %+v", p)
	case d.Err != nil || d.Strategy != "single" || !slices.Equal(d.Targets, p.Targets) || d.Duration <= 0:
		t.Errorf("done event = %+v", d)
	case rec.starts[0].Shard != p.Targets[0] || rec.dones[0].Shard != p.Targets[0]:
		t.Errorf("shard events %+v %+v should be for %v", rec.starts[0], rec.dones[0], p.Targets)
	case rec.dones[0].Duration <= 0 || rec.dones[0].Kind != observe.Query:
		t.Errorf("shard done event = %+v", rec.dones[0])
	}
}

func TestHooksForAFanOutQuery(t *testing.T) {
	rec := &recorder{}
	db := fakeDB(t, rec, &fakeShard{rows: 2}, &fakeShard{rows: 3}, &fakeShard{rows: 1})

	rows, err := db.WithAllShards().QueryStatement(context.Background(), builtSelect(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()

	// the shard events run in parallel, so only their place relative to the
	// rest is fixed
	got := rec.kinds()
	if got[0] != "plan" || got[len(got)-1] != "done" || got[len(got)-2] != "merge" {
		t.Fatalf("events = %v, want plan first, then the shards, merge, done", got)
	}
	starts, dones := 0, 0
	for _, e := range rec.events {
		switch {
		case strings.HasPrefix(e, "start "):
			starts++
		case strings.HasPrefix(e, "done "):
			dones++
			if !slices.Contains(rec.events, "start "+e[5:]) || slices.Index(rec.events, "start "+e[5:]) > slices.Index(rec.events, e) {
				t.Errorf("%q came before its start", e)
			}
		default:
			// plan, merge, done
		}
	}
	if starts != 3 || dones != 3 {
		t.Errorf("%d starts and %d dones, want 3 each", starts, dones)
	}
	if p := rec.plan[0]; p.Strategy != "all" || len(p.Targets) != 3 || p.Reason != "WithAllShards()" {
		t.Errorf("plan event = %+v", p)
	}
	if m := rec.merges[0]; m.Shards != 3 || m.Failed != 0 || m.Err != nil {
		t.Errorf("merge event = %+v", m)
	}
	for _, p := range rec.ctxErr {
		t.Error(p)
	}
}

func TestHooksForAWrite(t *testing.T) {
	rec := &recorder{}
	db := fakeDB(t, rec, &fakeShard{affected: 7}, &fakeShard{affected: 7})
	st, err := query.Update("profiles").Set("name", "x").Where(query.Eq("id", 1)).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecStatement(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	assertOrder(t, rec, "plan", "start", "shard-done", "done")
	if rec.plan[0].Kind != observe.Exec || rec.dones[0].Kind != observe.Exec || rec.dones[0].RowsAffected != 7 {
		t.Errorf("events: %+v %+v", rec.plan[0], rec.dones[0])
	}
}

func TestHooksWhenRoutingFails(t *testing.T) {
	rec := &recorder{}
	db := fakeDB(t, rec, &fakeShard{}, &fakeShard{})

	_, err := db.QueryStatement(context.Background(), builtSelect(t)) // no key, no WithAllShards
	if !errors.Is(err, ErrShardKeyRequired) {
		t.Fatalf("err = %v", err)
	}
	assertOrder(t, rec, "plan", "done")
	if !errors.Is(rec.plan[0].Err, ErrShardKeyRequired) || !errors.Is(rec.ends[0].Err, ErrShardKeyRequired) || len(rec.plan[0].Targets) != 0 {
		t.Errorf("plan %+v, done %+v: both should carry the routing error", rec.plan[0], rec.ends[0])
	}
}

func TestHooksWhenAShardFails(t *testing.T) {
	boom := errors.New("boom")
	rec := &recorder{}
	db := fakeDB(t, rec, &fakeShard{rows: 1}, &fakeShard{queryErr: boom}, &fakeShard{rows: 1})

	_, err := db.WithAllShards().QueryStatement(context.Background(), builtSelect(t))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if d := rec.ends[0]; !errors.Is(d.Err, boom) {
		t.Errorf("done event = %+v, want the error", d)
	}
	var failed []observe.ShardDoneEvent
	for _, d := range rec.dones {
		if d.Err != nil {
			failed = append(failed, d)
		}
	}
	if len(failed) != 1 || failed[0].Shard != "shard-02" || !errors.Is(failed[0].Err, boom) {
		t.Errorf("failed shard events = %+v", failed)
	}
	if len(rec.merges) != 0 {
		t.Error("a query that failed was merged")
	}
}

func TestHooksReportShardsLeftOutByAllowPartial(t *testing.T) {
	rec := &recorder{}
	db := fakeDB(t, rec, &fakeShard{rows: 1}, &fakeShard{queryErr: errors.New("down")}, &fakeShard{rows: 1})

	rows, err := db.WithAllShards().QueryStatement(AllowPartial(context.Background()), builtSelect(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if m := rec.merges[0]; m.Shards != 2 || m.Failed != 1 {
		t.Errorf("merge event = %+v, want 2 merged and 1 left out", m)
	}
	if rec.ends[0].Err != nil {
		t.Errorf("the statement succeeded: %v", rec.ends[0].Err)
	}
}

func TestHooksForATransaction(t *testing.T) {
	rec := &recorder{}
	db := fakeDB(t, rec, &fakeShard{rows: 1, affected: 1}, &fakeShard{rows: 1, affected: 1})
	ctx := context.Background()

	tx, err := db.Begin(ctx, ForShard("shard-01"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	var key int64
	for id, _ := db.router.ShardFor(key); id != "shard-01"; id, _ = db.router.ShardFor(key) {
		key++
	}
	rows, err := tx.QueryStatement(ctx, builtSelect(t, query.Eq("id", key)))
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if _, err := tx.Unchecked().Exec(ctx, "UPDATE profiles SET name = 'x'"); err != nil {
		t.Fatal(err)
	}
	assertOrder(t, rec, "plan", "start", "shard-done", "done", "plan", "start", "shard-done", "done")
	if p := rec.plan[1]; p.Kind != observe.Exec || !strings.Contains(p.Reason, "Unchecked") {
		t.Errorf("unchecked plan event = %+v", p)
	}
}

func TestNoHooksIsFine(t *testing.T) {
	db := fakeDB(t, nil, &fakeShard{rows: 1}, &fakeShard{rows: 1})
	rows, err := db.WithAllShards().QueryStatement(context.Background(), builtSelect(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
}

func TestPoolStatsDoesNotNeedAPing(t *testing.T) {
	db := fakeDB(t, nil, &fakeShard{}, &fakeShard{})
	stats := db.PoolStats()
	if len(stats) != 2 {
		t.Errorf("stats for %d shards, want 2", len(stats))
	}
	if _, ok := stats["shard-02"]; !ok {
		t.Errorf("stats = %v", stats)
	}
}

func TestHooksForAStatementOutsideTheTransaction(t *testing.T) {
	rec := &recorder{}
	db := fakeDB(t, rec, &fakeShard{}, &fakeShard{})
	ctx := context.Background()
	tx, err := db.Begin(ctx, ForShard("shard-01"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	var key int64
	for id, _ := db.router.ShardFor(key); id != "shard-02"; id, _ = db.router.ShardFor(key) {
		key++
	}
	if _, err := tx.QueryStatement(ctx, builtSelect(t, query.Eq("id", key))); !errors.Is(err, ErrCrossShardTx) {
		t.Fatalf("err = %v", err)
	}
	assertOrder(t, rec, "plan", "done")
	if !errors.Is(rec.ends[0].Err, ErrCrossShardTx) {
		t.Errorf("done event = %+v", rec.ends[0])
	}
}
