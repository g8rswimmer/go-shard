package observe

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
)

type tag struct {
	Nop
	name  string
	calls *[]string
}

type key string

func (h tag) OnPlan(ctx context.Context, _ PlanEvent) context.Context {
	*h.calls = append(*h.calls, h.name+":plan")
	return context.WithValue(ctx, key(h.name), true)
}

func (h tag) OnShardStart(ctx context.Context, _ ShardStartEvent) context.Context {
	*h.calls = append(*h.calls, h.name+":start")
	return ctx
}

func (h tag) OnDone(ctx context.Context, _ DoneEvent) {
	// both hooks' contexts reach every hook
	*h.calls = append(*h.calls, h.name+":done:"+boolText(ctx.Value(key("a")) == true)+boolText(ctx.Value(key("b")) == true))
}

func boolText(b bool) string {
	if b {
		return "y"
	}
	return "n"
}

func TestMultiCallsEachHookAndChainsTheContext(t *testing.T) {
	var calls []string
	h := Multi(tag{name: "a", calls: &calls}, tag{name: "b", calls: &calls})
	ctx := h.OnPlan(context.Background(), PlanEvent{})
	h.OnShardStart(ctx, ShardStartEvent{})
	h.OnShardDone(ctx, ShardDoneEvent{}) // Nop inside: must not panic
	h.OnMerge(ctx, MergeEvent{})
	h.OnDone(ctx, DoneEvent{})

	want := []string{"a:plan", "b:plan", "a:start", "b:start", "a:done:yy", "b:done:yy"}
	if !reflect.DeepEqual(calls, want) {
		t.Errorf("calls = %v, want %v", calls, want)
	}
}

func TestMultiOfNothingOrOne(t *testing.T) {
	if _, ok := Multi().(Nop); !ok {
		t.Error("Multi() should be Nop")
	}
	one := Nop{}
	if Multi(one) != Hooks(one) {
		t.Error("Multi(h) should be h")
	}
}

func TestKindString(t *testing.T) {
	if Query.String() != "query" || Exec.String() != "exec" || Kind(9).String() != "Kind(9)" {
		t.Errorf("%v %v %v", Query, Exec, Kind(9))
	}
}

// run drives hooks through the events of a statement.
func run(h Hooks, plan PlanEvent, shardErr, doneErr error, merge *MergeEvent) {
	ctx := h.OnPlan(context.Background(), plan)
	sctx := h.OnShardStart(ctx, ShardStartEvent{Kind: plan.Kind, Shard: "shard-01"})
	h.OnShardDone(sctx, ShardDoneEvent{Kind: plan.Kind, Shard: "shard-01", Duration: time.Millisecond, RowsAffected: 3, Err: shardErr})
	if merge != nil {
		h.OnMerge(ctx, *merge)
	}
	h.OnDone(ctx, DoneEvent{Kind: plan.Kind, Strategy: plan.Strategy, Targets: plan.Targets, Duration: 2 * time.Millisecond, Err: doneErr})
}

func logLines(t *testing.T, opts []SlogOption, f func(Hooks)) []string {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
	f(Slog(logger, opts...))
	return strings.Split(strings.TrimSpace(buf.String()), "\n")
}

var onePlan = PlanEvent{Kind: Query, SQL: "SELECT name FROM profiles WHERE id = 1", Strategy: "single", Targets: []ShardID{"shard-01"}}

func TestSlogLogsOneLinePerStatement(t *testing.T) {
	lines := logLines(t, nil, func(h Hooks) { run(h, onePlan, nil, nil, nil) })
	want := `level=DEBUG msg="shard statement" kind=query strategy=single targets=shard-01 duration=2ms`
	if len(lines) != 1 || lines[0] != want {
		t.Errorf("lines = %q\nwant      %q", lines, want)
	}
}

func TestSlogWithSQLAndLevelAndShardEvents(t *testing.T) {
	merge := &MergeEvent{Shards: 2, Duration: time.Millisecond}
	lines := logLines(t, []SlogOption{WithSQL(), WithLevel(slog.LevelInfo), WithShardEvents()}, func(h Hooks) {
		run(h, onePlan, nil, nil, merge)
	})
	if len(lines) != 3 {
		t.Fatalf("lines = %q, want shard, merge, statement", lines)
	}
	for i, want := range []string{
		`level=INFO msg="shard statement done" kind=query shard=shard-01 duration=1ms`,
		`level=INFO msg="shard merge" shards=2 failed=0 duration=1ms`,
		`level=INFO msg="shard statement" kind=query strategy=single targets=shard-01 duration=2ms sql="SELECT name FROM profiles WHERE id = 1"`,
	} {
		if lines[i] != want {
			t.Errorf("line %d = %q\nwant      %q", i, lines[i], want)
		}
	}
}

func TestSlogFailures(t *testing.T) {
	boom := errors.New("boom")
	lines := logLines(t, nil, func(h Hooks) {
		run(h, onePlan, boom, boom, &MergeEvent{Shards: 1, Failed: 1})
	})
	if len(lines) != 3 {
		t.Fatalf("lines = %q", lines)
	}
	for i, want := range []string{
		`level=WARN msg="shard statement failed" kind=query shard=shard-01 duration=1ms error=boom`,
		`level=WARN msg="shard merge" shards=1 failed=1`,
		`level=ERROR msg="shard statement failed" kind=query strategy=single targets=shard-01 duration=2ms error=boom`,
	} {
		if !strings.HasPrefix(lines[i], want) {
			t.Errorf("line %d = %q\nwant prefix %q", i, lines[i], want)
		}
	}
}

func TestEventsCarryNoStatementArguments(t *testing.T) {
	// hooks cannot leak what they are never given
	for _, typ := range []reflect.Type{reflect.TypeOf(PlanEvent{}), reflect.TypeOf(ShardStartEvent{}), reflect.TypeOf(DoneEvent{}), reflect.TypeOf(ShardDoneEvent{}), reflect.TypeOf(MergeEvent{})} {
		for i := 0; i < typ.NumField(); i++ {
			if n := strings.ToLower(typ.Field(i).Name); n == "args" || n == "arguments" || n == "params" {
				t.Errorf("%s.%s: events must not carry statement arguments", typ.Name(), typ.Field(i).Name)
			}
		}
	}
}
