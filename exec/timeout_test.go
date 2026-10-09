package exec

import (
	"context"
	"errors"
	"testing"
	"time"
)

type tkey struct{}

func TestPushableEndsAfterItsTime(t *testing.T) {
	c := newPushable(context.Background(), 20*time.Millisecond)
	defer c.cancel()
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("never ended")
	}
	if !errors.Is(c.Err(), context.DeadlineExceeded) {
		t.Errorf("Err = %v, want DeadlineExceeded", c.Err())
	}
}

func TestPushableRestartGivesFullTimeAgain(t *testing.T) {
	c := newPushable(context.Background(), 80*time.Millisecond)
	defer c.cancel()
	time.Sleep(50 * time.Millisecond)
	c.restart()
	time.Sleep(50 * time.Millisecond) // 100ms since the start: past the first deadline
	if err := c.Err(); err != nil {
		t.Fatalf("ended early: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if !errors.Is(c.Err(), context.DeadlineExceeded) {
		t.Errorf("Err = %v, want DeadlineExceeded after the restarted time", c.Err())
	}
	c.restart() // after the end: nothing happens
	if c.Err() == nil {
		t.Error("restart revived an ended context")
	}
}

func TestPushableFollowsItsParent(t *testing.T) {
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), tkey{}, "v"))
	c := newPushable(parent, time.Minute)
	defer c.cancel()
	if c.Value(tkey{}) != "v" {
		t.Error("values of the parent are lost")
	}
	cancel()
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("did not follow the parent")
	}
	if !errors.Is(c.Err(), context.Canceled) {
		t.Errorf("Err = %v, want the parent's error", c.Err())
	}
}

func TestPushableIsAlreadyEndedWithAnEndedParent(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	c := newPushable(parent, time.Minute)
	defer c.cancel()
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("did not end")
	}
}

func TestPushableCancel(t *testing.T) {
	c := newPushable(context.Background(), time.Minute)
	c.cancel()
	c.cancel() // harmless twice
	if !errors.Is(c.Err(), context.Canceled) {
		t.Errorf("Err = %v", c.Err())
	}
}

// A slow shard must not use up the time the fast shards have for being read:
// the rows are read only after the slowest shard answered.
func TestSlowShardDoesNotStarveTheRowsOfOthers(t *testing.T) {
	pool := newPool(t,
		&fakeShard{query: func(context.Context) (int, error) { return 3, nil }},
		&fakeShard{query: func(ctx context.Context) (int, error) {
			select {
			case <-time.After(150 * time.Millisecond):
				return 1, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}},
	)
	got, err := NewExecutor(pool, Options{ShardTimeout: 250 * time.Millisecond}).Query(context.Background(), planFor("s0", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond) // 300ms since s0 answered: more than its timeout
	n := 0
	for got[0].Rows.Next() {
		n++
	}
	if err := got[0].Rows.Err(); err != nil || n != 3 {
		t.Errorf("read %d rows, err %v; want 3 rows: the time started when the query returned", n, err)
	}
	for _, r := range got {
		_ = r.Rows.Close()
	}
}

// Reading rows is still bounded.
func TestRowsAreStillBoundedAfterTheQueryReturns(t *testing.T) {
	pool := newPool(t, &fakeShard{query: func(context.Context) (int, error) { return 3, nil }})
	got, err := NewExecutor(pool, Options{ShardTimeout: 50 * time.Millisecond}).Query(context.Background(), planFor("s0"))
	if err != nil {
		t.Fatal(err)
	}
	defer got[0].Rows.Close()
	time.Sleep(150 * time.Millisecond)
	for got[0].Rows.Next() {
		t.Fatal("rows were still readable long after the shard timeout")
	}
	if err := got[0].Rows.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Err = %v, want DeadlineExceeded", err)
	}
}

// The slow shard times out after exactly as long as the fast shard's timeout.
// The fast shard's rows must still be usable for the partial result.
func TestPartialResultWhenAShardTimesOut(t *testing.T) {
	pool := newPool(t,
		&fakeShard{query: func(context.Context) (int, error) { return 3, nil }},
		&fakeShard{query: blockUntilDone},
	)
	got, failed, err := NewExecutor(pool, Options{ShardTimeout: 80 * time.Millisecond}).QueryPartial(context.Background(), planFor("s0", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || failed[0].Shard != "s1" || !errors.Is(failed[0], context.DeadlineExceeded) {
		t.Fatalf("failed = %v, want s1 timed out", failed)
	}
	n := 0
	for got[0].Rows.Next() {
		n++
	}
	if err := got[0].Rows.Err(); err != nil || n != 3 {
		t.Errorf("read %d rows, err %v; want the 3 rows of the shard that answered", n, err)
	}
	_ = got[0].Rows.Close()
}
