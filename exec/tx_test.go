package exec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

func newTxFixture(t *testing.T, opts Options, f *fakeShard) (*Executor, *fakeShard) {
	t.Helper()
	if f == nil {
		f = &fakeShard{query: func(context.Context) (int, error) { return 2, nil }}
	}
	pool := newPool(t, f)
	return NewExecutor(pool, opts), f
}

func TestTxRunsEveryStatementOnOneConnection(t *testing.T) {
	e, f := newTxFixture(t, Options{}, nil)
	ctx := context.Background()

	tx, err := e.Begin(ctx, "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Shard() != "s0" {
		t.Errorf("Shard() = %s", tx.Shard())
	}
	if n, err := tx.Exec(ctx, "INSERT 1", nil); err != nil || n != 1 {
		t.Fatalf("exec: %d, %v", n, err)
	}
	rows, err := tx.Query(ctx, "SELECT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Close(); err != nil || count != 2 {
		t.Fatalf("rows: %d, close %v", count, err)
	}
	if _, err := tx.Exec(ctx, "INSERT 2", nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	want := fmt.Sprint([]string{"BEGIN", "INSERT 1", "SELECT 1", "INSERT 2", "COMMIT"})
	if got := fmt.Sprint(f.queries()); got != want {
		t.Errorf("the shard saw %s\nwant %s", got, want)
	}
	if n := f.connections.Load(); n != 1 {
		t.Errorf("%d connections were opened, want 1: a transaction must stay on one", n)
	}
}

func TestTxRollback(t *testing.T) {
	e, f := newTxFixture(t, Options{}, nil)
	ctx := context.Background()
	tx, _ := e.Begin(ctx, "s0", nil)
	if _, err := tx.Exec(ctx, "INSERT 1", nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(f.queries()); got != fmt.Sprint([]string{"BEGIN", "INSERT 1", "ROLLBACK"}) {
		t.Errorf("the shard saw %s", got)
	}
}

func TestTxAfterItEnds(t *testing.T) {
	e, _ := newTxFixture(t, Options{}, nil)
	ctx := context.Background()
	tx, _ := e.Begin(ctx, "s0", nil)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(ctx, "INSERT", nil); !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("exec after commit: %v, want ErrTxDone", err)
	}
	if _, err := tx.Query(ctx, "SELECT", nil); !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("query after commit: %v, want ErrTxDone", err)
	}
	if err := tx.Commit(); !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("second commit: %v, want ErrTxDone", err)
	}
	// A deferred Rollback after Commit reports ErrTxDone, which callers ignore.
	err := tx.Rollback()
	var se *ShardError
	if !errors.Is(err, sql.ErrTxDone) || !errors.As(err, &se) || se.Shard != "s0" {
		t.Errorf("rollback after commit: %v, want a ShardError wrapping ErrTxDone", err)
	}
}

func TestBeginErrors(t *testing.T) {
	e, _ := newTxFixture(t, Options{}, nil)
	if _, err := e.Begin(context.Background(), "nope", nil); !errors.Is(err, ErrUnknownShard) {
		t.Errorf("error = %v, want ErrUnknownShard", err)
	}

	e, _ = newTxFixture(t, Options{}, &fakeShard{begin: errors.New("too many connections")})
	_, err := e.Begin(context.Background(), "s0", nil)
	var se *ShardError
	if !errors.As(err, &se) || se.Shard != "s0" {
		t.Errorf("error = %v, want a ShardError for s0", err)
	}
}

func TestTxTimeoutAppliesToEachStatementNotTheTransaction(t *testing.T) {
	f := &fakeShard{query: func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}}
	e, _ := newTxFixture(t, Options{ShardTimeout: 30 * time.Millisecond}, f)
	ctx := context.Background()
	tx, err := e.Begin(ctx, "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// Longer than the timeout in total, but each statement is quick.
	for i := 0; i < 3; i++ {
		time.Sleep(25 * time.Millisecond)
		if _, err := tx.Exec(ctx, "UPDATE", nil); err != nil {
			t.Fatalf("statement %d: %v (the timeout must not run for the whole transaction)", i, err)
		}
	}

	if _, err := tx.Query(ctx, "SELECT slow", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a slow statement: error = %v, want DeadlineExceeded", err)
	}
}

func TestCancellingTheBeginContextRollsBack(t *testing.T) {
	e, f := newTxFixture(t, Options{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := e.Begin(ctx, "s0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT", nil); err != nil {
		t.Fatal(err)
	}
	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, q := range f.queries() {
			if q == "ROLLBACK" {
				if err := tx.Commit(); err == nil {
					t.Error("commit after cancellation must fail")
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the transaction was not rolled back after its context was cancelled; the shard saw %v", f.queries())
}
