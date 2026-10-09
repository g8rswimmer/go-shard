package exec

import (
	"context"
	"sync"
	"time"
)

// pushable is a context that ends with context.DeadlineExceeded after d, and
// whose time can be started over.
//
// A query's shard timeout covers starting the query and then reading its rows.
// Both must have their own budget: the rows of a shard that answered quickly
// are read only after the slowest shard has answered (or failed, after a
// timeout of its own), so a deadline fixed when the query began would be spent
// by the time the caller can start reading. A shard's clock is therefore
// paused when it answers and started again when every shard has answered, so
// waiting for others never counts against it.
type pushable struct {
	context.Context // the parent: values

	d    time.Duration
	done chan struct{}

	mu    sync.Mutex
	err   error
	timer *time.Timer
	stop  func() bool // detaches from the parent
}

func newPushable(parent context.Context, d time.Duration) *pushable {
	c := &pushable{Context: parent, d: d, done: make(chan struct{})}
	c.timer = time.AfterFunc(d, func() { c.end(context.DeadlineExceeded) })
	c.mu.Lock() // end reads stop; the parent may already be done
	c.stop = context.AfterFunc(parent, func() { c.end(parent.Err()) })
	c.mu.Unlock()
	return c
}

func (c *pushable) end(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	c.err = err
	close(c.done)
	c.timer.Stop()
	if c.stop != nil {
		c.stop()
	}
}

func (c *pushable) Done() <-chan struct{} { return c.done }

func (c *pushable) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Deadline reports none: it moves, and the database/sql and pgx code that
// reads it only needs Done.
func (c *pushable) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *pushable) cancel() { c.end(context.Canceled) }

// pause stops the clock until restart. A shard that has answered is paused
// while the others are still answering.
func (c *pushable) pause() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timer.Stop()
}

// restart gives the context its full time again, unless it has already ended.
func (c *pushable) restart() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.timer.Reset(c.d)
	}
}
