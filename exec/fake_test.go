package exec

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync/atomic"
)

// fakeShard is a database/sql driver whose behavior a test controls. It lets
// the executor be tested with real *sql.DB / *sql.Rows semantics and no
// database.
type fakeShard struct {
	// query is called for every query; it may block, and should return when ctx
	// is done. A nil error yields `rows` rows of one int column (the row index).
	query func(ctx context.Context) (rows int, err error)
	exec  func(ctx context.Context) (affected int64, err error)
	ping  func(ctx context.Context) error

	inFlight, maxInFlight atomic.Int32
	openRows              atomic.Int32
	cancelledQueries      atomic.Int32
}

func (f *fakeShard) db() *sql.DB { return sql.OpenDB(fakeConnector{f}) }

type fakeConnector struct{ f *fakeShard }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return &fakeConn{c.f}, nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, io.ErrUnexpectedEOF }

type fakeConn struct{ f *fakeShard }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, io.ErrUnexpectedEOF }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return nil, io.ErrUnexpectedEOF }

func (c *fakeConn) Ping(ctx context.Context) error {
	if c.f.ping != nil {
		return c.f.ping(ctx)
	}
	return nil
}

func (c *fakeConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	n := c.f.inFlight.Add(1)
	for {
		m := c.f.maxInFlight.Load()
		if n <= m || c.f.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	defer c.f.inFlight.Add(-1)

	rows, err := 0, error(nil)
	if c.f.query != nil {
		rows, err = c.f.query(ctx)
	}
	if ctx.Err() != nil {
		c.f.cancelledQueries.Add(1)
	}
	if err != nil {
		return nil, err
	}
	c.f.openRows.Add(1)
	return &fakeRows{f: c.f, n: rows}, nil
}

func (c *fakeConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	if c.f.exec == nil {
		return driver.RowsAffected(1), nil
	}
	n, err := c.f.exec(ctx)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(n), nil
}

type fakeRows struct {
	f      *fakeShard
	n, pos int
	closed bool
}

func (r *fakeRows) Columns() []string { return []string{"n"} }

func (r *fakeRows) Close() error {
	if !r.closed {
		r.closed = true
		r.f.openRows.Add(-1)
	}
	return nil
}

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= r.n {
		return io.EOF
	}
	dest[0] = int64(r.pos)
	r.pos++
	return nil
}
