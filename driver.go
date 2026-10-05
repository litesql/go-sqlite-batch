package sqlitebatch

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Options controls how standalone Exec operations are grouped.
type Options struct {
	BaseDriver driver.Driver
	MaxBatch   int
	MaxDelay   time.Duration
	QueueSize  int
}

// Driver wraps modernc.org/sqlite and batches standalone Exec operations.
// Explicit transactions and queries continue to use their own database/sql connection.
type Driver struct {
	base  driver.Driver
	opts  Options
	mu    sync.Mutex
	conns map[string]*coordinator
}

// New creates a SQLite database/sql driver with a shared writer worker per DSN.
func New(opts Options) *Driver {
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 200
	}
	if opts.MaxDelay <= 0 {
		opts.MaxDelay = 20 * time.Millisecond
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = 5000
	}
	return &Driver{
		base:  opts.BaseDriver,
		opts:  opts,
		conns: make(map[string]*coordinator),
	}
}

// Open implements database/sql/driver.Driver.
func (d *Driver) Open(name string) (driver.Conn, error) {
	physicalConn, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	coord := d.conns[name]
	if coord == nil {
		writer, openErr := d.base.Open(name)
		if openErr != nil {
			d.mu.Unlock()
			_ = physicalConn.Close()
			return nil, openErr
		}
		coord = newCoordinator(writer, d.opts)
		d.conns[name] = coord
	}
	coord.refs++
	d.mu.Unlock()

	return &batchConn{Conn: physicalConn, owner: d, coord: coord, name: name}, nil
}

func (d *Driver) release(name string, coord *coordinator) {
	d.mu.Lock()
	coord.refs--
	last := coord.refs == 0
	if last {
		delete(d.conns, name)
	}
	d.mu.Unlock()

	if last {
		coord.close()
	}
}

type batchConn struct {
	driver.Conn
	owner *Driver
	coord *coordinator
	name  string
	inTx  atomic.Bool
	close sync.Once
	err   error
}

func (c *batchConn) Close() error {
	c.close.Do(func() {
		c.err = c.Conn.Close()
		c.owner.release(c.name, c.coord)
	})
	return c.err
}

func (c *batchConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(context.Background(), query)
}

func (c *batchConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.prepare(ctx, query)
}

func (c *batchConn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	var stmt driver.Stmt
	var err error
	if preparer, ok := c.Conn.(driver.ConnPrepareContext); ok {
		stmt, err = preparer.PrepareContext(ctx, query)
	} else {
		stmt, err = c.Conn.Prepare(query)
	}
	if err != nil {
		return nil, err
	}
	return &batchStmt{Stmt: stmt, conn: c, query: query}, nil
}

func (c *batchConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *batchConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if !c.inTx.CompareAndSwap(false, true) {
		return nil, errors.New("sqlite batch driver: transaction already active on connection")
	}
	var tx driver.Tx
	var err error
	if beginner, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = beginner.BeginTx(ctx, opts)
	} else if opts.Isolation != driver.IsolationLevel(0) || opts.ReadOnly {
		err = errors.New("sqlite batch driver: transaction options are not supported by the base driver")
	} else {
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		c.inTx.Store(false)
		return nil, err
	}
	return &batchTx{Tx: tx, conn: c}, nil
}

func (c *batchConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.inTx.Load() {
		return execContext(ctx, c.Conn, query, args)
	}
	return c.coord.submit(ctx, query, args)
}

func (c *batchConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := c.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *batchConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

type batchTx struct {
	driver.Tx
	conn *batchConn
}

func (t *batchTx) Commit() error {
	defer t.conn.inTx.Store(false)
	return t.Tx.Commit()
}

func (t *batchTx) Rollback() error {
	defer t.conn.inTx.Store(false)
	return t.Tx.Rollback()
}

type batchStmt struct {
	driver.Stmt
	conn  *batchConn
	query string
}

func (s *batchStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.exec(context.Background(), namedValues(args))
}

func (s *batchStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.exec(ctx, args)
}

func (s *batchStmt) exec(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.conn.inTx.Load() {
		return execStmtContext(ctx, s.Stmt, args)
	}
	return s.conn.coord.submit(ctx, s.query, args)
}

func (s *batchStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.Stmt.Query(args)
}

func (s *batchStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := s.Stmt.(driver.StmtQueryContext); ok {
		return queryer.QueryContext(ctx, args)
	}
	return nil, driver.ErrSkip
}

type job struct {
	ctx    context.Context
	query  string
	args   []driver.NamedValue
	result chan response
}

type response struct {
	result driver.Result
	err    error
}

type coordinator struct {
	writer    driver.Conn
	opts      Options
	queue     chan job
	done      chan struct{}
	seq       atomic.Uint64
	refs      int
	closeOnce sync.Once
}

func newCoordinator(writer driver.Conn, opts Options) *coordinator {
	c := &coordinator{
		writer: writer,
		opts:   opts,
		queue:  make(chan job, opts.QueueSize),
		done:   make(chan struct{}),
	}
	go c.run()
	return c
}

func (c *coordinator) submit(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := job{
		ctx:    ctx,
		query:  query,
		args:   append([]driver.NamedValue(nil), args...),
		result: make(chan response, 1),
	}
	select {
	case c.queue <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	answer := <-request.result
	return answer.result, answer.err
}

func (c *coordinator) close() {
	c.closeOnce.Do(func() {
		close(c.queue)
		<-c.done
	})
}

func (c *coordinator) run() {
	defer close(c.done)
	defer c.writer.Close()
	for first := range c.queue {
		batch := []job{first}
		timer := time.NewTimer(c.opts.MaxDelay)
		collecting := true
		for collecting && len(batch) < c.opts.MaxBatch {
			select {
			case next, ok := <-c.queue:
				if !ok {
					collecting = false
					break
				}
				batch = append(batch, next)
			case <-timer.C:
				collecting = false
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		c.execute(batch)
	}
}

func (c *coordinator) execute(batch []job) {
	beginner, ok := c.writer.(driver.ConnBeginTx)
	if !ok {
		c.fail(batch, errors.New("sqlite batch driver: base connection does not support BeginTx"))
		return
	}
	tx, err := beginner.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		c.fail(batch, err)
		return
	}

	results := make([]response, len(batch))
	var fatalErr error
	for i, request := range batch {
		if err := request.ctx.Err(); err != nil {
			results[i].err = err
			continue
		}
		name := "worker_microbatch_" + strconv.FormatUint(c.seq.Add(1), 10)
		if _, err := execContext(context.Background(), c.writer, "SAVEPOINT "+name, nil); err != nil {
			fatalErr = err
			break
		}
		result, execErr := execContext(request.ctx, c.writer, request.query, request.args)
		if execErr != nil {
			_, rollbackErr := execContext(context.Background(), c.writer, "ROLLBACK TO "+name, nil)
			_, releaseErr := execContext(context.Background(), c.writer, "RELEASE "+name, nil)
			results[i].err = errors.Join(execErr, rollbackErr, releaseErr)
			if rollbackErr != nil || releaseErr != nil {
				fatalErr = errors.Join(rollbackErr, releaseErr)
				break
			}
			continue
		}
		if _, err := execContext(context.Background(), c.writer, "RELEASE "+name, nil); err != nil {
			fatalErr = err
			break
		}
		results[i].result = result
	}

	if fatalErr != nil {
		_ = tx.Rollback()
		for i := range results {
			if results[i].err == nil {
				results[i].err = fatalErr
				results[i].result = nil
			}
		}
	} else if err := tx.Commit(); err != nil {
		for i := range results {
			if results[i].err == nil {
				results[i].err = err
				results[i].result = nil
			}
		}
	}

	for i, request := range batch {
		request.result <- results[i]
	}
}

func (c *coordinator) fail(batch []job, err error) {
	for _, request := range batch {
		request.result <- response{err: err}
	}
}

func execContext(ctx context.Context, conn driver.Conn, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	if execer, ok := conn.(driver.Execer); ok {
		values, err := driverValues(args)
		if err != nil {
			return nil, err
		}
		return execer.Exec(query, values)
	}
	statement, err := conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer statement.Close()
	return execStmtContext(ctx, statement, args)
}

func execStmtContext(ctx context.Context, statement driver.Stmt, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := statement.(driver.StmtExecContext); ok {
		return execer.ExecContext(ctx, args)
	}
	values, err := driverValues(args)
	if err != nil {
		return nil, err
	}
	return statement.Exec(values)
}

func driverValues(args []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		value, err := driver.DefaultParameterConverter.ConvertValue(arg.Value)
		if err != nil {
			return nil, fmt.Errorf("convert argument %d: %w", i+1, err)
		}
		values[i] = value
	}
	return values, nil
}

func namedValues(args []driver.Value) []driver.NamedValue {
	values := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		values[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return values
}
