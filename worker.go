package sqlitebatch

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrWorkerClosed = errors.New("sqlite batch worker is closed")

type Worker[T any] struct {
	queue    chan T
	done     chan struct{}
	mu       sync.Mutex
	closed   bool
	validate func(T) error
}

func (w *Worker[T]) Submit(item T) error {
	if err := w.validate(item); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWorkerClosed
	}
	w.queue <- item
	return nil
}

func (w *Worker[T]) Close() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.queue)
	}
	w.mu.Unlock()
	<-w.done
}

type PreparedWorkerItem struct {
	Args     []any
	Response chan error
}

func StartPreparedWorker(db *sql.DB, stmt *sql.Stmt, opts Options) (*Worker[PreparedWorkerItem], error) {
	if db == nil {
		return nil, errors.New("sqlite batch worker requires a database")
	}
	if stmt == nil {
		return nil, errors.New("sqlite batch prepared worker requires a statement")
	}
	return newWorker(opts, func(batch []PreparedWorkerItem) {
		executePreparedBatch(db, stmt, batch)
	}, func(item PreparedWorkerItem) error {
		if item.Response == nil {
			return errors.New("sqlite batch worker item requires a response channel")
		}
		return nil
	})
}

func executePreparedBatch(db *sql.DB, stmt *sql.Stmt, batch []PreparedWorkerItem) {
	tx, err := db.Begin()
	if err != nil {
		for _, item := range batch {
			item.Response <- err
		}
		return
	}

	txStmt := tx.Stmt(stmt)

	commitList := make([]PreparedWorkerItem, 0, len(batch))
	for i, item := range batch {
		savepointName := fmt.Sprintf("sp%d", i)
		_, err := tx.Exec(fmt.Sprintf("SAVEPOINT %s", savepointName))
		if err != nil {
			item.Response <- err
			continue
		}
		_, err = txStmt.Exec(item.Args...)
		if err != nil {
			tx.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName))
			tx.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))
			item.Response <- err
			continue
		}
		tx.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))
		commitList = append(commitList, item)
	}

	err = tx.Commit()
	for _, item := range commitList {
		item.Response <- err
	}
}

type WorkerItem struct {
	Query    string
	Args     []any
	Response chan error
}

func StartWorker(db *sql.DB, opts Options) (*Worker[WorkerItem], error) {
	if db == nil {
		return nil, errors.New("sqlite batch worker requires a database")
	}
	return newWorker(opts, func(batch []WorkerItem) {
		executeBatch(db, batch)
	}, func(item WorkerItem) error {
		if item.Response == nil {
			return errors.New("sqlite batch worker item requires a response channel")
		}
		return nil
	})
}

func newWorker[T any](opts Options, execute func([]T), validate func(T) error) (*Worker[T], error) {
	if opts.MaxBatch <= 0 {
		return nil, errors.New("sqlite batch worker MaxBatch must be positive")
	}
	if opts.MaxDelay <= 0 {
		return nil, errors.New("sqlite batch worker MaxDelay must be positive")
	}
	if opts.QueueSize < 0 {
		return nil, errors.New("sqlite batch worker QueueSize cannot be negative")
	}

	worker := &Worker[T]{
		queue:    make(chan T, opts.QueueSize),
		done:     make(chan struct{}),
		validate: validate,
	}
	go runWorker(worker, opts, execute)
	return worker, nil
}

func runWorker[T any](worker *Worker[T], opts Options, execute func([]T)) {
	defer close(worker.done)

	batch := make([]T, 0, opts.MaxBatch)
	var timer *time.Timer
	var timerC <-chan time.Time
	flush := func() {
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer = nil
			timerC = nil
		}
		if len(batch) > 0 {
			execute(batch)
			batch = batch[:0]
		}
	}

	for {
		select {
		case item, ok := <-worker.queue:
			if !ok {
				flush()
				return
			}
			if len(batch) == 0 {
				timer = time.NewTimer(opts.MaxDelay)
				timerC = timer.C
			}
			batch = append(batch, item)
			if len(batch) >= opts.MaxBatch {
				flush()
			}
		case <-timerC:
			flush()
		}
	}
}

func executeBatch(db *sql.DB, batch []WorkerItem) {
	tx, err := db.Begin()
	if err != nil {
		for _, item := range batch {
			item.Response <- err
		}
		return
	}

	commitList := make([]WorkerItem, 0, len(batch))
	for i, item := range batch {
		savepointName := fmt.Sprintf("sp%d", i)
		_, err := tx.Exec(fmt.Sprintf("SAVEPOINT %s", savepointName))
		if err != nil {
			item.Response <- err
			continue
		}
		_, err = tx.Exec(item.Query, item.Args...)
		if err != nil {
			tx.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName))
			tx.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))
			item.Response <- err
			continue
		}
		tx.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))
		commitList = append(commitList, item)
	}

	err = tx.Commit()
	for _, item := range commitList {
		item.Response <- err
	}
}
