package sqlitebatch

import (
	"database/sql"
	"fmt"
	"time"
)

type PreparedWorkerItem struct {
	Args     []any
	Response chan error
}

func StartPreparedWorker(db *sql.DB, stmt *sql.Stmt, opts Options) chan<- PreparedWorkerItem {
	ticker := time.Tick(opts.MaxDelay)
	batch := make([]PreparedWorkerItem, 0, opts.MaxBatch)
	queue := make(chan PreparedWorkerItem, opts.QueueSize)

	go func() {
		for {
			select {
			case item := <-queue:
				batch = append(batch, item)
				if len(batch) >= opts.MaxBatch {
					executePreparedBatch(db, stmt, batch)
					batch = batch[:0]
				}
			case <-ticker:
				if len(batch) > 0 {
					executePreparedBatch(db, stmt, batch)
					batch = batch[:0]
				}
			}
		}
	}()

	return queue
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

func StartWorker(db *sql.DB, opts Options) chan<- WorkerItem {
	ticker := time.Tick(opts.MaxDelay)
	batch := make([]WorkerItem, 0, opts.MaxBatch)
	queue := make(chan WorkerItem, opts.QueueSize)

	go func() {
		for {
			select {
			case item := <-queue:
				batch = append(batch, item)
				if len(batch) >= opts.MaxBatch {
					executeBatch(db, batch)
					batch = batch[:0]
				}
			case <-ticker:
				if len(batch) > 0 {
					executeBatch(db, batch)
					batch = batch[:0]
				}
			}
		}
	}()

	return queue
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
