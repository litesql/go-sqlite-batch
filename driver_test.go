package sqlitebatch_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	sqlitebatch "github.com/litesql/go-sqlite-batch"
	sqlite "modernc.org/sqlite"
)

var registerDriver sync.Once

func openDB(t *testing.T) *sql.DB {
	t.Helper()

	const name = "sqlite-batch-test"
	registerDriver.Do(func() {
		sql.Register(name, sqlitebatch.New(sqlitebatch.Options{
			BaseDriver: &sqlite.Driver{},
			MaxBatch:   32,
			MaxDelay:   10 * time.Millisecond,
			QueueSize:  256,
		}))
	})

	db, err := sql.Open(name, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})

	if _, err := db.Exec(`CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestBatchesConcurrentExecAndPreparedStatements(t *testing.T) {
	db := openDB(t)
	statement, err := db.Prepare(`INSERT INTO records(value) VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()

	const count = 60
	var group sync.WaitGroup
	errors := make(chan error, count)
	for i := 0; i < count; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			_, err := statement.ExecContext(context.Background(), fmt.Sprintf("value-%d", i))
			if err != nil {
				errors <- err
			}
		}(i)
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}

	var got int
	if err := db.QueryRow(`SELECT count(*) FROM records`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != count {
		t.Fatalf("got %d records, want %d", got, count)
	}
}

func TestFailedExecDoesNotRollbackOtherOperations(t *testing.T) {
	db := openDB(t)

	if _, err := db.Exec(`INSERT INTO records(value) VALUES ('kept-before')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO missing_table(value) VALUES ('bad')`); err == nil {
		t.Fatal("expected error for missing table")
	}
	if _, err := db.Exec(`INSERT INTO records(value) VALUES ('kept-after')`); err != nil {
		t.Fatal(err)
	}

	var got int
	if err := db.QueryRow(`SELECT count(*) FROM records`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 2 {
		t.Fatalf("got %d records, want 2", got)
	}
}

func TestExplicitTransactionRollbackIsPreserved(t *testing.T) {
	db := openDB(t)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO records(value) VALUES ('rolled-back')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var got int
	if err := db.QueryRow(`SELECT count(*) FROM records`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("got %d records after rollback, want 0", got)
	}
}
