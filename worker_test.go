package sqlitebatch_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	sqlitebatch "github.com/litesql/go-sqlite-batch"
	_ "modernc.org/sqlite"
)

func openWorkerTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(2)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})

	if _, err := db.Exec(`CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func waitForRecordCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var got int
		if err := db.QueryRow(`SELECT count(*) FROM records`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("record count did not reach %d before timeout", want)
}

func TestStartWorkerFlushesByBatchSizeOrDelay(t *testing.T) {
	tests := []struct {
		name    string
		options sqlitebatch.Options
		values  []string
	}{
		{
			name: "batch size",
			options: sqlitebatch.Options{
				MaxBatch:  2,
				MaxDelay:  time.Second,
				QueueSize: 2,
			},
			values: []string{"first", "second"},
		},
		{
			name: "max delay",
			options: sqlitebatch.Options{
				MaxBatch:  2,
				MaxDelay:  20 * time.Millisecond,
				QueueSize: 2,
			},
			values: []string{"only"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openWorkerTestDB(t)
			worker := sqlitebatch.StartWorker(db, test.options)
			for _, value := range test.values {
				worker <- sqlitebatch.WorkerItem{
					Query:    `INSERT INTO records(value) VALUES (?)`,
					Args:     []any{value},
					Response: make(chan error, 1),
				}
			}
			waitForRecordCount(t, db, len(test.values))
		})
	}
}

func TestStartWorkerReportsItemErrorWithoutRollingBackBatch(t *testing.T) {
	db := openWorkerTestDB(t)
	worker := sqlitebatch.StartWorker(db, sqlitebatch.Options{
		MaxBatch:  2,
		MaxDelay:  time.Second,
		QueueSize: 2,
	})

	worker <- sqlitebatch.WorkerItem{
		Query:    `INSERT INTO records(value) VALUES (?)`,
		Args:     []any{"kept"},
		Response: make(chan error, 1),
	}
	response := make(chan error, 1)
	worker <- sqlitebatch.WorkerItem{
		Query:    `INSERT INTO missing_table(value) VALUES (?)`,
		Args:     []any{"bad"},
		Response: response,
	}

	select {
	case err := <-response:
		if err == nil {
			t.Fatal("expected an error for the invalid query")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the invalid query error")
	}
	waitForRecordCount(t, db, 1)
}

func TestStartPreparedWorkerFlushesByBatchSizeOrDelay(t *testing.T) {
	tests := []struct {
		name    string
		options sqlitebatch.Options
		values  []string
	}{
		{
			name: "batch size",
			options: sqlitebatch.Options{
				MaxBatch:  2,
				MaxDelay:  time.Second,
				QueueSize: 2,
			},
			values: []string{"first", "second"},
		},
		{
			name: "max delay",
			options: sqlitebatch.Options{
				MaxBatch:  2,
				MaxDelay:  20 * time.Millisecond,
				QueueSize: 2,
			},
			values: []string{"only"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openWorkerTestDB(t)
			stmt, err := db.Prepare(`INSERT INTO records(value) VALUES (?)`)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := stmt.Close(); err != nil {
					t.Errorf("close statement: %v", err)
				}
			})

			worker := sqlitebatch.StartPreparedWorker(db, stmt, test.options)
			for _, value := range test.values {
				worker <- sqlitebatch.PreparedWorkerItem{
					Args:     []any{value},
					Response: make(chan error, 1),
				}
			}
			waitForRecordCount(t, db, len(test.values))
		})
	}
}

func TestStartPreparedWorkerReportsItemErrorWithoutRollingBackBatch(t *testing.T) {
	db := openWorkerTestDB(t)
	stmt, err := db.Prepare(`INSERT INTO records(value) VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stmt.Close(); err != nil {
			t.Errorf("close statement: %v", err)
		}
	})

	worker := sqlitebatch.StartPreparedWorker(db, stmt, sqlitebatch.Options{
		MaxBatch:  2,
		MaxDelay:  time.Second,
		QueueSize: 2,
	})
	worker <- sqlitebatch.PreparedWorkerItem{
		Args:     []any{"kept"},
		Response: make(chan error, 1),
	}
	response := make(chan error, 1)
	worker <- sqlitebatch.PreparedWorkerItem{
		Args:     []any{"kept"},
		Response: response,
	}

	select {
	case err := <-response:
		if err == nil {
			t.Fatal("expected a uniqueness error for the duplicate value")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the duplicate value error")
	}
	waitForRecordCount(t, db, 1)
}
