package sqlitebatch_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	sqlitebatch "github.com/litesql/go-sqlite-batch"
	sqlite "modernc.org/sqlite"
)

var registerBenchmarkDriver sync.Once

func BenchmarkSQLiteConcurrentWrites(b *testing.B) {
	registerBenchmarkDriver.Do(func() {
		sql.Register("sqlite-batch-bench", sqlitebatch.New(&sqlite.Driver{}, sqlitebatch.Options{
			MaxBatch:  64,
			MaxDelay:  1 * time.Millisecond,
			QueueSize: 4096,
		}))
	})

	b.Run("sqlite-autocommit", func(b *testing.B) {
		benchmarkSQLiteWrites(b, "sqlite", 1)
	})
	b.Run("batch", func(b *testing.B) {
		benchmarkSQLiteWrites(b, "sqlite-batch-bench", max(2, runtime.GOMAXPROCS(0)*4))
	})
}

func benchmarkSQLiteWrites(b *testing.B, driverName string, maxOpenConns int) {
	b.Helper()
	db, err := sql.Open(driverName, filepath.Join(b.TempDir(), "benchmark.db"))
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns)
	if _, err := db.Exec(`CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Errorf("close database: %v", err)
		}
	})

	b.ReportAllocs()
	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := db.ExecContext(context.Background(), `INSERT INTO records(value) VALUES ('payload')`); err != nil {
				b.Errorf("insert: %v", err)
				return
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/s")
}
