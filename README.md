# go-sqlite-batch

A `database/sql` wrapper driver for micro-batching standalone SQLite writes.

## Usage

Register the wrapper under its own driver name before calling `sql.Open`:

```go
sql.Register("sqlite-batch", sqlitebatch.New(sqlitebatch.Options{
    BaseDriver: &sqlite.Driver{},
	MaxBatch:  200,
	MaxDelay:  20 * time.Millisecond,
	QueueSize: 5000,
}))

db, err := sql.Open("sqlite-batch", "file:app.db?mode=rwc")
```

Standalone `Exec` calls, including executions through prepared statements, are queued in a bounded channel and committed together when `MaxBatch` is reached or `MaxDelay` expires. Each operation gets a savepoint: one failed statement does not undo successful statements in that batch. The caller receives success only after the physical transaction commits.

Queries and explicit transactions use the normal connection path. Statements that return rows, including SQLite `RETURNING`, are not batched. A separate SQLite connection is reserved for the shared writer worker for each DSN; the other connections remain available for reads and explicit transactions.

## Benchmark

Compare concurrent single-row writes against the base SQLite driver:

```sh
go test . -run '^$' -bench '^BenchmarkSQLiteConcurrentWrites$' -benchmem -count=3
```

The benchmark reports `ns/op`, allocations, and `writes/s` for autocommit and batch modes. Results depend on SQLite settings, storage, and concurrency; measure with the workload and durability settings used by your application.

Run the driver tests with `go test ./...`.

