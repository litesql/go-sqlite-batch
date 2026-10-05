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

## Worker Helpers

If you can't register the wrapper as a `database/sql` driver, use `StartWorker` directly with your `*sql.DB` and send `WorkerItem` values to its returned channel:

```go
worker := sqlitebatch.StartWorker(db, sqlitebatch.Options{
	MaxBatch:  100,
	MaxDelay:  20 * time.Millisecond,
	QueueSize: 500,
})

response := make(chan error, 1)
worker <- sqlitebatch.WorkerItem{
	Query:    `INSERT INTO records(value) VALUES (?)`,
	Args:     []any{"value"},
	Response: response,
}
if err := <-response; err != nil {
	panic(err)
}
```

`StartWorker` flushes when the batch reaches `MaxBatch` or when `MaxDelay` expires. Each item runs in a transaction under its own savepoint, so one failed query does not roll back other successful items. Each item receives exactly one result on its `Response` channel: `nil` after a successful commit, or an error if the item or transaction fails. Provide a writable response channel for every item. If the worker cannot send a result because the channel is unbuffered and no receiver is ready, it blocks.

Use `StartPreparedWorker` to batch arguments for an existing prepared statement:

```go
stmt, err := db.Prepare(`INSERT INTO records(value) VALUES (?)`)
if err != nil {
	panic(err)
}

worker := sqlitebatch.StartPreparedWorker(db, stmt, sqlitebatch.Options{
	MaxBatch:  100,
	MaxDelay:  20 * time.Millisecond,
	QueueSize: 500,
})

response := make(chan error, 1)
worker <- sqlitebatch.PreparedWorkerItem{
	Args:     []any{"value"},
	Response: response,
}
if err := <-response; err != nil {
	panic(err)
}
```

Both helpers start a background worker and return a send-only channel; they do not provide a stop method. Keep the database and, for `StartPreparedWorker`, the statement open while the worker is in use. Unlike `New`, the helpers do not fill in defaults: pass a positive `MaxDelay`, a positive `MaxBatch`, and a non-negative `QueueSize`.

## Benchmark

Compare concurrent single-row writes against the base SQLite driver:

```sh
go test . -run '^$' -bench '^BenchmarkSQLiteConcurrentWrites$' -benchmem -count=3
```

The benchmark reports `ns/op`, allocations, and `writes/s` for autocommit and batch modes. Results depend on SQLite settings, storage, and concurrency; measure with the workload and durability settings used by your application.

Run the driver tests with `go test ./...`.

