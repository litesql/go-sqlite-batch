package main

import (
	"database/sql"
	"net/http"
	"time"

	sqlitebatch "github.com/litesql/go-sqlite-batch"
	"modernc.org/sqlite"
)

func main() {
	sql.Register("sqlite-batch", sqlitebatch.New(sqlitebatch.Options{
		BaseDriver: &sqlite.Driver{},
		MaxBatch:   200,
		MaxDelay:   20 * time.Millisecond,
		QueueSize:  5000,
	}))

	db, err := sql.Open("sqlite-microbatch", "file:example.db?cache=shared&mode=rwc")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS logs(
		id INTEGER PRIMARY KEY,
		line TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/logs", handleLogs(db))
	http.ListenAndServe(":8080", nil)
}

func handleLogs(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		line := r.URL.Query().Get("line")
		if _, err := db.ExecContext(r.Context(), `INSERT INTO logs (line) VALUES (?)`, line); err != nil {
			http.Error(w, "failed to insert log", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
