package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go-ai-future/internal/data"
	"go-ai-future/internal/web"
	"go-ai-future/workflow"

	_ "modernc.org/sqlite"
)

func main() {
	addr := envOrDefault("ADDR", ":8080")
	workflowDir := envOrDefault("WORKFLOW_DIR", "workflows")
	dbPath := envOrDefault("SQLITE_PATH", filepath.Join("data", "demo.db"))

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping sqlite: %v", err)
	}

	if err := data.EnsureDemoData(ctx, db); err != nil {
		log.Fatalf("bootstrap demo data: %v", err)
	}

	catalog, err := workflow.LoadDir(workflowDir)
	if err != nil {
		log.Fatalf("load workflows: %v", err)
	}

	executor := workflow.NewExecutor(map[string]*sql.DB{
		"default": db,
	})

	server, err := web.NewServer(catalog, executor)
	if err != nil {
		log.Fatalf("build web server: %v", err)
	}

	mux := http.NewServeMux()
	server.Register(mux)

	log.Printf("AI workflow web is listening on http://localhost%s", addr)

	if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("listen: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
