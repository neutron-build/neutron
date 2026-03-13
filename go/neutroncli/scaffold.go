package neutroncli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

func cmdNew(name string) int {
	if name == "" {
		fmt.Fprintln(os.Stderr, "Project name is required")
		return 1
	}

	// Sanitize name
	name = strings.ToLower(strings.ReplaceAll(name, " ", "-"))

	if err := scaffoldProject(name); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	fmt.Printf("Created project %s/\n\n", name)
	fmt.Printf("  cd %s\n", name)
	fmt.Println("  go mod tidy")
	fmt.Println("  neutron dev          # or: neutron-go dev")
	fmt.Println()
	return 0
}

func scaffoldProject(name string) error {
	dirs := []string{
		name,
		filepath.Join(name, "cmd", "server"),
		filepath.Join(name, "internal", "handler"),
		filepath.Join(name, "internal", "model"),
		filepath.Join(name, "migrations"),
	}

	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}

	files := []scaffoldFile{
		{filepath.Join(name, "go.mod"), goModTmpl},
		{filepath.Join(name, "cmd", "server", "main.go"), mainTmpl},
		{filepath.Join(name, "internal", "handler", "health.go"), healthHandlerTmpl},
		{filepath.Join(name, "internal", "model", "model.go"), modelTmpl},
		{filepath.Join(name, "migrations", "001_init.up.sql"), migrationTmpl},
		{filepath.Join(name, "migrations", "001_init.down.sql"), migrationDownTmpl},
		{filepath.Join(name, ".env.example"), envExampleTmpl},
		{filepath.Join(name, ".gitignore"), gitignoreTmpl},
	}

	data := map[string]string{
		"Name":   name,
		"Module": "github.com/your-org/" + name,
	}

	for _, f := range files {
		if err := writeTemplate(f.path, f.tmpl, data); err != nil {
			return err
		}
	}

	return nil
}

type scaffoldFile struct {
	path string
	tmpl string
}

func writeTemplate(path, tmplStr string, data any) error {
	t, err := template.New(filepath.Base(path)).Parse(tmplStr)
	if err != nil {
		return fmt.Errorf("parse template %s: %w", path, err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	return t.Execute(f, data)
}

var goModTmpl = `module {{.Module}}

go 1.22

require (
	github.com/neutron-dev/neutron-go v0.1.0
)
`

var mainTmpl = `package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/neutron-dev/neutron-go/neutron"
	"github.com/neutron-dev/neutron-go/nucleus"

	"{{.Module}}/internal/handler"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://localhost:5432/{{.Name}}"
	}

	db, err := nucleus.Connect(context.Background(), dbURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}

	app := neutron.New(
		neutron.WithLogger(logger),
		neutron.WithLifecycle(db.LifecycleHook()),
		neutron.WithMiddleware(
			neutron.Logger(logger),
			neutron.Recover(),
			neutron.RequestID(),
			neutron.CORS(neutron.CORSOptions{
				AllowOrigins: []string{"*"},
			}),
		),
	)

	api := app.Router().Group("/api")
	handler.RegisterHealth(api)

	_ = db // use db in your handlers

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = "8080"
	}
	app.Run(":" + addr)
}
`

var healthHandlerTmpl = `package handler

import (
	"context"

	"github.com/neutron-dev/neutron-go/neutron"
)

type HealthResponse struct {
	Status string ` + "`" + `json:"status"` + "`" + `
}

func RegisterHealth(r *neutron.Router) {
	neutron.Get(r, "/health", func(ctx context.Context, _ neutron.Empty) (HealthResponse, error) {
		return HealthResponse{Status: "ok"}, nil
	})
}
`

var modelTmpl = `package model

// Define your database models here.
//
// Example:
//
//	type User struct {
//		ID        int64     ` + "`" + `json:"id" db:"id"` + "`" + `
//		Email     string    ` + "`" + `json:"email" db:"email"` + "`" + `
//		Name      string    ` + "`" + `json:"name" db:"name"` + "`" + `
//		CreatedAt time.Time ` + "`" + `json:"created_at" db:"created_at"` + "`" + `
//	}
`

var migrationTmpl = `-- Initial schema
CREATE TABLE IF NOT EXISTS _schema_version (
    version INT PRIMARY KEY,
    applied_at TIMESTAMPTZ DEFAULT now()
);
`

var migrationDownTmpl = `DROP TABLE IF EXISTS _schema_version;
`

var envExampleTmpl = `DATABASE_URL=postgres://localhost:5432/{{.Name}}
PORT=8080
LOG_LEVEL=debug
`

var gitignoreTmpl = `.env
*.exe
/tmp
`
