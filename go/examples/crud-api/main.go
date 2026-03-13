// Example: CRUD API with Neutron
//
// A simple todo-list API demonstrating:
//   - Generic typed handlers
//   - Automatic validation
//   - RFC 7807 error responses
//   - OpenAPI documentation
//   - Nucleus SQL model with pgx
//
// Run:
//
//	DATABASE_URL=postgres://localhost:5432/todos go run .
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/neutron-dev/neutron-go/neutron"
	"github.com/neutron-dev/neutron-go/nucleus"
)

// --- Models ---

type Todo struct {
	ID    int64  `json:"id" db:"id"`
	Title string `json:"title" db:"title"`
	Done  bool   `json:"done" db:"done"`
}

// --- Inputs ---

type CreateTodoInput struct {
	Title string `json:"title" validate:"required,min=1,max=200"`
}

type UpdateTodoInput struct {
	ID    int64  `path:"id"`
	Title string `json:"title" validate:"required,min=1,max=200"`
	Done  bool   `json:"done"`
}

type TodoIDInput struct {
	ID int64 `path:"id"`
}

// --- App ---

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://localhost:5432/todos"
	}

	db, err := nucleus.Connect(context.Background(), dbURL)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}

	app := neutron.New(
		neutron.WithLogger(logger),
		neutron.WithLifecycle(db.LifecycleHook()),
		neutron.WithOpenAPIInfo("Todo API", "1.0.0"),
		neutron.WithMiddleware(
			neutron.Logger(logger),
			neutron.Recover(),
			neutron.RequestID(),
			neutron.CORS(neutron.CORSOptions{AllowOrigins: []string{"*"}}),
		),
	)

	api := app.Router().Group("/api")

	// List todos
	neutron.Get(api, "/todos", func(ctx context.Context, _ neutron.Empty) ([]Todo, error) {
		return nucleus.Query[Todo](ctx, db.SQL(),
			"SELECT id, title, done FROM todos ORDER BY id")
	}, neutron.WithSummary("List all todos"), neutron.WithTags("todos"))

	// Create todo
	neutron.Post(api, "/todos", func(ctx context.Context, input CreateTodoInput) (Todo, error) {
		return nucleus.QueryOne[Todo](ctx, db.SQL(),
			"INSERT INTO todos (title) VALUES ($1) RETURNING id, title, done", input.Title)
	}, neutron.WithSummary("Create a todo"), neutron.WithTags("todos"))

	// Get todo by ID
	neutron.Get(api, "/todos/{id}", func(ctx context.Context, input TodoIDInput) (Todo, error) {
		todo, err := nucleus.QueryOne[Todo](ctx, db.SQL(),
			"SELECT id, title, done FROM todos WHERE id = $1", input.ID)
		if err != nil {
			return Todo{}, neutron.ErrNotFound("todo not found")
		}
		return todo, nil
	}, neutron.WithSummary("Get a todo"), neutron.WithTags("todos"))

	// Update todo
	neutron.Put(api, "/todos/{id}", func(ctx context.Context, input UpdateTodoInput) (Todo, error) {
		return nucleus.QueryOne[Todo](ctx, db.SQL(),
			"UPDATE todos SET title = $1, done = $2 WHERE id = $3 RETURNING id, title, done",
			input.Title, input.Done, input.ID)
	}, neutron.WithSummary("Update a todo"), neutron.WithTags("todos"))

	// Delete todo
	neutron.Delete(api, "/todos/{id}", func(ctx context.Context, input TodoIDInput) (neutron.Empty, error) {
		_, err := db.SQL().Exec(ctx, "DELETE FROM todos WHERE id = $1", input.ID)
		return neutron.Empty{}, err
	}, neutron.WithSummary("Delete a todo"), neutron.WithTags("todos"))

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = "8080"
	}
	app.Run(":" + addr)
}
