// Example: Real-time Chat with Neutron
//
// Demonstrates:
//   - WebSocket-like Hub for room-based messaging
//   - SSE streaming for real-time updates
//   - Nucleus LISTEN/NOTIFY bridge for multi-instance sync
//   - JWT authentication
//
// Run:
//
//	JWT_SECRET=mysecret DATABASE_URL=postgres://localhost:5432/chat go run .
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/neutron-dev/neutron-go/neutron"
	"github.com/neutron-dev/neutron-go/neutronauth"
	"github.com/neutron-dev/neutron-go/neutronrealtime"
	"github.com/neutron-dev/neutron-go/nucleus"
)

type Message struct {
	ID   int64  `json:"id" db:"id"`
	Room string `json:"room" db:"room"`
	User string `json:"user" db:"user_name"`
	Body string `json:"body" db:"body"`
}

type SendMessageInput struct {
	Room string `json:"room" validate:"required"`
	Body string `json:"body" validate:"required,min=1,max=1000"`
}

type JoinRoomInput struct {
	Room string `query:"room" validate:"required"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = "dev-secret-change-me"
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://localhost:5432/chat"
	}

	db, err := nucleus.Connect(context.Background(), dbURL)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}

	hub := neutronrealtime.NewHub()

	app := neutron.New(
		neutron.WithLogger(logger),
		neutron.WithLifecycle(db.LifecycleHook()),
		neutron.WithMiddleware(
			neutron.Logger(logger),
			neutron.Recover(),
			neutron.RequestID(),
		),
	)

	// Public routes
	app.Router().Handle("GET /stream", neutronrealtime.SSEHandler(
		func(ctx interface{ Done() <-chan struct{} }, send func(string, []byte) error) error {
			<-ctx.Done()
			return nil
		},
	))

	// Protected API routes
	api := app.Router().Group("/api",
		neutronauth.JWTMiddleware(jwtSecret),
	)

	// Send message — persists to DB, broadcasts via Hub, notifies other instances
	neutron.Post(api, "/messages", func(ctx context.Context, input SendMessageInput) (Message, error) {
		claims, err := neutronauth.ClaimsFromContext(ctx)
		if err != nil {
			return Message{}, neutron.ErrUnauthorized("missing claims")
		}
		user, _ := claims["sub"].(string)

		msg, err := nucleus.QueryOne[Message](ctx, db.SQL(),
			"INSERT INTO messages (room, user_name, body) VALUES ($1, $2, $3) RETURNING id, room, user_name, body",
			input.Room, user, input.Body)
		if err != nil {
			return Message{}, err
		}

		// Broadcast to local Hub subscribers
		data, _ := json.Marshal(msg)
		hub.Broadcast(input.Room, data)

		// Notify other instances via Nucleus LISTEN/NOTIFY
		db.Notify(ctx, "chat:"+input.Room, string(data))

		return msg, nil
	}, neutron.WithSummary("Send a chat message"), neutron.WithTags("chat"))

	// Get room history
	neutron.Get(api, "/rooms/{room}/messages", func(ctx context.Context, input struct {
		Room string `path:"room"`
	}) ([]Message, error) {
		return nucleus.Query[Message](ctx, db.SQL(),
			"SELECT id, room, user_name, body FROM messages WHERE room = $1 ORDER BY id DESC LIMIT 50",
			input.Room)
	}, neutron.WithSummary("Get room message history"), neutron.WithTags("chat"))

	_ = hub // Hub would be wired to WebSocket upgrade handler in production

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = "8080"
	}
	fmt.Println("Chat server starting on", addr)
	app.Run(":" + addr)
}
