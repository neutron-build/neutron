package neutronrealtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sync"
)

// WebSocketConn abstracts a WebSocket connection. Implement this interface
// using your preferred WebSocket library (e.g., nhooyr.io/websocket,
// gorilla/websocket, golang.org/x/net/websocket).
type WebSocketConn interface {
	// ReadMessage blocks until a message is received or the connection closes.
	// Returns the message bytes and any error (io.EOF on close).
	ReadMessage(ctx context.Context) ([]byte, error)

	// WriteMessage sends a message on the WebSocket.
	WriteMessage(ctx context.Context, msg []byte) error

	// Close closes the WebSocket connection.
	Close() error
}

// Upgrader is a function that upgrades an HTTP request to a WebSocket connection.
// Implement this using your preferred WebSocket library.
type Upgrader func(w http.ResponseWriter, r *http.Request) (WebSocketConn, error)

// WebSocketHandler returns an http.Handler that upgrades HTTP connections to
// WebSocket using the provided Upgrader, and registers them with the Hub.
// Messages received from the client are broadcast to all hub connections.
//
// The handler validates the Origin header against the request Host to prevent
// cross-origin WebSocket hijacking.
func WebSocketHandler(hub *Hub, upgrader Upgrader) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Validate Origin header to prevent cross-origin WebSocket hijacking
		if origin := r.Header.Get("Origin"); origin != "" {
			originURL, err := url.Parse(origin)
			if err != nil || originURL.Host != r.Host {
				http.Error(w, "Origin not allowed", http.StatusForbidden)
				return
			}
		}

		ws, err := upgrader(w, r)
		if err != nil {
			http.Error(w, "WebSocket upgrade failed", http.StatusBadRequest)
			return
		}
		defer ws.Close()

		connID := generateConnID()
		conn := NewConn(connID, 256)
		hub.Register(conn)
		defer hub.Unregister(conn)

		ctx := r.Context()

		// Writer goroutine: send messages from hub to websocket
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range conn.Send {
				if err := ws.WriteMessage(ctx, msg); err != nil {
					return
				}
			}
		}()

		// Reader loop: read messages from websocket and broadcast
		for {
			msg, err := ws.ReadMessage(ctx)
			if err != nil {
				break
			}
			hub.BroadcastAll(msg)
		}

		wg.Wait()
	})
}

// WebSocketHandlerWithRoom returns an http.Handler that upgrades HTTP connections
// to WebSocket and auto-subscribes them to the given room. Messages received
// from the client are broadcast to that room.
//
// The handler validates the Origin header against the request Host to prevent
// cross-origin WebSocket hijacking.
func WebSocketHandlerWithRoom(hub *Hub, room string, upgrader Upgrader) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Validate Origin header to prevent cross-origin WebSocket hijacking
		if origin := r.Header.Get("Origin"); origin != "" {
			originURL, err := url.Parse(origin)
			if err != nil || originURL.Host != r.Host {
				http.Error(w, "Origin not allowed", http.StatusForbidden)
				return
			}
		}

		ws, err := upgrader(w, r)
		if err != nil {
			http.Error(w, "WebSocket upgrade failed", http.StatusBadRequest)
			return
		}
		defer ws.Close()

		connID := generateConnID()
		conn := NewConn(connID, 256)
		hub.Register(conn)
		hub.Subscribe(room, conn)
		defer func() {
			hub.Unsubscribe(room, conn)
			hub.Unregister(conn)
		}()

		ctx := r.Context()

		// Writer goroutine
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range conn.Send {
				if err := ws.WriteMessage(ctx, msg); err != nil {
					return
				}
			}
		}()

		// Reader loop
		for {
			msg, err := ws.ReadMessage(ctx)
			if err != nil {
				break
			}
			hub.Broadcast(room, msg)
		}

		wg.Wait()
	})
}

func generateConnID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("conn-%p", &b)
	}
	return hex.EncodeToString(b)
}
