package neutronrealtime

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"

	"github.com/neutron-dev/neutron-go/neutron"
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
//
// Failure-response contract: the widely used implementations (gorilla/websocket's
// Upgrader.Upgrade, nhooyr.io/websocket's Accept) write an HTTP error response
// to w before returning a non-nil error, and that is the contract this package
// assumes. The handler will not write a second response over one the Upgrader
// already sent. An Upgrader that returns an error without having written
// anything gets an RFC 7807 400 problem+json response written on its behalf.
type Upgrader func(w http.ResponseWriter, r *http.Request) (WebSocketConn, error)

// responseTracker wraps the ResponseWriter handed to an Upgrader and records
// whether the upgrader committed the response — wrote to it or hijacked the
// connection — so the handler can avoid writing a second response over one
// that was already sent (the net/http server logs "superfluous
// response.WriteHeader call" and appends the bytes to the flushed body).
//
// It forwards Flusher and Hijacker rather than hiding them: an upgrader that
// cannot hijack cannot upgrade.
type responseTracker struct {
	http.ResponseWriter
	wrote    bool
	hijacked bool
}

func (t *responseTracker) WriteHeader(code int) {
	t.wrote = true
	t.ResponseWriter.WriteHeader(code)
}

func (t *responseTracker) Write(b []byte) (int, error) {
	t.wrote = true
	return t.ResponseWriter.Write(b)
}

func (t *responseTracker) Flush() {
	t.wrote = true
	if f, ok := t.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (t *responseTracker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := t.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("neutronrealtime: underlying ResponseWriter does not support Hijack")
	}
	t.wrote = true
	t.hijacked = true
	return h.Hijack()
}

// Unwrap exposes the underlying writer to http.ResponseController and to
// interface probes that walk Unwrap chains.
func (t *responseTracker) Unwrap() http.ResponseWriter { return t.ResponseWriter }

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
				neutron.WriteError(w, r, neutron.ErrForbidden("Origin not allowed"))
				return
			}
		}

		tw := &responseTracker{ResponseWriter: w}
		ws, err := upgrader(tw, r)
		if err != nil {
			// The upgrader owns the failure response (see the Upgrader
			// contract). Writing here too would emit a second response over
			// one already sent; only answer if nothing was written.
			if !tw.wrote {
				neutron.WriteError(w, r, neutron.ErrBadRequest(fmt.Sprintf("WebSocket upgrade failed: %v", err)))
			}
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
				neutron.WriteError(w, r, neutron.ErrForbidden("Origin not allowed"))
				return
			}
		}

		tw := &responseTracker{ResponseWriter: w}
		ws, err := upgrader(tw, r)
		if err != nil {
			// The upgrader owns the failure response (see the Upgrader
			// contract). Writing here too would emit a second response over
			// one already sent; only answer if nothing was written.
			if !tw.wrote {
				neutron.WriteError(w, r, neutron.ErrBadRequest(fmt.Sprintf("WebSocket upgrade failed: %v", err)))
			}
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
