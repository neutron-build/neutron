package neutronrealtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// problemJSON is the wire shape FRAMEWORK_CONTRACT.md §2 requires of every
// error, asserted field by field so a renamed key fails here rather than
// downstream.
type problemJSON struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

// assertProblem checks one response against the contract: status, the
// problem+json media type, and all four required fields mapped to the
// standard titles.
func assertProblem(t *testing.T, w *httptest.ResponseRecorder, status int, typeSuffix, title string) problemJSON {
	t.Helper()
	if w.Code != status {
		t.Errorf("status = %d, want %d", w.Code, status)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/problem+json") {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
	var p problemJSON
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatalf("body is not problem+json: %v (%q)", err, w.Body.String())
	}
	if p.Type != "https://neutron.dev/errors/"+typeSuffix {
		t.Errorf("type = %q, want .../%s", p.Type, typeSuffix)
	}
	if p.Title != title {
		t.Errorf("title = %q, want %q", p.Title, title)
	}
	if p.Status != status {
		t.Errorf("status field = %d, want %d", p.Status, status)
	}
	if p.Detail == "" {
		t.Error("detail is empty; the contract requires it")
	}
	return p
}

// countHeaderWrites wraps a recorder to count WriteHeader calls. The real
// server logs "superfluous response.WriteHeader call" for the second one and
// appends the body to the already-sent response; the recorder alone hides
// the double write because it drops repeated WriteHeader calls.
type countHeaderWrites struct {
	*httptest.ResponseRecorder
	headers int
}

func (c *countHeaderWrites) WriteHeader(code int) {
	c.headers++
	c.ResponseRecorder.WriteHeader(code)
}

// gorillaStyleUpgrader mimics what gorilla/websocket's Upgrader.Upgrade does
// on handshake failure (and nhooyr.io/websocket's Accept likewise): it writes
// an HTTP error response and THEN returns the error.
func gorillaStyleUpgrader(w http.ResponseWriter, _ *http.Request) (WebSocketConn, error) {
	w.Header().Set("Sec-Websocket-Version", "13")
	http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	return nil, errors.New("websocket: the client is not using the websocket protocol")
}

// silentUpgrader returns an error without touching the ResponseWriter.
func silentUpgrader(_ http.ResponseWriter, _ *http.Request) (WebSocketConn, error) {
	return nil, errors.New("boom")
}

func crossOriginWSRequest() *http.Request {
	r := httptest.NewRequest("GET", "http://example.com/ws", nil)
	r.Header.Set("Origin", "http://evil.example")
	return r
}

func TestWebSocketOriginRejectedAsProblem(t *testing.T) {
	for _, tc := range []struct {
		name string
		h    http.Handler
	}{
		{"WebSocketHandler", WebSocketHandler(NewHub(), nil)},
		{"WebSocketHandlerWithRoom", WebSocketHandlerWithRoom(NewHub(), "r", nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.h.ServeHTTP(w, crossOriginWSRequest())
			assertProblem(t, w, http.StatusForbidden, "forbidden", "Forbidden")
		})
	}
}

func TestWebSocketUpgradeFailureUpgraderOwnsTheResponse(t *testing.T) {
	for _, tc := range []struct {
		name string
		h    http.Handler
	}{
		{"WebSocketHandler", WebSocketHandler(NewHub(), gorillaStyleUpgrader)},
		{"WebSocketHandlerWithRoom", WebSocketHandlerWithRoom(NewHub(), "r", gorillaStyleUpgrader)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &countHeaderWrites{ResponseRecorder: httptest.NewRecorder()}
			tc.h.ServeHTTP(w, httptest.NewRequest("GET", "/ws", nil))

			if w.headers != 1 {
				t.Errorf("WriteHeader called %d times for one failed upgrade, want 1 — the upgrader already wrote the failure response", w.headers)
			}
			if body := w.Body.String(); strings.Contains(body, "WebSocket upgrade failed") {
				t.Errorf("handler wrote a second body over the upgrader's response; body = %q", body)
			}
			if body := w.Body.String(); !strings.Contains(body, http.StatusText(http.StatusBadRequest)) {
				t.Errorf("upgrader's own response must survive intact; body = %q", body)
			}
		})
	}
}

func TestWebSocketUpgradeFailureWithoutResponseIsProblem(t *testing.T) {
	for _, tc := range []struct {
		name string
		h    http.Handler
	}{
		{"WebSocketHandler", WebSocketHandler(NewHub(), silentUpgrader)},
		{"WebSocketHandlerWithRoom", WebSocketHandlerWithRoom(NewHub(), "r", silentUpgrader)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.h.ServeHTTP(w, httptest.NewRequest("GET", "/ws", nil))
			assertProblem(t, w, http.StatusBadRequest, "bad-request", "Bad Request")
		})
	}
}

func TestGenerateConnID(t *testing.T) {
	id1 := generateConnID()
	id2 := generateConnID()
	if id1 == "" {
		t.Error("empty conn ID")
	}
	if id1 == id2 {
		t.Error("conn IDs should be unique")
	}
	if len(id1) != 32 { // 16 bytes hex-encoded
		t.Errorf("conn ID length = %d, want 32", len(id1))
	}
}

func TestWebSocketHandlerNotNil(t *testing.T) {
	hub := NewHub()
	// Just verify it returns a non-nil handler
	handler := WebSocketHandler(hub, nil)
	if handler == nil {
		t.Error("handler should not be nil")
	}
}

func TestWebSocketHandlerWithRoomNotNil(t *testing.T) {
	hub := NewHub()
	handler := WebSocketHandlerWithRoom(hub, "test-room", nil)
	if handler == nil {
		t.Error("handler should not be nil")
	}
}
