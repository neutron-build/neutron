package neutronauth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// memoryStore implements SessionStore for testing.
type memoryStore struct {
	data map[string]map[string]any
}

func newMemoryStore() *memoryStore {
	return &memoryStore{data: make(map[string]map[string]any)}
}

func (m *memoryStore) Get(_ context.Context, id string) (map[string]any, error) {
	return m.data[id], nil
}

func (m *memoryStore) Set(_ context.Context, id string, data map[string]any, ttl time.Duration) error {
	m.data[id] = data
	return nil
}

func (m *memoryStore) Delete(_ context.Context, id string) error {
	delete(m.data, id)
	return nil
}

func TestSessionGetSet(t *testing.T) {
	s := &Session{
		ID:   "test-session",
		Data: make(map[string]any),
	}

	// Set and Get
	s.Set("user_id", 42)
	val := s.Get("user_id")
	if val != 42 {
		t.Errorf("Get('user_id') = %v, want 42", val)
	}
}

func TestSessionGetMissing(t *testing.T) {
	s := &Session{
		ID:   "test",
		Data: make(map[string]any),
	}

	val := s.Get("nonexistent")
	if val != nil {
		t.Errorf("Get('nonexistent') = %v, want nil", val)
	}
}

func TestSessionSetMultipleKeys(t *testing.T) {
	s := &Session{
		ID:   "test",
		Data: make(map[string]any),
	}

	s.Set("name", "Alice")
	s.Set("role", "admin")
	s.Set("count", 5)

	if s.Get("name") != "Alice" {
		t.Errorf("name = %v", s.Get("name"))
	}
	if s.Get("role") != "admin" {
		t.Errorf("role = %v", s.Get("role"))
	}
	if s.Get("count") != 5 {
		t.Errorf("count = %v", s.Get("count"))
	}
}

func TestSessionSave(t *testing.T) {
	store := newMemoryStore()
	s := &Session{
		ID:    "sess-1",
		Data:  map[string]any{"key": "value"},
		store: store,
		ttl:   time.Hour,
	}

	err := s.Save(context.Background())
	if err != nil {
		t.Fatalf("Save error: %v", err)
	}

	// Verify data was stored
	got, _ := store.Get(context.Background(), "sess-1")
	if got["key"] != "value" {
		t.Errorf("stored data = %v", got)
	}
}

func TestSessionDestroy(t *testing.T) {
	store := newMemoryStore()
	store.data["sess-1"] = map[string]any{"key": "value"}

	s := &Session{
		ID:    "sess-1",
		Data:  map[string]any{"key": "value"},
		store: store,
		ttl:   time.Hour,
	}

	err := s.Destroy(context.Background())
	if err != nil {
		t.Fatalf("Destroy error: %v", err)
	}

	got, _ := store.Get(context.Background(), "sess-1")
	if got != nil {
		t.Errorf("session should be deleted, got %v", got)
	}
}

func TestSessionFromContextMissing(t *testing.T) {
	ctx := context.Background()
	s := SessionFromContext(ctx)
	if s != nil {
		t.Errorf("expected nil session, got %v", s)
	}
}

func TestSessionFromContextPresent(t *testing.T) {
	sess := &Session{ID: "test-123", Data: make(map[string]any)}
	ctx := context.WithValue(context.Background(), ctxKeySession, sess)

	got := SessionFromContext(ctx)
	if got == nil {
		t.Fatal("expected session in context")
	}
	if got.ID != "test-123" {
		t.Errorf("session ID = %q", got.ID)
	}
}

func TestGenerateSessionID(t *testing.T) {
	id := generateSessionID()
	if len(id) != 64 { // 32 bytes hex-encoded
		t.Errorf("session ID length = %d, want 64", len(id))
	}

	// Uniqueness
	id2 := generateSessionID()
	if id == id2 {
		t.Error("session IDs should be unique")
	}
}

func TestGenerateSessionIDHex(t *testing.T) {
	id := generateSessionID()
	for _, c := range id {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("invalid hex char: %c", c)
		}
	}
}

func TestSessionOptionWithCookieName(t *testing.T) {
	var o sessionOpts
	WithCookieName("my_session")(&o)
	if o.cookieName != "my_session" {
		t.Errorf("cookieName = %q, want my_session", o.cookieName)
	}
}

func TestSessionOptionWithSessionTTL(t *testing.T) {
	var o sessionOpts
	WithSessionTTL(2 * time.Hour)(&o)
	if o.ttl != 2*time.Hour {
		t.Errorf("ttl = %v, want 2h", o.ttl)
	}
}

func TestSessionOptionWithSecure(t *testing.T) {
	var o sessionOpts
	WithSecure(true)(&o)
	if !o.secure {
		t.Error("expected secure = true")
	}
}

func TestSessionMiddlewareSetsCookie(t *testing.T) {
	store := newMemoryStore()
	mw := SessionMiddleware(store)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := SessionFromContext(r.Context())
		if s == nil {
			t.Error("session not found in context")
			return
		}
		s.Set("visited", true)
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}

	// Check that a session cookie was set
	cookies := w.Result().Cookies()
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "session_id" {
			sessionCookie = c
		}
	}
	if sessionCookie == nil {
		t.Fatal("session cookie not set")
	}
	if sessionCookie.Value == "" {
		t.Error("session cookie value is empty")
	}
	if !sessionCookie.HttpOnly {
		t.Error("session cookie should be HttpOnly")
	}
}

func TestSessionMiddlewareCustomCookieName(t *testing.T) {
	store := newMemoryStore()
	mw := SessionMiddleware(store, WithCookieName("my_sess"))

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	var found bool
	for _, c := range w.Result().Cookies() {
		if c.Name == "my_sess" {
			found = true
		}
	}
	if !found {
		t.Error("custom cookie name not used")
	}
}

func TestSessionMiddlewareReusesExistingSession(t *testing.T) {
	store := newMemoryStore()
	store.data["existing-session-id"] = map[string]any{"user": "Alice"}

	mw := SessionMiddleware(store)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := SessionFromContext(r.Context())
		if s == nil {
			t.Error("session not found")
			return
		}
		if s.Get("user") != "Alice" {
			t.Errorf("user = %v, want Alice", s.Get("user"))
		}
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: "existing-session-id"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestSessionMiddlewareCreatesNewForMissingSession(t *testing.T) {
	store := newMemoryStore()
	mw := SessionMiddleware(store)

	var sessionID string
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := SessionFromContext(r.Context())
		if s == nil {
			t.Error("session not found")
			return
		}
		sessionID = s.ID
		if len(s.Data) != 0 {
			t.Errorf("new session should have empty data, got %v", s.Data)
		}
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: "nonexistent-id"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	// A new session ID should have been generated
	if sessionID == "nonexistent-id" {
		t.Error("should not reuse nonexistent session ID")
	}
}

func TestSessionStoreInterface(t *testing.T) {
	// Verify memoryStore satisfies SessionStore
	var _ SessionStore = newMemoryStore()
}
