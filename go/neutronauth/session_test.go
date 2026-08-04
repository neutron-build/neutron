package neutronauth

import (
	"context"
	"errors"
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

// failingStore fails whichever operations are switched on, so the middleware's
// behavior on a store outage is observable rather than inferred.
type failingStore struct {
	*memoryStore
	failGet    error
	failSet    error
	failDelete error
}

func (f *failingStore) Get(ctx context.Context, id string) (map[string]any, error) {
	if f.failGet != nil {
		return nil, f.failGet
	}
	return f.memoryStore.Get(ctx, id)
}

func (f *failingStore) Set(ctx context.Context, id string, data map[string]any, ttl time.Duration) error {
	if f.failSet != nil {
		return f.failSet
	}
	return f.memoryStore.Set(ctx, id, data, ttl)
}

func (f *failingStore) Delete(ctx context.Context, id string) error {
	if f.failDelete != nil {
		return f.failDelete
	}
	return f.memoryStore.Delete(ctx, id)
}

// sessionCookie returns the session cookie the response set, or nil.
func sessionCookie(t *testing.T, w *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, c := range w.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// GO-001. Regenerate exists to defeat session fixation, and the doc comment
// says to call it after authentication. The cookie was written before the
// handler ran, so rotating the ID inside the handler could never reach the
// browser: the attacker-planted ID stayed valid and addressable, and the
// authenticated data was saved under an ID the browser never sends.
func TestSessionRegenerateRotatesCookieAndRetiresOldID(t *testing.T) {
	store := newMemoryStore()
	store.data["attacker-planted-id"] = map[string]any{}

	mw := SessionMiddleware(store)
	var newID string
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := SessionFromContext(r.Context())
		s.Regenerate()
		s.Set("user_id", 42)
		if err := s.Save(r.Context()); err != nil {
			t.Fatalf("Save: %v", err)
		}
		newID = s.ID
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: "attacker-planted-id"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	c := sessionCookie(t, w, "session_id")
	if c == nil {
		t.Fatal("no session cookie in response")
	}
	if c.Value == "attacker-planted-id" {
		t.Error("cookie still carries the pre-authentication session ID; " +
			"Regenerate did not rotate what the browser holds")
	}
	if c.Value != newID {
		t.Errorf("cookie = %q, want the regenerated ID %q", c.Value, newID)
	}
	if _, ok := store.data["attacker-planted-id"]; ok {
		t.Error("the pre-authentication session record still resolves")
	}
	if got := store.data[newID]["user_id"]; got != 42 {
		t.Errorf("regenerated session data = %v, want user_id 42", got)
	}
}

// The functional half of the same defect: with the cookie never rotated, the
// next request addresses the old record and the login is silently lost.
func TestSessionRegenerateSurvivesTheNextRequest(t *testing.T) {
	store := newMemoryStore()
	mw := SessionMiddleware(store)

	login := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := SessionFromContext(r.Context())
		s.Regenerate()
		s.Set("user_id", 42)
		_ = s.Save(r.Context())
		w.WriteHeader(200)
	}))
	w1 := httptest.NewRecorder()
	login.ServeHTTP(w1, httptest.NewRequest("GET", "/login", nil))
	c := sessionCookie(t, w1, "session_id")
	if c == nil {
		t.Fatal("no session cookie after login")
	}

	var seen any
	after := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = SessionFromContext(r.Context()).Get("user_id")
		w.WriteHeader(200)
	}))
	r2 := httptest.NewRequest("GET", "/me", nil)
	r2.AddCookie(c)
	after.ServeHTTP(httptest.NewRecorder(), r2)

	if seen != 42 {
		t.Errorf("user_id after login = %v, want 42 — the authenticated session "+
			"was written under an ID the browser never received", seen)
	}
}

// GO-001, second acceptance criterion.
func TestSessionDestroyExpiresCookie(t *testing.T) {
	store := newMemoryStore()
	store.data["live-session"] = map[string]any{"user": "Alice"}

	mw := SessionMiddleware(store)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := SessionFromContext(r.Context()).Destroy(r.Context()); err != nil {
			t.Fatalf("Destroy: %v", err)
		}
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/logout", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: "live-session"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	c := sessionCookie(t, w, "session_id")
	if c == nil {
		t.Fatal("no session cookie in response")
	}
	if c.MaxAge >= 0 {
		t.Errorf("MaxAge = %d, want < 0 so the browser drops the cookie", c.MaxAge)
	}
	if c.Value != "" {
		t.Errorf("cookie value = %q, want empty on destroy", c.Value)
	}
	if _, ok := store.data["live-session"]; ok {
		t.Error("destroyed session still in storage")
	}
}

// A destroyed session must not be resurrected by a later Save in the same
// request — otherwise logout is undone by any handler that touches the session.
func TestSessionDestroyIsNotUndoneBySave(t *testing.T) {
	store := newMemoryStore()
	store.data["live-session"] = map[string]any{"user": "Alice"}

	mw := SessionMiddleware(store)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := SessionFromContext(r.Context())
		_ = s.Destroy(r.Context())
		s.Set("user", "Alice")
		_ = s.Save(r.Context())
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/logout", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: "live-session"})
	handler.ServeHTTP(httptest.NewRecorder(), r)

	if len(store.data) != 0 {
		t.Errorf("logout left %d session(s) in storage: %v", len(store.data), store.data)
	}
}

// GO-002. A store outage read as "no session", which mints a fresh anonymous
// session: every user appears logged out, and the failure is invisible.
func TestSessionStoreReadFailureIsNotAnAnonymousSession(t *testing.T) {
	store := &failingStore{memoryStore: newMemoryStore(), failGet: errStoreDown}

	var handlerRan bool
	mw := SessionMiddleware(store)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerRan = true
		w.WriteHeader(200)
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: "real-session"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if handlerRan {
		t.Error("handler ran with a fabricated anonymous session while the store was down")
	}
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 — a store outage must not read as a logged-out user", w.Code)
	}
	if c := sessionCookie(t, w, "session_id"); c != nil && c.Value != "" && c.Value != "real-session" {
		t.Errorf("minted a new session ID (%q) during a store outage", c.Value)
	}
}

// A missing session is not a failing store, and must still create one.
func TestSessionMissingIsDistinctFromStoreDown(t *testing.T) {
	store := &failingStore{memoryStore: newMemoryStore()}
	mw := SessionMiddleware(store)

	var ran bool
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ran = true
		w.WriteHeader(200)
	}))
	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: "no-such-id"})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if !ran || w.Code != 200 {
		t.Errorf("missing session must create a new one: ran=%v status=%d", ran, w.Code)
	}
}

// The error handler is configurable, and a caller can choose to degrade.
func TestSessionErrorHandlerOverride(t *testing.T) {
	store := &failingStore{memoryStore: newMemoryStore(), failGet: errStoreDown}
	var got error
	mw := SessionMiddleware(store, WithSessionErrorHandler(
		func(w http.ResponseWriter, _ *http.Request, err error) {
			got = err
			w.WriteHeader(http.StatusServiceUnavailable)
		}))

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("handler must not run when the error handler wrote a response")
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: "x"})
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 from the override", w.Code)
	}
	if !errors.Is(got, errStoreDown) {
		t.Errorf("error handler got %v, want errStoreDown", got)
	}
}

// A handler that never touches its session must not have it rewritten, so a
// read-only request does not extend a session or churn the store.
func TestSessionUntouchedIsNotRewritten(t *testing.T) {
	store := newMemoryStore()
	store.data["existing"] = map[string]any{"user": "Alice"}
	writes := &countingStore{memoryStore: store}

	mw := SessionMiddleware(writes)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	r := httptest.NewRequest("GET", "/", nil)
	r.AddCookie(&http.Cookie{Name: "session_id", Value: "existing"})
	handler.ServeHTTP(httptest.NewRecorder(), r)

	if writes.sets != 0 {
		t.Errorf("store.Set called %d times for a read-only request, want 0", writes.sets)
	}
}

type countingStore struct {
	*memoryStore
	sets int
}

func (c *countingStore) Set(ctx context.Context, id string, data map[string]any, ttl time.Duration) error {
	c.sets++
	return c.memoryStore.Set(ctx, id, data, ttl)
}

var errStoreDown = errors.New("session store unavailable")

// A panicking handler must still rotate its session. Recovery middleware sits
// outside this one, so before the commit was deferred the panic unwound past
// it: the new ID was never stored, the old one was never deleted, and the
// browser kept a cookie for a session that had just been used to log in.
func TestSessionRotatesEvenIfHandlerPanics(t *testing.T) {
	store := newMemoryStore()
	if err := store.Set(context.Background(), "old-id", map[string]any{"anon": true}, time.Hour); err != nil {
		t.Fatalf("seed: %v", err)
	}

	h := SessionMiddleware(store)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		SessionFromContext(r.Context()).Regenerate()
		panic("handler exploded")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "session_id", Value: "old-id"})
	rec := httptest.NewRecorder()

	func() {
		defer func() { _ = recover() }() // stand in for recovery middleware
		h.ServeHTTP(rec, req)
	}()

	data, err := store.Get(context.Background(), "old-id")
	if err != nil {
		t.Fatalf("get old id: %v", err)
	}
	if data != nil {
		t.Fatal("old session survived a panicking handler — fixation window left open")
	}
}

// The commit-time error handler must never receive a ResponseWriter. At that
// point the status is decided and the cookie is unwritten, so anything that
// writes corrupts the response — the load-time default calls http.Error, which
// would commit a 500 and silently drop the rotated cookie.
func TestCommitErrorHandlerCannotCorruptTheResponse(t *testing.T) {
	store := &failingStore{memoryStore: newMemoryStore(), failDelete: errStoreDown}
	if err := store.Set(context.Background(), "old-id", map[string]any{}, time.Hour); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var reported error
	var sawRequest bool
	h := SessionMiddleware(store,
		WithSessionCommitErrorHandler(func(r *http.Request, err error) {
			reported = err
			sawRequest = r != nil // the load-time path used to pass nil here
		}),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		SessionFromContext(r.Context()).Regenerate()
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "session_id", Value: "old-id"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if reported == nil {
		t.Fatal("a failed delete must be reported, not swallowed")
	}
	if !sawRequest {
		t.Fatal("commit error handler must receive the real request, not nil")
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status was rewritten by the error path: got %d, want %d", rec.Code, http.StatusTeapot)
	}
	if body := rec.Body.String(); body != "ok" {
		t.Fatalf("body was corrupted by the error path: %q", body)
	}
	if len(rec.Result().Cookies()) == 0 {
		t.Fatal("rotated cookie was dropped when the store errored")
	}
}
