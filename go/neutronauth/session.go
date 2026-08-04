package neutronauth

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/neutron-dev/neutron-go/neutron"
	"github.com/neutron-dev/neutron-go/nucleus"
)

// SessionStore is the interface for session backends.
type SessionStore interface {
	Get(ctx context.Context, id string) (map[string]any, error)
	Set(ctx context.Context, id string, data map[string]any, ttl time.Duration) error
	Delete(ctx context.Context, id string) error
}

// Session provides access to session data from the request context.
//
// The cookie that addresses a session is written when the response headers are
// committed, not when the request arrives, so an ID change made by the handler
// reaches the browser. See [SessionMiddleware].
type Session struct {
	ID   string
	Data map[string]any

	store SessionStore
	ttl   time.Duration
	// originalID is the ID the request arrived with, or the minted ID for a
	// new session. Rotation and destruction are both defined against it.
	originalID string
	destroyed  bool
}

// Get returns a session value.
func (s *Session) Get(key string) any {
	return s.Data[key]
}

// Set stores a session value.
func (s *Session) Set(key string, value any) {
	s.Data[key] = value
}

// ErrSessionDestroyed is returned by Save when the session was destroyed
// earlier in the same request.
var ErrSessionDestroyed = errors.New("neutronauth: session destroyed")

// Save persists the session.
//
// Saving a destroyed session fails rather than recreating it: a logout handler
// that touches the session afterwards — or any middleware further down the
// chain that does — would otherwise write the record straight back and undo
// the logout.
func (s *Session) Save(ctx context.Context) error {
	if s.destroyed {
		return ErrSessionDestroyed
	}
	return s.store.Set(ctx, s.ID, s.Data, s.ttl)
}

// Destroy removes the session and expires the browser cookie.
//
// The session is marked destroyed for the rest of the request: a later Save
// cannot resurrect it, and the record the request arrived with is deleted at
// finalization even if the ID was rotated first.
func (s *Session) Destroy(ctx context.Context) error {
	s.destroyed = true
	s.Data = make(map[string]any)
	return s.store.Delete(ctx, s.ID)
}

// Regenerate creates a new session ID, preserving data. Call after
// authentication to prevent session fixation attacks.
//
// The rotation completes when the response headers are committed: the data
// moves to the new ID, the record the request arrived with is deleted, and the
// cookie carries the new ID. Calling Save afterwards is optional.
//
// This used to change only the in-memory ID. The cookie had already been sent
// before the handler ran, so the browser kept presenting the pre-authentication
// ID — whose record was never deleted — and the authenticated data was written
// under an ID nothing would ever ask for. That defeated the fixation defense
// this method exists for AND lost the login.
func (s *Session) Regenerate() {
	s.ID = generateSessionID()
}

// SessionFromContext extracts the session from the request context.
func SessionFromContext(ctx context.Context) *Session {
	s, _ := ctx.Value(ctxKeySession).(*Session)
	return s
}

// SessionMiddleware returns middleware that loads/creates sessions.
func SessionMiddleware(store SessionStore, opts ...SessionOption) neutron.Middleware {
	o := sessionOpts{
		cookieName: "session_id",
		ttl:        24 * time.Hour,
		path:       "/",
		httpOnly:   true,
		secure:     true,
		sameSite:   http.SameSiteLaxMode,
	}
	for _, fn := range opts {
		fn(&o)
	}

	if o.onError == nil {
		o.onError = func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "session store unavailable", http.StatusInternalServerError)
		}
	}
	if o.onCommitError == nil {
		// Silent by default: the response is already committed, so there is
		// nothing safe to do in-band. Applications that care should set one.
		o.onCommitError = func(*http.Request, error) {}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var sessionID string
			cookie, err := r.Cookie(o.cookieName)
			if err == nil {
				sessionID = cookie.Value
			}

			var data map[string]any
			if sessionID != "" {
				// A store outage is not a logged-out user. Reading the error
				// as "no session" minted a fresh anonymous session instead,
				// so a backend blip looked like a mass logout to users and
				// like nothing at all to operators.
				loaded, err := store.Get(r.Context(), sessionID)
				if err != nil {
					o.onError(w, r, err)
					return
				}
				data = loaded
			}
			if data == nil {
				sessionID = generateSessionID()
				data = make(map[string]any)
			}

			sess := &Session{
				ID:         sessionID,
				Data:       data,
				store:      store,
				ttl:        o.ttl,
				originalID: sessionID,
			}

			// The cookie is written when the response headers are committed,
			// so it reflects what the handler did to the session. Writing it
			// here, before `next`, is what made Regenerate and Destroy unable
			// to change what the browser holds.
			sw := &sessionWriter{ResponseWriter: w, finalize: func() {
				// WithoutCancel: a client that disconnects mid-response must
				// not abort the store writes. Otherwise a dropped connection
				// during login leaves the previous session ID undeleted and
				// still valid — the fixation window this rotation exists to
				// close.
				finalizeSession(context.WithoutCancel(r.Context()), r, w, sess, &o)
			}}
			ctx := context.WithValue(r.Context(), ctxKeySession, sess)
			// Deferred: a panicking handler still has to rotate or destroy its
			// session. Recovery middleware sits outside this one, so without
			// the defer the panic unwinds past commit and the old session
			// stays valid.
			defer sw.commit()
			next.ServeHTTP(sw, r.WithContext(ctx))
		})
	}
}

// finalizeSession reconciles storage and the cookie with what the handler did.
//
// It runs at most once per request, at the moment the headers are committed.
// Storage errors here cannot be returned to the caller — the status is already
// decided — so they are reported through the error handler for observability
// and the cookie is still written.
func finalizeSession(ctx context.Context, r *http.Request, w http.ResponseWriter, s *Session, o *sessionOpts) {
	switch {
	case s.destroyed:
		// Delete both ends: Destroy removed the current ID, but a handler that
		// regenerated first would otherwise leave the original behind.
		if s.originalID != "" && s.originalID != s.ID {
			if err := s.store.Delete(ctx, s.originalID); err != nil {
				o.onCommitError(r, err)
			}
		}
		http.SetCookie(w, sessionCookieFor(o, "", -1))
		return

	case s.ID != s.originalID:
		// Rotation completes here so Regenerate alone is sufficient: the data
		// moves to the new ID and the old record stops resolving.
		if err := s.store.Set(ctx, s.ID, s.Data, s.ttl); err != nil {
			o.onCommitError(r, err)
		}
		if s.originalID != "" {
			if err := s.store.Delete(ctx, s.originalID); err != nil {
				o.onCommitError(r, err)
			}
		}
	}

	http.SetCookie(w, sessionCookieFor(o, s.ID, int(o.ttl.Seconds())))
}

func sessionCookieFor(o *sessionOpts, value string, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     o.cookieName,
		Value:    value,
		Path:     o.path,
		MaxAge:   maxAge,
		HttpOnly: o.httpOnly,
		Secure:   o.secure,
		SameSite: o.sameSite,
	}
}

// sessionWriter defers the session cookie to the moment the response headers
// are committed.
//
// It forwards the optional ResponseWriter interfaces rather than hiding them:
// a wrapper that drops Flush breaks SSE, and one that drops Hijack breaks
// WebSocket upgrades, both silently.
type sessionWriter struct {
	http.ResponseWriter
	finalize  func()
	committed bool
}

func (w *sessionWriter) commit() {
	if w.committed {
		return
	}
	w.committed = true
	w.finalize()
}

func (w *sessionWriter) WriteHeader(code int) {
	w.commit()
	w.ResponseWriter.WriteHeader(code)
}

func (w *sessionWriter) Write(b []byte) (int, error) {
	w.commit()
	return w.ResponseWriter.Write(b)
}

// Unwrap exposes the underlying writer to http.ResponseController and to
// interface probes that walk Unwrap chains.
func (w *sessionWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *sessionWriter) Flush() {
	w.commit()
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *sessionWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.commit()
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("neutronauth: underlying ResponseWriter does not support Hijack")
}

// ReadFrom keeps the sendfile fast path available to the underlying writer.
func (w *sessionWriter) ReadFrom(src io.Reader) (int64, error) {
	w.commit()
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}
	return io.Copy(w.ResponseWriter, src)
}

type SessionOption func(*sessionOpts)

type sessionOpts struct {
	cookieName string
	ttl        time.Duration
	path       string
	httpOnly   bool
	secure     bool
	sameSite   http.SameSite
	onError    func(http.ResponseWriter, *http.Request, error)
	// Separate from onError on purpose — see WithSessionCommitErrorHandler.
	onCommitError func(*http.Request, error)
}

func WithCookieName(name string) SessionOption {
	return func(o *sessionOpts) { o.cookieName = name }
}

func WithSessionTTL(d time.Duration) SessionOption {
	return func(o *sessionOpts) { o.ttl = d }
}

func WithSecure(s bool) SessionOption {
	return func(o *sessionOpts) { o.secure = s }
}

// WithSessionErrorHandler sets the handler invoked when the session store fails
// while LOADING, before the downstream handler runs. It owns the response; the
// downstream handler does not run.
func WithSessionErrorHandler(fn func(http.ResponseWriter, *http.Request, error)) SessionOption {
	return func(o *sessionOpts) { o.onError = fn }
}

// WithSessionCommitErrorHandler sets the handler invoked when the session store
// fails while COMMITTING, after the handler has run.
//
// It deliberately receives no ResponseWriter. By that point the status is
// already decided and the cookie has yet to be written, so anything that writes
// to the response corrupts it — the default load-time handler calls
// `http.Error`, which would commit a 500 and body and then silently drop the
// rotated session cookie. Withholding the writer makes that mistake
// unexpressible rather than merely documented.
//
// Use it for logging and alerting. A commit failure is not recoverable in-band,
// but it is worth knowing about: a failed delete of the previous session ID
// leaves that ID valid until it expires.
func WithSessionCommitErrorHandler(fn func(*http.Request, error)) SessionOption {
	return func(o *sessionOpts) { o.onCommitError = fn }
}

// NucleusSessionStore implements SessionStore using Nucleus KV.
type NucleusSessionStore struct {
	kv *nucleus.KVModel
}

// NewNucleusSessionStore creates a session store backed by Nucleus KV.
func NewNucleusSessionStore(kv *nucleus.KVModel) *NucleusSessionStore {
	return &NucleusSessionStore{kv: kv}
}

func (s *NucleusSessionStore) Get(ctx context.Context, id string) (map[string]any, error) {
	data, err := s.kv.Get(ctx, "session:"+id)
	if err != nil || data == nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *NucleusSessionStore) Set(ctx context.Context, id string, data map[string]any, ttl time.Duration) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return s.kv.Set(ctx, "session:"+id, jsonData, nucleus.WithTTL(ttl))
}

func (s *NucleusSessionStore) Delete(ctx context.Context, id string) error {
	_, err := s.kv.Delete(ctx, "session:"+id)
	return err
}

// generateSessionID returns 32 bytes of cryptographic randomness, hex encoded.
//
// The discarded error is deliberate and not a swallowed failure: since Go 1.24
// crypto/rand.Read "never returns an error, and always fills b entirely",
// crashing the program instead if the system source fails. go.mod requires
// 1.24, so there is no build of this package where the error can be non-nil
// and no path that returns a zeroed ID. Making this return (string, error)
// would add a permanently nil error to the public API.
func generateSessionID() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
