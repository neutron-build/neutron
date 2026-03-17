package neutronauth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
type Session struct {
	ID   string
	Data map[string]any
	store SessionStore
	ttl   time.Duration
}

// Get returns a session value.
func (s *Session) Get(key string) any {
	return s.Data[key]
}

// Set stores a session value.
func (s *Session) Set(key string, value any) {
	s.Data[key] = value
}

// Save persists the session.
func (s *Session) Save(ctx context.Context) error {
	return s.store.Set(ctx, s.ID, s.Data, s.ttl)
}

// Destroy removes the session.
func (s *Session) Destroy(ctx context.Context) error {
	return s.store.Delete(ctx, s.ID)
}

// Regenerate creates a new session ID, preserving data. Call after
// authentication to prevent session fixation attacks.
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

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var sessionID string
			cookie, err := r.Cookie(o.cookieName)
			if err == nil {
				sessionID = cookie.Value
			}

			var data map[string]any
			if sessionID != "" {
				data, _ = store.Get(r.Context(), sessionID)
			}
			if data == nil {
				sessionID = generateSessionID()
				data = make(map[string]any)
			}

			sess := &Session{
				ID:    sessionID,
				Data:  data,
				store: store,
				ttl:   o.ttl,
			}

			// Set cookie
			http.SetCookie(w, &http.Cookie{
				Name:     o.cookieName,
				Value:    sessionID,
				Path:     o.path,
				MaxAge:   int(o.ttl.Seconds()),
				HttpOnly: o.httpOnly,
				Secure:   o.secure,
				SameSite: o.sameSite,
			})

			ctx := context.WithValue(r.Context(), ctxKeySession, sess)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

type SessionOption func(*sessionOpts)

type sessionOpts struct {
	cookieName string
	ttl        time.Duration
	path       string
	httpOnly   bool
	secure     bool
	sameSite   http.SameSite
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

func generateSessionID() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
