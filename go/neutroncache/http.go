package neutroncache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/neutron-dev/neutron-go/neutron"
)

// HTTPCacheOption configures HTTPCache.
type HTTPCacheOption func(*httpCacheOpts)

type httpCacheOpts struct {
	publicPrefixes []string
	varyHeaders    []string
	cacheable      func(*http.Request) bool
}

// WithPublicRoutes marks path prefixes whose response does not depend on who
// is asking, so they stay cacheable even when the request carries a session
// cookie.
//
// This is the knob that makes the cache usable at all in a real application.
// Any app with a login sends a session cookie on *every* request, including
// ones for entirely public pages, so without an opt-in the safe default turns
// the cache off everywhere. The opt-in is per-route rather than global because
// "this response is the same for everyone" is a property of the route, and
// only the person who wrote the route knows it.
func WithPublicRoutes(prefixes ...string) HTTPCacheOption {
	return func(o *httpCacheOpts) { o.publicPrefixes = append(o.publicPrefixes, prefixes...) }
}

// WithVaryHeaders includes the named request headers in the cache key, for
// routes that legitimately serve different bytes per header (Accept-Encoding,
// Accept-Language).
func WithVaryHeaders(names ...string) HTTPCacheOption {
	return func(o *httpCacheOpts) { o.varyHeaders = append(o.varyHeaders, names...) }
}

// WithCacheableRequest supplies a predicate deciding whether a request may be
// served from cache, for cases prefixes cannot express. It runs after the
// method check and replaces the credential check, so a predicate returning
// true for an authenticated request opts into sharing that response between
// callers.
func WithCacheableRequest(fn func(*http.Request) bool) HTTPCacheOption {
	return func(o *httpCacheOpts) { o.cacheable = fn }
}

// HTTPCache returns middleware that caches full HTTP responses in the tiered
// cache. Only GET requests answered with 200 are cached.
//
// A request carrying credentials — `Authorization` or any `Cookie` — is not
// cached unless its route is marked public with [WithPublicRoutes].
//
// The cookie half of that is the point. This previously checked only
// `Authorization`, while keying the entry on the URL alone, so an application
// authenticating with a session cookie (which is the norm, and the default in
// this stack) had every personalised GET response stored under a key shared by
// every visitor — and served to them. The failure is silent, only shows up
// under concurrent users, and leaks whatever the page contained.
func HTTPCache(c *TieredCache, ttl time.Duration, opts ...HTTPCacheOption) neutron.Middleware {
	var o httpCacheOpts
	for _, fn := range opts {
		fn(&o)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				next.ServeHTTP(w, r)
				return
			}

			if !requestIsCacheable(r, &o) {
				next.ServeHTTP(w, r)
				return
			}

			cacheKey := "httpcache:" + hashKey(cacheKeyFor(r, o.varyHeaders))

			if data, ok := c.l1.Get(cacheKey); ok {
				if entry, err := decodeEntry(data); err == nil {
					// Replay the response as it was produced. Forcing a
					// Content-Type here — this used to always claim
					// application/json — silently rewrites an HTML or image
					// response into the wrong type on every hit, so the route
					// works until the moment it starts being cached.
					for k, vs := range entry.Header {
						for _, v := range vs {
							w.Header().Add(k, v)
						}
					}
					w.Header().Set("X-Cache", "HIT")
					w.WriteHeader(entry.Status)
					_, _ = w.Write(entry.Body)
					return
				}
				// An entry we cannot decode is treated as a miss rather than
				// served as garbage.
			}

			rec := &responseRecorder{
				ResponseWriter: w,
				body:           &bytes.Buffer{},
				status:         http.StatusOK,
			}
			next.ServeHTTP(rec, r)

			if rec.status == http.StatusOK && responseIsCacheable(rec) {
				entry := cacheEntry{
					Status: rec.status,
					Header: cacheableHeaders(rec.Header()),
					Body:   rec.body.Bytes(),
				}
				if encoded, err := json.Marshal(entry); err == nil {
					c.l1.Set(cacheKey, encoded, ttl)
				}
			}
		})
	}
}

// requestIsCacheable reports whether this request may be served from a shared
// cache.
func requestIsCacheable(r *http.Request, o *httpCacheOpts) bool {
	if o.cacheable != nil {
		return o.cacheable(r)
	}
	if isPublicPath(r.URL.Path, o.publicPrefixes) {
		return true
	}
	// Anything that identifies a caller makes the response potentially
	// caller-specific, and this cache is shared by every visitor.
	if r.Header.Get("Authorization") != "" {
		return false
	}
	if r.Header.Get("Cookie") != "" {
		return false
	}
	return true
}

func isPublicPath(path string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

// responseIsCacheable rejects responses that carry per-caller state.
func responseIsCacheable(rec *responseRecorder) bool {
	h := rec.Header()
	// A stored Set-Cookie would be replayed to everyone who hits the entry,
	// handing one visitor's session to the next.
	if len(h.Values("Set-Cookie")) > 0 {
		return false
	}
	cc := strings.ToLower(h.Get("Cache-Control"))
	if strings.Contains(cc, "no-store") || strings.Contains(cc, "private") {
		return false
	}
	// `Vary: Cookie` is the origin saying the body depends on the caller.
	for _, v := range h.Values("Vary") {
		if strings.Contains(strings.ToLower(v), "cookie") ||
			strings.Contains(strings.ToLower(v), "authorization") {
			return false
		}
	}
	return true
}

// cacheableHeaders copies the response headers worth replaying, dropping ones
// that describe a single exchange rather than the body.
func cacheableHeaders(h http.Header) http.Header {
	out := http.Header{}
	for k, vs := range h {
		switch http.CanonicalHeaderKey(k) {
		case "Set-Cookie", "X-Cache", "Date", "Connection", "Transfer-Encoding":
			continue
		}
		out[http.CanonicalHeaderKey(k)] = append([]string(nil), vs...)
	}
	return out
}

// cacheKeyFor builds the key from the URL plus any headers the caller declared
// the response varies on.
func cacheKeyFor(r *http.Request, vary []string) string {
	if len(vary) == 0 {
		return r.URL.String()
	}
	var b strings.Builder
	b.WriteString(r.URL.String())
	for _, name := range vary {
		b.WriteString("\x00")
		b.WriteString(name)
		b.WriteString("=")
		b.WriteString(r.Header.Get(name))
	}
	return b.String()
}

// cacheEntry is the stored form of a cached response. The body alone is not
// enough: replaying it without its status and headers produces a response that
// differs from the one the handler wrote.
type cacheEntry struct {
	Status int         `json:"status"`
	Header http.Header `json:"header"`
	Body   []byte      `json:"body"`
}

func decodeEntry(data []byte) (cacheEntry, error) {
	var e cacheEntry
	err := json.Unmarshal(data, &e)
	return e, err
}

type responseRecorder struct {
	http.ResponseWriter
	body   *bytes.Buffer
	status int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

func hashKey(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:16])
}
