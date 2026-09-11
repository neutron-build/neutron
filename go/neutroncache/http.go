package neutroncache

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/neutron-build/neutron/go/neutron"
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
//
// This declares an additional key dimension; it is not permission to ignore
// a response's Vary field. A response that varies on a header not named
// here — or on `*` — is not cached at all, because the key cannot represent
// the variation.
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
//
// Freshness contract (audit neutron-19): by default the cache honors the
// freshness signals it can represent. A response marked `no-cache`, or with
// `max-age=0`, is never stored; a numeric `max-age`/`s-maxage` bounds the
// stored TTL below the middleware TTL. A request carrying
// `Cache-Control: no-cache` bypasses the lookup (this cache cannot
// revalidate, so bypass is the only correct interpretation) but still
// refreshes the entry. Responses with no freshness metadata use the
// middleware TTL as before.
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

			// A client asking for revalidation gets the origin: this cache
			// has no conditional-request support, so bypass is the only
			// answer that respects the request's intent.
			_, bypassRead := cacheControlDirectives(r.Header.Get("Cache-Control"))["no-cache"]

			if !bypassRead {
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
			}

			rec := &responseRecorder{
				ResponseWriter: w,
				body:           &bytes.Buffer{},
				status:         http.StatusOK,
			}
			next.ServeHTTP(rec, r)

			// A flushed or hijacked response is a stream; it was never
			// fully captured and must not be replayed.
			if rec.streamed {
				return
			}
			if rec.status == http.StatusOK && responseIsCacheable(rec, o.varyHeaders) {
				entry := cacheEntry{
					Status: rec.status,
					Header: cacheableHeaders(rec.Header()),
					Body:   rec.body.Bytes(),
				}
				if encoded, err := json.Marshal(entry); err == nil {
					c.l1.Set(cacheKey, encoded, responseTTL(ttl, rec.Header()))
				}
			}
		})
	}
}

// responseTTL bounds the middleware TTL by the response's own freshness
// metadata when it is stricter: s-maxage (the shared-cache directive)
// over max-age, over the configured TTL.
func responseTTL(ttl time.Duration, h http.Header) time.Duration {
	cc := cacheControlDirectives(h.Get("Cache-Control"))
	if v, ok := cc["s-maxage"]; ok {
		if d, err := parseSeconds(v); err == nil && d < ttl {
			ttl = d
		}
	}
	if v, ok := cc["max-age"]; ok {
		if d, err := parseSeconds(v); err == nil && d < ttl {
			ttl = d
		}
	}
	return ttl
}

func parseSeconds(v string) (time.Duration, error) {
	seconds, err := strconv.Atoi(strings.Trim(v, `"`))
	if err != nil || seconds < 0 {
		return 0, fmt.Errorf("invalid seconds %q", v)
	}
	return time.Duration(seconds) * time.Second, nil
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

// cacheControlDirectives parses a Cache-Control field into a map of
// lowercase directive names to values ("no-cache" and friends have empty
// values). Multiple field values are comma-separated per RFC 9111; values
// may be quoted.
func cacheControlDirectives(values ...string) map[string]string {
	out := map[string]string{}
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			name, arg, _ := strings.Cut(strings.TrimSpace(part), "=")
			name = strings.ToLower(strings.TrimSpace(name))
			if name == "" {
				continue
			}
			out[name] = strings.Trim(strings.TrimSpace(arg), `"`)
		}
	}
	return out
}

// responseIsCacheable rejects responses that must not be stored or shared:
// per-caller state, explicit freshness refusals, and any Vary dimension the
// cache key cannot represent.
func responseIsCacheable(rec *responseRecorder, varyHeaders []string) bool {
	h := rec.Header()
	// A stored Set-Cookie would be replayed to everyone who hits the entry,
	// handing one visitor's session to the next.
	if len(h.Values("Set-Cookie")) > 0 {
		return false
	}
	cc := cacheControlDirectives(h.Get("Cache-Control"))
	if _, ok := cc["no-store"]; ok {
		return false
	}
	if _, ok := cc["private"]; ok {
		return false
	}
	// no-cache requires revalidation before reuse, and max-age=0 declares
	// zero freshness: neither may become a fresh entry under the
	// middleware TTL (audit neutron-19).
	if _, ok := cc["no-cache"]; ok {
		return false
	}
	if v, ok := cc["max-age"]; ok {
		if d, err := parseSeconds(v); err == nil && d == 0 {
			return false
		}
	}

	// Every Vary dimension the response names must be either rejected
	// outright (per-caller) or represented in the cache key; an unlisted
	// field would let one representation replay to requests it does not
	// match, and `*` can never be represented (audit neutron-18).
	declared := make(map[string]bool, len(varyHeaders))
	for _, name := range varyHeaders {
		declared[strings.ToLower(http.CanonicalHeaderKey(name))] = true
	}
	for _, value := range h.Values("Vary") {
		for _, name := range strings.Split(value, ",") {
			field := strings.ToLower(strings.TrimSpace(name))
			switch {
			case field == "":
				continue
			case field == "*":
				return false
			case field == "cookie", field == "authorization":
				return false
			case !declared[field]:
				return false
			}
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

// cacheKeyFor builds the key from the host, the URL, and any headers the
// caller declared the response varies on. The host participates even though
// r.URL.String() omits it: a cache shared across virtual hosts would
// otherwise store host A's response under the same key as host B's and serve
// it across.
func cacheKeyFor(r *http.Request, vary []string) string {
	var b strings.Builder
	b.WriteString(r.Host)
	b.WriteString("\x00")
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

// responseRecorder wraps the writer for a response being considered for
// caching. Optional writer capabilities are forwarded (Flush, Hijack) and
// Unwrap lets http.ResponseController walk past this wrapper; once a handler
// flushes or hijacks, the response is a stream — capture stops and the
// response is never cached (audit neutron-20).
type responseRecorder struct {
	http.ResponseWriter
	body     *bytes.Buffer
	status   int
	streamed bool
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.streamed {
		r.body.Write(b)
	}
	return r.ResponseWriter.Write(b)
}

// Unwrap exposes the underlying writer so http.ResponseController can reach
// capabilities this wrapper does not forward itself.
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Flush forwards to the underlying writer when it supports flushing. The
// plain http.Flusher signature keeps direct `w.(http.Flusher)` assertions
// working for handlers that predate ResponseController; a no-op on a
// non-flushable underlying writer matches flushing a buffered writer.
func (r *responseRecorder) Flush() {
	r.streamed = true
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the underlying writer. A hijacked connection is owned
// by the handler; capture has stopped and the exchange is never cached.
func (r *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		r.streamed = true
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("neutroncache: underlying ResponseWriter does not support Hijack")
}

func hashKey(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:16])
}
