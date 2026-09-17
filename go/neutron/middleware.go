package neutron

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RateLimitConfig configures the rate-limit layer of DefaultStack.
type RateLimitConfig struct {
	RPS   float64
	Burst int
}

// DefaultStackConfig configures the standard middleware stack. The order is
// fixed (see DefaultStack); these fields only toggle/configure layers. Nil/zero
// optional fields skip that layer.
type DefaultStackConfig struct {
	Logger    *slog.Logger     // nil → slog.Default()
	CORS      *CORSOptions     // nil → no CORS layer
	Compress  bool             // true → gzip at default level
	RateLimit *RateLimitConfig // nil → no rate limit
	Auth      Middleware       // nil → no auth layer (app-specific)
	Timeout   time.Duration    // 0 → no timeout layer
	OTel      *OTelOptions     // nil → no OpenTelemetry layer
}

// DefaultStack returns the standard middleware in the exact order mandated by
// FRAMEWORK_CONTRACT.md:
//
//	RequestID → Logging → Recovery → CORS → Compression → RateLimit → Auth → Timeout → OpenTelemetry
//
// The order is hard-coded and cannot be reordered — callers configure layers,
// they do not arrange them. RequestID strictly precedes Logging so every log
// line carries the request id. Use it as:
//
//	app := neutron.New(neutron.WithMiddleware(neutron.DefaultStack(cfg)...))
func DefaultStack(cfg DefaultStackConfig) []Middleware {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Always-on first three, in contract order.
	mw := []Middleware{
		RequestID(),
		Logger(logger),
		Recover(),
	}
	if cfg.CORS != nil {
		mw = append(mw, CORS(*cfg.CORS))
	}
	if cfg.Compress {
		mw = append(mw, Compress(gzip.DefaultCompression))
	}
	if cfg.RateLimit != nil {
		mw = append(mw, RateLimit(cfg.RateLimit.RPS, cfg.RateLimit.Burst))
	}
	if cfg.Auth != nil {
		mw = append(mw, cfg.Auth)
	}
	if cfg.Timeout > 0 {
		mw = append(mw, Timeout(cfg.Timeout))
	}
	if cfg.OTel != nil {
		mw = append(mw, OTel(*cfg.OTel))
	}
	return mw
}

// Middleware is the standard Go middleware signature.
type Middleware = func(next http.Handler) http.Handler

// Chain composes middleware in order: first middleware is outermost.
func Chain(mw ...Middleware) Middleware {
	return func(next http.Handler) http.Handler {
		for i := len(mw) - 1; i >= 0; i-- {
			next = mw[i](next)
		}
		return next
	}
}

// Logger returns middleware that logs each request using slog.
func Logger(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r)
			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", sw.status,
				"duration", time.Since(start).String(),
				"request_id", RequestIDFromContext(r.Context()),
			)
		})
	}
}

// Recover returns middleware that catches panics and returns a 500 error.
// The panic details are logged server-side but NOT exposed to the client.
func Recover() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Printf("[neutron] panic recovered: %v\n%s", rec, debug.Stack())
					err := ErrInternal("An unexpected error occurred")
					WriteError(w, r, err)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RequestID returns middleware that generates a unique request ID and
// stores it in the context and X-Request-Id header.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get("X-Request-Id")
			if id == "" {
				id = generateID()
			}
			ctx := withRequestID(r.Context(), id)
			w.Header().Set("X-Request-Id", id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// CORSOptions configures CORS behavior.
type CORSOptions struct {
	AllowOrigins     []string
	AllowMethods     []string
	AllowHeaders     []string
	ExposeHeaders    []string
	AllowCredentials bool
	MaxAge           int
}

// validateCORSOptions rejects configurations whose runtime behavior would
// contradict their warning (GO-06): `AllowCredentials: true` with a wildcard
// origin used to log a restriction and then admit every origin WITH
// credentials. Origin entries must be scheme://host[:port] forms.
func validateCORSOptions(opts *CORSOptions) error {
	if opts.AllowCredentials {
		for _, origin := range opts.AllowOrigins {
			if origin == "*" {
				return errors.New(
					"neutron: credentialed CORS requires explicit allowed origins " +
						"(AllowCredentials cannot be combined with \"*\")",
				)
			}
		}
	}
	for _, origin := range opts.AllowOrigins {
		if origin == "*" {
			continue
		}
		u, err := url.Parse(origin)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") ||
			u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
			(u.Path != "" && u.Path != "/") {
			return fmt.Errorf("neutron: invalid CORS origin %q (want scheme://host[:port])", origin)
		}
	}
	return nil
}

// CORS returns middleware that handles Cross-Origin Resource Sharing.
//
// Invalid security configuration panics at construction (GO-06) — the stack
// is assembled at startup, so this is the earliest, loudest failure point.
func CORS(opts CORSOptions) Middleware {
	if err := validateCORSOptions(&opts); err != nil {
		panic(err)
	}
	if len(opts.AllowMethods) == 0 {
		opts.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
	}
	if len(opts.AllowHeaders) == 0 {
		opts.AllowHeaders = []string{"Content-Type", "Authorization", "X-Request-Id"}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Origin-dependent responses declare their variation on EVERY
			// path (GO-07) — allowed, disallowed, and no-origin alike — or a
			// shared cache can reuse one origin's header state for another.
			appendVary(w.Header(), "Origin")
			origin := r.Header.Get("Origin")
			if origin != "" && originAllowed(origin, opts.AllowOrigins) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", strings.Join(opts.AllowMethods, ", "))
				w.Header().Set("Access-Control-Allow-Headers", strings.Join(opts.AllowHeaders, ", "))
				if len(opts.ExposeHeaders) > 0 {
					w.Header().Set("Access-Control-Expose-Headers", strings.Join(opts.ExposeHeaders, ", "))
				}
				if opts.AllowCredentials {
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}
				if opts.MaxAge > 0 {
					w.Header().Set("Access-Control-Max-Age", fmt.Sprintf("%d", opts.MaxAge))
				}
			}
			// Intercept only genuine CORS preflights (GO-07): an OPTIONS
			// request without Origin AND Access-Control-Request-Method is an
			// ordinary application route and must reach its handler.
			isPreflight := r.Method == http.MethodOptions &&
				origin != "" &&
				r.Header.Get("Access-Control-Request-Method") != ""
			if isPreflight {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func originAllowed(origin string, allowed []string) bool {
	if len(allowed) == 0 {
		return false // fail-closed: no origins configured means no origins allowed
	}
	for _, a := range allowed {
		if a == "*" || a == origin {
			return true
		}
	}
	return false
}

// appendVary merges a Vary token case-insensitively, preserving `*`.
func appendVary(h http.Header, token string) {
	for _, line := range h.Values("Vary") {
		for _, existing := range strings.Split(line, ",") {
			existing = strings.TrimSpace(existing)
			if existing == "*" || strings.EqualFold(existing, token) {
				return
			}
		}
	}
	h.Add("Vary", token)
}

// tokenBucket holds per-IP token bucket state.
type tokenBucket struct {
	tokens   float64
	lastTime time.Time
}

// RateLimit returns middleware implementing a per-IP token-bucket rate limiter.
//
// Configuration is validated at construction (GO-24): non-finite or
// non-positive rates and non-positive bursts panic at stack-assembly time
// rather than producing a limiter that never limits or divides by garbage.
func RateLimit(rps float64, burst int) Middleware {
	if math.IsNaN(rps) || math.IsInf(rps, 0) || rps <= 0 {
		panic("neutron: rate must be finite and positive")
	}
	if burst < 1 {
		panic("neutron: burst must be positive")
	}
	var mu sync.Mutex
	buckets := make(map[string]*tokenBucket)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Standard host/port split (GO-24): the manual LastIndex(":")
			// mangled IPv6 literal addresses.
			ip := r.RemoteAddr
			if host, _, err := net.SplitHostPort(ip); err == nil {
				ip = host
			}

			mu.Lock()
			b, ok := buckets[ip]
			if !ok {
				b = &tokenBucket{tokens: float64(burst), lastTime: time.Now()}
				buckets[ip] = b
				// Evict stale entries to prevent unbounded growth
				if len(buckets) > 100000 {
					for k, v := range buckets {
						if time.Since(v.lastTime) > 2*time.Minute {
							delete(buckets, k)
						}
					}
				}
			}

			now := time.Now()
			elapsed := now.Sub(b.lastTime).Seconds()
			b.lastTime = now
			b.tokens += elapsed * rps
			if b.tokens > float64(burst) {
				b.tokens = float64(burst)
			}
			if b.tokens < 1 {
				mu.Unlock()
				WriteError(w, r, ErrRateLimited("Too many requests"))
				return
			}
			b.tokens--
			mu.Unlock()
			next.ServeHTTP(w, r)
		})
	}
}

// Timeout returns middleware that applies a request-scoped deadline.
//
// Cooperative by contract (GO-24): it cancels the request context and relies
// on the handler honoring it. It does not terminate a non-cooperative
// handler or prevent response writes after the deadline — bound the server's
// ReadTimeout/WriteTimeout and use http.ResponseController for anything more.
func Timeout(d time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// parseQuality parses a strict HTTP q-value: `0`, `1`, or `0.x`/`1.xx` with
// up to three decimals. Anything else is malformed and treated as q=0.
func parseQuality(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "0" {
		return 0, true
	}
	if raw == "1" {
		return 1, true
	}
	if len(raw) < 2 || len(raw) > 5 || raw[1] != '.' || (raw[0] != '0' && raw[0] != '1') {
		return 0, false
	}
	for _, ch := range raw[2:] {
		if ch < '0' || ch > '9' || (raw[0] == '1' && ch != '0') {
			return 0, false
		}
	}
	q, err := strconv.ParseFloat(raw, 64)
	return q, err == nil
}

// encodingQuality reports the client's quality for a content coding.
// An explicit listing wins over the wildcard; an explicit q=0 is never
// overridden by `*`. Missing advertisement conservatively means 0 (do not
// transform to that coding).
func encodingQuality(header, coding string) float64 {
	explicit, wildcard := -1.0, -1.0
	for _, item := range strings.Split(header, ",") {
		fields := strings.Split(item, ";")
		name := strings.TrimSpace(fields[0])
		if !strings.EqualFold(name, coding) && name != "*" {
			continue
		}
		quality, seenQ := 1.0, false
		for _, parameter := range fields[1:] {
			key, value, ok := strings.Cut(strings.TrimSpace(parameter), "=")
			if !strings.EqualFold(strings.TrimSpace(key), "q") {
				continue
			}
			if !ok || seenQ {
				quality = 0
				seenQ = true
				continue
			}
			seenQ = true
			var valid bool
			quality, valid = parseQuality(value)
			if !valid {
				quality = 0
			}
		}
		target := &explicit
		if name == "*" {
			target = &wildcard
		}
		// Duplicate advertisements use their most restrictive value.
		if *target < 0 || quality < *target {
			*target = quality
		}
	}
	if explicit >= 0 {
		return explicit
	}
	if wildcard >= 0 {
		return wildcard
	}
	return 0
}

// gzipWriter wraps http.ResponseWriter with a gzip writer.
//
// Header commitment is LAZY (GO-09): Content-Encoding is set and
// Content-Length removed only at the moment the response actually commits
// (first WriteHeader/Write), after eligibility is decidable from the final
// headers. The old wrapper set both before the handler ran, so a handler
// that set Content-Length afterwards produced compressed bytes with the
// uncompressed length on the wire, and a panic before the first byte still
// carried the gzip header into the recovery response.
type gzipWriter struct {
	http.ResponseWriter
	Writer io.Writer
	// decided marks that compression headers were emitted; once true the
	// response is committed as gzip and no plain-text recovery body may be
	// appended.
	decided  bool
	skipGzip bool
}

func (w *gzipWriter) commitHeaders(code int) {
	if w.decided {
		return
	}
	w.decided = true
	h := w.ResponseWriter.Header()
	// Response-side eligibility, judged on the FINAL headers at commitment
	// (GO-09): bodyless statuses, existing encodings, ranges, and
	// no-transform responses pass through uncompressed.
	if h.Get("Content-Encoding") != "" ||
		h.Get("Content-Range") != "" ||
		code == http.StatusNoContent ||
		code == http.StatusResetContent ||
		code == http.StatusNotModified {
		w.skipGzip = true
		return
	}
	for _, directive := range strings.Split(h.Get("Cache-Control"), ",") {
		key, _, _ := strings.Cut(strings.TrimSpace(directive), "=")
		if strings.EqualFold(key, "no-transform") {
			w.skipGzip = true
			return
		}
	}
	h.Set("Content-Encoding", "gzip")
	h.Del("Content-Length")
}

func (w *gzipWriter) WriteHeader(code int) {
	w.commitHeaders(code)
	w.ResponseWriter.WriteHeader(code)
}

func (w *gzipWriter) Write(b []byte) (int, error) {
	// Implicit 200 commitment shares the same lazy decision.
	w.commitHeaders(http.StatusOK)
	if w.skipGzip {
		return w.ResponseWriter.Write(b)
	}
	return w.Writer.Write(b)
}

func (w *gzipWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Flush flushes the gzip writer (to push buffered compressed bytes) and then
// the underlying writer, so SSE works through compression.
func (w *gzipWriter) Flush() {
	if f, ok := w.Writer.(interface{ Flush() error }); ok {
		_ = f.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the underlying writer (the hijacked connection bypasses
// gzip, which is correct for WebSocket upgrades).
func (w *gzipWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("neutron: underlying ResponseWriter does not support Hijack")
}

// requestWantsGzip is the request-side half of compression eligibility,
// decidable before the handler runs: the client must accept gzip with a
// non-zero quality factor. HEAD requests and protocol upgrades are excluded.
func requestWantsGzip(r *http.Request) bool {
	if r.Method == http.MethodHead || r.Header.Get("Upgrade") != "" || r.Header.Get("Range") != "" {
		return false
	}
	return encodingQuality(r.Header.Get("Accept-Encoding"), "gzip") > 0
}

// Compress returns middleware that gzip-compresses responses.
// Level should be gzip.DefaultCompression or a value from 1-9.
//
// The negotiation is quality-aware (GO-08): `gzip;q=0` (or a wildcard `*`
// with an explicit gzip;q=0) never selects gzip — the old substring test
// happily compressed for clients that had forbidden the coding.
func Compress(level int) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The response varies on Accept-Encoding whether or not we compress,
			// so caches must key on it (RFC 9110 §12.5.5). Set on both paths.
			appendVary(w.Header(), "Accept-Encoding")
			if !requestWantsGzip(r) {
				next.ServeHTTP(w, r)
				return
			}
			gz, err := gzip.NewWriterLevel(w, level)
			if err != nil {
				next.ServeHTTP(w, r)
				return
			}
			gw := &gzipWriter{ResponseWriter: w, Writer: gz}
			next.ServeHTTP(gw, r)
			// Close only on normal completion: a panic unwinds past this
			// point, and writing a gzip trailer into a stream the recovery
			// middleware is about to append plain text to would only deepen
			// the corruption.
			_ = gz.Close()
		})
	}
}

// OTelOptions configures the observability middleware.
type OTelOptions struct {
	ServiceName string
}

// OTel returns middleware that adds trace context (trace ID in context and
// response headers). For full OpenTelemetry integration, use the OTel SDK
// and bring your own middleware.
func OTel(opts OTelOptions) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			traceID := r.Header.Get("X-Trace-Id")
			if traceID == "" {
				traceID = generateID()
			}
			ctx := withTraceID(r.Context(), traceID)
			w.Header().Set("X-Trace-Id", traceID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// statusWriter wraps http.ResponseWriter to capture the status code.
//
// It records the FIRST final status (GO-10): net/http ignores repeated
// WriteHeader calls, but the old wrapper overwrote its record on every call,
// so logs showed a later 500 for a response that had actually gone out as a
// 200. Informational 1xx responses are forwarded without finalizing, and a
// body Write implies 200.
type statusWriter struct {
	http.ResponseWriter
	status    int
	committed bool
}

func (w *statusWriter) WriteHeader(code int) {
	// Informational responses do not commit the final response (101 is a
	// protocol switch, handled by Hijack).
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.committed {
		return
	}
	w.committed = true
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if !w.committed {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

// Unwrap exposes the underlying writer to http.ResponseController and to
// interface probes that walk Unwrap chains.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Flush forwards to the underlying writer so SSE / streaming responses are not
// silently buffered when this middleware is in the chain. Flushing before any
// Write establishes the implicit 200 first, so the recorded status matches
// what actually went out.
func (w *statusWriter) Flush() {
	if !w.committed {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the underlying writer so WebSocket upgrades work behind
// this middleware. Embedding the ResponseWriter interface does not promote
// Hijack (it is not part of http.ResponseWriter), so it must be forwarded.
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if !w.committed {
		w.WriteHeader(http.StatusSwitchingProtocols)
	}
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("neutron: underlying ResponseWriter does not support Hijack")
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
