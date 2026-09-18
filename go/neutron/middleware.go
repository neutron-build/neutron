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
//
// http.ErrAbortHandler is re-panicked, not handled (GO-09): it is how the
// compression middleware signals "response already partially on the wire,
// no in-band error is possible". net/http aborts the connection on it —
// the client sees a truncated response (detectable) instead of a
// complete-looking corrupt one.
func Recover() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if rec == http.ErrAbortHandler {
						log.Printf("[neutron] aborting response after mid-stream panic (headers already sent; no in-band error possible)")
						panic(rec)
					}
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

// Hard ceiling on live rate-limit buckets (GO-27). Without it, >100k
// distinct source addresses made EVERY new key sweep the whole map under
// the global mutex (O(n) per request once over the old threshold), and the
// map still grew without bound when keys were all recent. At capacity, new
// identities are refused with 429; existing buckets keep their state.
const rateLimitMaxBuckets = 100_000

// Minimum spacing between expiry sweeps, so a flood of new keys cannot
// trigger a full-map scan per request (GO-27).
const rateLimitSweepInterval = 10 * time.Second

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
	var nextSweep time.Time

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
				// Bounded maintenance (GO-27): the expiry sweep runs at most
				// once per interval — never once per new key — and admission
				// stops at the hard bucket ceiling instead of scanning
				// forever.
				now := time.Now()
				if now.After(nextSweep) {
					for k, v := range buckets {
						if now.Sub(v.lastTime) > 2*time.Minute {
							delete(buckets, k)
						}
					}
					nextSweep = now.Add(rateLimitSweepInterval)
				}
				if len(buckets) >= rateLimitMaxBuckets {
					mu.Unlock()
					w.Header().Set("Retry-After", "10")
					WriteError(w, r, ErrRateLimited("Rate limiter capacity reached"))
					return
				}
				b = &tokenBucket{tokens: float64(burst), lastTime: time.Now()}
				buckets[ip] = b
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

// gzipWriter wraps http.ResponseWriter with a lazily-committed gzip writer.
//
// The encoder is created only when the FINAL headers prove the response
// eligible (GO-09), and finalized only if it was ever selected (GO-25):
// the old writer created the gzip.Writer eagerly and closed it
// unconditionally after the handler, appending an empty gzip member to
// bypassed responses ("abc" → 26 bytes with no Content-Encoding) and
// emitting trailer-only gzip bytes for empty handlers. Flush used to push
// gzip bytes BEFORE the eligibility decision, producing compressed output
// under an unencoded implicit-200; informational WriteHeader calls
// finalized compression state mid-handshake. A Write with no Content-Type
// now sniffs the ORIGINAL bytes before compression, so the underlying
// server advertises the real media type instead of the gzip magic.
type gzipWriter struct {
	http.ResponseWriter
	level int
	// encoder is non-nil exactly while a gzip member is open.
	encoder *gzip.Writer
	// decided marks that the final-header decision was made; once true no
	// later WriteHeader can change the outcome.
	decided  bool
	skipGzip bool
}

// commitHeaders makes the one-way eligibility decision and (when eligible)
// installs the encoder. `sample` is the first body bytes when the commit is
// triggered by Write, used for Content-Type sniffing.
func (w *gzipWriter) commitHeaders(code int, sample []byte) {
	if w.decided {
		return
	}
	w.decided = true
	h := w.ResponseWriter.Header()
	// Response-side eligibility, judged on the FINAL headers at commitment
	// (GO-09): bodyless statuses, existing encodings, ranges, and
	// no-transform responses pass through uncompressed. Non-final statuses
	// never reach here (see WriteHeader).
	if code < 200 || code == http.StatusNoContent || code == http.StatusResetContent ||
		code == http.StatusNotModified ||
		h.Get("Content-Encoding") != "" || h.Get("Content-Range") != "" {
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
	if h.Get("Content-Type") == "" {
		if len(sample) == 0 {
			// No body bytes to sniff yet (explicit WriteHeader, or a Flush
			// before any Write): compressing would leave the underlying
			// server to sniff the GZIP magic and advertise the wrong type.
			// Stay identity.
			w.skipGzip = true
			return
		}
		h.Set("Content-Type", http.DetectContentType(sample))
	}
	w.encoder, _ = gzip.NewWriterLevel(w.ResponseWriter, w.level)
	h.Set("Content-Encoding", "gzip")
	h.Del("Content-Length")
	// The representation is transformed, so a strong ETag no longer
	// identifies the delivered bytes.
	if etag := h.Get("ETag"); etag != "" && !strings.HasPrefix(etag, "W/") {
		h.Set("ETag", "W/"+etag)
	}
}

func (w *gzipWriter) WriteHeader(code int) {
	// Informational responses are NOT final commitments (GO-25): forward
	// them untouched — finalizing compression state on a 103 left the
	// eventual body appended to a decided-but-unencoded response. 101 is a
	// protocol switch: the connection stops being an HTTP response, so
	// bypass compression for whatever follows.
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if code == http.StatusSwitchingProtocols {
		w.decided = true
		w.skipGzip = true
		w.ResponseWriter.WriteHeader(code)
		return
	}
	w.commitHeaders(code, nil)
	w.ResponseWriter.WriteHeader(code)
}

func (w *gzipWriter) Write(b []byte) (int, error) {
	w.commitHeaders(http.StatusOK, b)
	if w.skipGzip || w.encoder == nil {
		return w.ResponseWriter.Write(b)
	}
	return w.encoder.Write(b)
}

func (w *gzipWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Flush flushes the gzip writer (to push buffered compressed bytes) and then
// the underlying writer, so SSE works through compression. Committing BEFORE
// flushing guarantees the Content-Encoding header rides the same implicit
// 200 as the first compressed bytes (GO-25).
func (w *gzipWriter) Flush() {
	w.commitHeaders(http.StatusOK, nil)
	if w.encoder != nil {
		_ = w.encoder.Flush()
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// finish closes the gzip member iff one was opened. Called only on normal
// handler completion (GO-09): a panic must NOT finalize a partial stream.
func (w *gzipWriter) finish() error {
	if w.encoder == nil {
		return nil
	}
	return w.encoder.Close()
}

// Hijack forwards to the underlying writer (the hijacked connection bypasses
// gzip, which is correct for WebSocket upgrades). No header is emitted.
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
//
// The encoder is fully lazy (GO-25): it is created at final-header
// commitment and closed only if it was ever created, so bypassed
// (no-transform / pre-encoded / ranged / bodyless) and empty responses go
// out byte-for-byte unchanged instead of dragging an empty gzip member.
func Compress(level int) Middleware {
	// Validate the level once, before any request is served.
	if _, err := gzip.NewWriterLevel(io.Discard, level); err != nil {
		panic("neutron: invalid gzip level: " + err.Error())
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The response varies on Accept-Encoding whether or not we compress,
			// so caches must key on it (RFC 9110 §12.5.5). Set on both paths.
			appendVary(w.Header(), "Accept-Encoding")
			if !requestWantsGzip(r) {
				next.ServeHTTP(w, r)
				return
			}
			gw := &gzipWriter{ResponseWriter: w, level: level}
			defer func() {
				if rec := recover(); rec != nil {
					if gw.encoder != nil {
						// A gzip member is open, so the final status and
						// Content-Encoding header are already on the wire:
						// no in-band error is possible. Appending the plain
						// problem+json bytes Recovery would write would only
						// deepen the corruption, and the member stays
						// deliberately unfinalized (no trailer — see finish).
						// Replace the panic with net/http's controlled abort
						// sentinel: the connection is closed mid-response and
						// the client detects truncation instead of decoding a
						// corrupt "complete" body (GO-09 residual).
						panic(http.ErrAbortHandler)
					}
					// Nothing compressed yet — the response is still
					// answerable in-band. Hand the original panic to Recovery.
					panic(rec)
				}
			}()
			next.ServeHTTP(gw, r)
			// Close only on normal completion: a panic unwinds past this
			// point, and writing a gzip trailer into a stream the recovery
			// middleware is about to append plain text to would only deepen
			// the corruption.
			_ = gw.finish()
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
//
// It must NOT emit any response header of its own (GO-26): the old
// `WriteHeader(101)` wrote an unsolicited protocol-switch status BEFORE
// delegating — duplicating/corrupting a WebSocket handshake the caller was
// about to perform, and emitting wire output even when the underlying writer
// does not support Hijack at all. Hijack is connection ownership, not a
// response; the recorded status for logging stays the implicit 200 unless
// the caller wrote one.
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
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
