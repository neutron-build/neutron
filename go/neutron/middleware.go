package neutron

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
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

// CORS returns middleware that handles Cross-Origin Resource Sharing.
func CORS(opts CORSOptions) Middleware {
	if len(opts.AllowMethods) == 0 {
		opts.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
	}
	if len(opts.AllowHeaders) == 0 {
		opts.AllowHeaders = []string{"Content-Type", "Authorization", "X-Request-Id"}
	}
	if opts.AllowCredentials {
		for _, o := range opts.AllowOrigins {
			if o == "*" {
				log.Println("[neutron] WARNING: CORS wildcard '*' with credentials is dangerous. Restricting to request origin matching.")
				break
			}
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			if r.Method == http.MethodOptions {
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

// tokenBucket holds per-IP token bucket state.
type tokenBucket struct {
	tokens   float64
	lastTime time.Time
}

// RateLimit returns middleware implementing a per-IP token-bucket rate limiter.
func RateLimit(rps float64, burst int) Middleware {
	var mu sync.Mutex
	buckets := make(map[string]*tokenBucket)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.RemoteAddr
			if idx := strings.LastIndex(ip, ":"); idx != -1 {
				ip = ip[:idx]
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

// Timeout returns middleware that applies a request timeout.
func Timeout(d time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// Compress returns middleware that gzip-compresses responses.
// Level should be gzip.DefaultCompression or a value from 1-9.
func Compress(level int) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The response varies on Accept-Encoding whether or not we compress,
			// so caches must key on it (RFC 7231 §7.1.4). Set on both paths.
			w.Header().Add("Vary", "Accept-Encoding")
			if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				next.ServeHTTP(w, r)
				return
			}
			gz, err := gzip.NewWriterLevel(w, level)
			if err != nil {
				next.ServeHTTP(w, r)
				return
			}
			defer gz.Close()
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Del("Content-Length")
			next.ServeHTTP(&gzipWriter{ResponseWriter: w, Writer: gz}, r)
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
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Unwrap exposes the underlying writer to http.ResponseController and to
// interface probes that walk Unwrap chains.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Flush forwards to the underlying writer so SSE / streaming responses are not
// silently buffered when this middleware is in the chain.
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the underlying writer so WebSocket upgrades work behind
// this middleware. Embedding the ResponseWriter interface does not promote
// Hijack (it is not part of http.ResponseWriter), so it must be forwarded.
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("neutron: underlying ResponseWriter does not support Hijack")
}

// gzipWriter wraps http.ResponseWriter with a gzip writer.
type gzipWriter struct {
	http.ResponseWriter
	Writer io.Writer
}

func (w *gzipWriter) Write(b []byte) (int, error) {
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

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

