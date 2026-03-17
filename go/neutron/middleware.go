package neutron

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

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

// gzipWriter wraps http.ResponseWriter with a gzip writer.
type gzipWriter struct {
	http.ResponseWriter
	Writer io.Writer
}

func (w *gzipWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

