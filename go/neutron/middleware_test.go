package neutron

import (
	"bytes"
	"compress/gzip"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// P0.1: response-writer wrappers must forward Flush/Hijack so SSE and WebSocket
// upgrades work behind the framework's own Logger/Compress middleware. Embedding
// the http.ResponseWriter interface does NOT promote those methods.
func TestStreamingWorksBehindMiddleware(t *testing.T) {
	var _ http.Flusher = (*statusWriter)(nil)
	var _ http.Hijacker = (*statusWriter)(nil)
	var _ http.Flusher = (*gzipWriter)(nil)
	var _ http.Hijacker = (*gzipWriter)(nil)

	streaming := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			t.Error("http.Flusher not available to handler behind middleware")
			return
		}
		_, _ = w.Write([]byte("data: hi\n\n"))
		f.Flush()
	})

	t.Run("behind Logger", func(t *testing.T) {
		rec := httptest.NewRecorder()
		Logger(slog.Default())(streaming).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
		if !rec.Flushed {
			t.Error("response not flushed behind Logger middleware")
		}
	})

	t.Run("behind Compress", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		Compress(gzip.DefaultCompression)(streaming).ServeHTTP(rec, req)
		if !rec.Flushed {
			t.Error("response not flushed behind Compress middleware")
		}
	})
}

func TestRequestIDMiddleware(t *testing.T) {
	handler := RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := RequestIDFromContext(r.Context())
		if id == "" {
			t.Error("request ID should be set in context")
		}
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	if w.Header().Get("X-Request-Id") == "" {
		t.Error("X-Request-Id header should be set")
	}
}

func TestRequestIDMiddlewarePreserves(t *testing.T) {
	handler := RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := RequestIDFromContext(r.Context())
		if id != "existing-id" {
			t.Errorf("expected existing-id, got %q", id)
		}
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Request-Id", "existing-id")
	handler.ServeHTTP(w, r)

	if w.Header().Get("X-Request-Id") != "existing-id" {
		t.Errorf("X-Request-Id = %q, want existing-id", w.Header().Get("X-Request-Id"))
	}
}

func TestRecoverMiddleware(t *testing.T) {
	handler := Recover()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestLoggerMiddleware(t *testing.T) {
	logger := slog.Default()
	handler := Logger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/test", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestCORSMiddleware(t *testing.T) {
	handler := CORS(CORSOptions{
		AllowOrigins: []string{"http://example.com"},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Regular request with allowed origin
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Origin", "http://example.com")
	handler.ServeHTTP(w, r)

	if w.Header().Get("Access-Control-Allow-Origin") != "http://example.com" {
		t.Errorf("ACAO = %q", w.Header().Get("Access-Control-Allow-Origin"))
	}

	// Preflight
	w = httptest.NewRecorder()
	r = httptest.NewRequest("OPTIONS", "/", nil)
	r.Header.Set("Origin", "http://example.com")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusNoContent {
		t.Errorf("preflight status = %d, want 204", w.Code)
	}

	// Disallowed origin
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Origin", "http://evil.com")
	handler.ServeHTTP(w, r)

	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("should not set ACAO for disallowed origin")
	}
}

func TestTimeoutMiddleware(t *testing.T) {
	handler := Timeout(50 * time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(200 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
		}
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)
	// The context should have timed out
}

func TestChain(t *testing.T) {
	var order []string
	mw1 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "mw1-before")
			next.ServeHTTP(w, r)
			order = append(order, "mw1-after")
		})
	}
	mw2 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "mw2-before")
			next.ServeHTTP(w, r)
			order = append(order, "mw2-after")
		})
	}

	handler := Chain(mw1, mw2)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	expected := []string{"mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"}
	if len(order) != len(expected) {
		t.Fatalf("order = %v, want %v", order, expected)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("order[%d] = %q, want %q", i, order[i], v)
		}
	}
}

func TestOTelMiddleware(t *testing.T) {
	handler := OTel(OTelOptions{ServiceName: "test"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := TraceIDFromContext(r.Context())
		if id == "" {
			t.Error("trace ID should be set")
		}
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)

	if w.Header().Get("X-Trace-Id") == "" {
		t.Error("X-Trace-Id header should be set")
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	handler := RateLimit(1, 1)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First request should pass
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("first request: status = %d, want 200", w.Code)
	}

	// Second request immediately should be rate limited
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", "/", nil)
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("second request: status = %d, want 429", w.Code)
	}
}

// P1: DefaultStack applies the contract middleware order. The key invariant —
// RequestID strictly before Logging — is verified by checking the log line
// carries a non-empty request_id (Logger ran after RequestID set it).
func TestDefaultStackEnforcesContractOrder(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	stack := DefaultStack(DefaultStackConfig{
		Logger:   logger,
		Compress: true,
		Timeout:  time.Second,
	})
	// First three are always-on in contract order.
	if len(stack) < 3 {
		t.Fatalf("stack has %d layers, want >= 3", len(stack))
	}

	sawID := false
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if RequestIDFromContext(r.Context()) != "" {
			sawID = true
		}
		w.WriteHeader(http.StatusOK)
	})
	var wrapped http.Handler = h
	for i := len(stack) - 1; i >= 0; i-- {
		wrapped = stack[i](wrapped)
	}
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if !sawID {
		t.Error("RequestID did not run before the handler")
	}
	out := buf.String()
	if !strings.Contains(out, "request_id=") {
		t.Errorf("Logger did not run; log = %q", out)
	}
	if strings.Contains(out, `request_id=""`) {
		t.Error("request_id empty in log — RequestID must precede Logging")
	}
}
