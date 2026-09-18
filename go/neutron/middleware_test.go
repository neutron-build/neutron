package neutron

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"net"
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

	// Preflight (a genuine one carries Access-Control-Request-Method)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("OPTIONS", "/", nil)
	r.Header.Set("Origin", "http://example.com")
	r.Header.Set("Access-Control-Request-Method", "POST")
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

// TestDefaultStackAllLayersInContractOrder pins the full DefaultStack order
// (contract §5) by observation rather than by inspecting the slice, so a
// reorder in middleware.go fails here instead of drifting silently. The
// injected Auth layer is the reference point: everything the contract places
// before it must have left its mark by the time Auth runs, and everything
// after it must not have.
//
// Adjacencies not pinned here: Timeout↔OTel. The two layers have no
// cross-observable effect (a context deadline is invisible to OTel; a trace
// id is invisible to Timeout), so no black-box test can distinguish their
// order — swapping them changes nothing any client can see.
func TestDefaultStackAllLayersInContractOrder(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var authCalls int
	var sawRequestID, sawCORS, sawVaryAcceptEncoding, sawDeadlineAtAuth, sawTraceAtAuth bool

	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authCalls++
			sawRequestID = w.Header().Get("X-Request-Id") != ""
			sawCORS = w.Header().Get("Access-Control-Allow-Origin") != ""
			sawVaryAcceptEncoding = w.Header().Get("Vary") != ""
			_, sawDeadlineAtAuth = r.Context().Deadline()
			sawTraceAtAuth = TraceIDFromContext(r.Context()) != ""
			next.ServeHTTP(w, r)
		})
	}

	stack := DefaultStack(DefaultStackConfig{
		Logger:    logger,
		CORS:      &CORSOptions{AllowOrigins: []string{"https://example.com"}},
		Compress:  true,
		RateLimit: &RateLimitConfig{RPS: 0.000001, Burst: 1},
		Auth:      auth,
		Timeout:   time.Second,
		OTel:      &OTelOptions{ServiceName: "test"},
	})
	if len(stack) != 9 {
		t.Fatalf("stack has %d layers, want 9 (all configured)", len(stack))
	}

	var handlerSawTrace, handlerSawDeadline bool
	var wrapped http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerSawTrace = TraceIDFromContext(r.Context()) != ""
		_, handlerSawDeadline = r.Context().Deadline()
		w.WriteHeader(http.StatusOK)
	})
	wrapped = Chain(stack...)(wrapped)

	// Request 1: normal request through every layer.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Accept-Encoding", "gzip")
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("request 1: status = %d, want 200", rec.Code)
	}
	if authCalls != 1 {
		t.Fatalf("request 1: auth called %d times, want 1", authCalls)
	}
	if !sawRequestID {
		t.Error("RequestID did not run before Auth (no X-Request-Id header at Auth)")
	}
	if !sawCORS {
		t.Error("CORS did not run before Auth (no Access-Control-Allow-Origin at Auth)")
	}
	// Compress commits Content-Encoding lazily (at first write), so its
	// presence at Auth is proven by the Vary header it sets eagerly.
	if !sawVaryAcceptEncoding {
		t.Error("Compress did not run before Auth (no Vary: Accept-Encoding at Auth)")
	}
	if sawDeadlineAtAuth {
		t.Error("Timeout ran before Auth — contract places Timeout after Auth")
	}
	if sawTraceAtAuth {
		t.Error("OTel ran before Auth — contract places OpenTelemetry after Auth")
	}
	if !handlerSawDeadline {
		t.Error("Timeout did not run after Auth (handler saw no deadline)")
	}
	if !handlerSawTrace {
		t.Error("OTel did not run after Auth (handler saw no trace id)")
	}
	if id := RequestIDFromContext(req.Context()); id != "" {
		t.Error("outer middleware must not mutate the original request context")
	}
	if out := logBuf.String(); !strings.Contains(out, "request_id=") || strings.Contains(out, `request_id=""`) {
		t.Errorf("log line missing non-empty request_id — RequestID must precede Logging: %q", out)
	}

	// Request 2: token bucket drained (burst 1, negligible refill), so RateLimit rejects.
	// Auth must not run, and the layers outer than RateLimit must still act.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("GET", "/", nil)
	req2.Header.Set("Origin", "https://example.com")
	req2.Header.Set("Accept-Encoding", "gzip")
	wrapped.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("request 2: status = %d, want 429", rec2.Code)
	}
	if authCalls != 1 {
		t.Errorf("request 2: auth called %d times total — RateLimit must run before Auth", authCalls)
	}
	if ct := rec2.Header().Get("Content-Type"); !strings.Contains(ct, "application/problem+json") {
		t.Errorf("request 2: Content-Type = %q, want application/problem+json", ct)
	}
	if rec2.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Error("request 2: no CORS header on 429 — CORS must run before RateLimit")
	}
	if rec2.Header().Get("Content-Encoding") != "gzip" {
		t.Error("request 2: 429 not compressed — Compress must run before RateLimit")
	}

	// Request 3: preflight. CORS answers OPTIONS itself without calling inner
	// layers (middleware.go:190-193), so an uncompressed 204 proves CORS wraps
	// Compress — had Compress run, Content-Encoding would be set on the header
	// map before CORS's WriteHeader froze it.
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodOptions, "/", nil)
	req3.Header.Set("Origin", "https://example.com")
	req3.Header.Set("Access-Control-Request-Method", "POST")
	req3.Header.Set("Accept-Encoding", "gzip")
	wrapped.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusNoContent {
		t.Fatalf("preflight: status = %d, want 204", rec3.Code)
	}
	if rec3.Header().Get("Content-Encoding") != "" {
		t.Error("preflight 204 is compressed — CORS must run before Compress and answer without it")
	}
	if authCalls != 1 {
		t.Errorf("preflight: auth called %d times total — CORS must short-circuit before RateLimit/Auth", authCalls)
	}

	// Panic probe: a panic in Auth (innermost user layer) is caught by Recover
	// and written as RFC 7807, while Logger — outer than Recover — still logs
	// the request.
	var panicBuf bytes.Buffer
	panicLogger := slog.New(slog.NewTextHandler(&panicBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	panicStack := DefaultStack(DefaultStackConfig{
		Logger: panicLogger,
		Auth: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic("boom") })
		},
	})
	rec4 := httptest.NewRecorder()
	Chain(panicStack...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).
		ServeHTTP(rec4, httptest.NewRequest("GET", "/", nil))

	if rec4.Code != http.StatusInternalServerError {
		t.Fatalf("panic: status = %d, want 500", rec4.Code)
	}
	if ct := rec4.Header().Get("Content-Type"); !strings.Contains(ct, "application/problem+json") {
		t.Errorf("panic: Content-Type = %q, want application/problem+json", ct)
	}
	if out := panicBuf.String(); !strings.Contains(out, "status=500") {
		t.Errorf("panic: no request log line — Logger must run outside Recover: %q", out)
	}
}

// GO-25: gzip finalization must not corrupt bypassed, empty, or flush-first
// responses. The old writer closed an eagerly-created encoder
// unconditionally, appending an empty gzip member to responses that were
// never compressed (and carried no Content-Encoding).
func TestGzipBypassedResponseIsByteForByteUnchanged(t *testing.T) {
	handler := Compress(gzip.DefaultCompression)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-transform")
		_, _ = w.Write([]byte("abc"))
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	handler.ServeHTTP(w, r)

	if enc := w.Header().Get("Content-Encoding"); enc != "" {
		t.Errorf("no-transform response was encoded: %q", enc)
	}
	if got := w.Body.String(); got != "abc" {
		t.Errorf("body = %q (%d bytes), want exactly \"abc\" — an empty gzip member was appended", got, w.Body.Len())
	}
}

func TestGzipEmptyHandlerEmitsNothing(t *testing.T) {
	handler := Compress(gzip.DefaultCompression)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	handler.ServeHTTP(w, r)

	if enc := w.Header().Get("Content-Encoding"); enc != "" {
		t.Errorf("empty response was encoded: %q", enc)
	}
	if w.Body.Len() != 0 {
		t.Errorf("empty handler produced %d bytes of trailer-only gzip output", w.Body.Len())
	}
}

// GO-25: a Flush before any Write must not push gzip bytes under an
// unencoded implicit-200. With no Content-Type set there is nothing safe to
// sniff from, so the response stays identity.
func TestGzipFlushFirstWithoutContentTypeStaysIdentity(t *testing.T) {
	handler := Compress(gzip.DefaultCompression)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := w.(http.Flusher)
		f.Flush()
		_, _ = w.Write([]byte("abc"))
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	handler.ServeHTTP(w, r)

	if enc := w.Header().Get("Content-Encoding"); enc != "" {
		t.Errorf("flush-first response was encoded with no Content-Type: %q", enc)
	}
	if got := w.Body.String(); got != "abc" {
		t.Errorf("body = %q, want \"abc\"", got)
	}
}

// GO-25: with a Content-Type declared, a flush-first response commits AS
// gzip and the flushed stream decodes back to the original bytes.
func TestGzipFlushFirstWithContentTypeCommitsAsGzip(t *testing.T) {
	handler := Compress(gzip.DefaultCompression)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		_, _ = w.Write([]byte("data: one\n\n"))
		f.Flush()
		_, _ = w.Write([]byte("data: two\n\n"))
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	handler.ServeHTTP(w, r)

	if enc := w.Header().Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", enc)
	}
	zr, err := gzip.NewReader(w.Body)
	if err != nil {
		t.Fatalf("body is not a gzip stream: %v", err)
	}
	decoded, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gzip decode: %v", err)
	}
	if got := string(decoded); got != "data: one\n\ndata: two\n\n" {
		t.Errorf("decoded body = %q", got)
	}
}

// GO-25: informational statuses are forwarded without finalizing
// compression; the final body still lands in a well-formed gzip member.
// Uses a real server: httptest's recorder (and net/http itself) refuse body
// writes after a 1xx on the same writer, while a live connection treats the
// 103 as an interim response and expects a final one after it.
func TestGzipInformationalStatusDoesNotFinalize(t *testing.T) {
	handler := Compress(gzip.DefaultCompression)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(103) // early-hint style interim response
		_, _ = w.Write([]byte("payload"))
	}))
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if enc := resp.Header.Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", enc)
	}
	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("body is not a valid gzip stream: %v", err)
	}
	decoded, _ := io.ReadAll(zr)
	if string(decoded) != "payload" {
		t.Errorf("decoded body = %q", decoded)
	}
}

// GO-25: a Write with no Content-Type sniffs the ORIGINAL bytes, so the
// served media type describes the content, not the gzip magic.
func TestGzipWriteWithoutContentTypeSniffsOriginalBytes(t *testing.T) {
	handler := Compress(gzip.DefaultCompression)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("<html><body>hi</body></html>"))
	}))
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	handler.ServeHTTP(w, r)

	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want sniffed text/html", ct)
	}
	zr, err := gzip.NewReader(w.Body)
	if err != nil {
		t.Fatalf("body is not a gzip stream: %v", err)
	}
	decoded, _ := io.ReadAll(zr)
	if string(decoded) != "<html><body>hi</body></html>" {
		t.Errorf("decoded body = %q", decoded)
	}
}

// GO-26: Hijack must never emit response state. The old statusWriter wrote
// an unsolicited 101 BEFORE delegating — even when the underlying writer
// did not support Hijack at all.
func TestHijackEmitsNoResponseState(t *testing.T) {
	// Unsupported underlying writer: must return an error with zero side
	// effects.
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec}
	if _, _, err := sw.Hijack(); err == nil {
		t.Fatal("unsupported Hijack must fail")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("unsupported Hijack wrote status %d", rec.Code)
	}
	if len(rec.Header()) != 0 || rec.Body.Len() != 0 {
		t.Errorf("unsupported Hijack produced output: headers=%v body=%q", rec.Header(), rec.Body.String())
	}

	// Supported underlying writer: delegation succeeds and STILL writes no
	// header.
	stub := &hijackableWriter{ResponseWriter: httptest.NewRecorder()}
	sw2 := &statusWriter{ResponseWriter: stub}
	conn, buf, err := sw2.Hijack()
	if err != nil {
		t.Fatalf("Hijack failed: %v", err)
	}
	defer conn.Close()
	if buf == nil {
		t.Error("Hijack returned no ReadWriter")
	}
	if stub.headerWrites != 0 {
		t.Errorf("Hijack triggered %d WriteHeader calls on the underlying writer", stub.headerWrites)
	}
}

// hijackableWriter is a ResponseWriter whose Hijack succeeds and counts
// WriteHeader invocations.
type hijackableWriter struct {
	http.ResponseWriter
	headerWrites int
}

func (h *hijackableWriter) WriteHeader(int) { h.headerWrites++ }

func (h *hijackableWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	_, client := net.Pipe()
	return client, bufio.NewReadWriter(bufio.NewReader(client), bufio.NewWriter(client)), nil
}

// GO-27: the bucket map has a hard live-key ceiling. Past capacity, NEW
// identities are refused with 429 + Retry-After while EXISTING buckets keep
// their state — the old limiter instead swept the whole map on every new
// key and grew without bound when keys were all recent.
func TestRateLimitHardBucketBound(t *testing.T) {
	handler := RateLimit(1e9, 10)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	serve := func(remote string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = remote + ":1234"
		handler.ServeHTTP(w, r)
		return w
	}

	// Fill to capacity with distinct recent identities.
	for i := 0; i < rateLimitMaxBuckets; i++ {
		w := serve(fmt.Sprintf("10.0.%d.%d", i/256, i%256))
		if w.Code != http.StatusOK {
			t.Fatalf("fill request %d: status = %d", i, w.Code)
		}
	}

	// One NEW identity at capacity: refused, with a retry hint.
	w := serve("192.0.2.9")
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("new identity at capacity: status = %d, want 429", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("429 admission refusal lacks Retry-After")
	}

	// An EXISTING bucket keeps working (its tokens are intact).
	w = serve("10.0.0.0")
	if w.Code != http.StatusOK {
		t.Errorf("existing bucket at capacity: status = %d, want 200", w.Code)
	}
}
