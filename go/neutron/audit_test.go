package neutron

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// GO-01: typed handlers with pointer inputs must bind instead of panicking.
func TestPointerInputBinding(t *testing.T) {
	type input struct {
		Name string `query:"name"`
		N    int8   `query:"n"`
	}
	router := newRouter()
	Register[*input, Empty](router, "GET", "/thing", func(ctx context.Context, in *input) (Empty, error) {
		if in.Name != "x" || in.N != 7 {
			t.Errorf("bound input = %+v", in)
		}
		return Empty{}, nil
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/thing?name=x&n=7", nil)
	router.ServeHTTP(rec, req)
	// 204: an Empty output maps to NoContent. The assertion that matters is
	// that the request completed at all — the old code PANICKED here.
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
}

// GO-02: narrow integer fields reject out-of-range values with a 400 rather
// than silently wrapping.
func TestNarrowIntegerRejected(t *testing.T) {
	type input struct {
		N int8 `query:"n"`
	}
	router := newRouter()
	Get(router, "/narrow", func(ctx context.Context, in input) (Empty, error) {
		return Empty{}, nil
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/narrow?n=128", nil)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("int8 overflow: status = %d, want 400", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/narrow?n=junk", nil)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("non-integer: status = %d, want 400", rec.Code)
	}
}

// GO-03: a JSON body with a trailing second value is rejected.
func TestTrailingJSONRejected(t *testing.T) {
	type input struct {
		A int `json:"a"`
	}
	router := newRouter()
	Post(router, "/json", func(ctx context.Context, in input) (Empty, error) {
		return Empty{}, nil
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/json", strings.NewReader(`{"a":1}{"a":2}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("trailing JSON: status = %d, want 400", rec.Code)
	}
}

// GO-04: an unencodable value (NaN) answers 500, never an empty 200.
func TestJSONEncodeFailureAnswers500(t *testing.T) {
	rec := httptest.NewRecorder()
	JSON(rec, http.StatusOK, map[string]float64{"x": nan()})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "{") {
		t.Fatalf("body must not be JSON, got %q", rec.Body.String())
	}
}

func nan() float64 {
	var z float64
	return z / z
}

// GO-06: credentialed wildcard CORS panics at construction.
func TestCredentialedWildcardCORSRejected(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("wildcard + credentials must panic at construction")
		}
	}()
	CORS(CORSOptions{AllowOrigins: []string{"*"}, AllowCredentials: true})
}

// GO-07: an OPTIONS request without CORS headers reaches the application
// handler instead of being swallowed by a canned 204.
func TestPlainOptionsReachesHandler(t *testing.T) {
	reached := false
	handler := CORS(CORSOptions{AllowOrigins: []string{"http://example.com"}})(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reached = true
			w.WriteHeader(http.StatusTeapot)
		}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("OPTIONS", "/", nil) // no Origin, no ACRM
	handler.ServeHTTP(rec, req)
	if !reached {
		t.Fatal("plain OPTIONS was intercepted by CORS")
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", rec.Code)
	}
}

// GO-08: gzip;q=0 must not be compressed.
func TestGzipZeroQualityNotCompressed(t *testing.T) {
	handler := Compress(gzip.DefaultCompression)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "hello")
		}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip;q=0")
	handler.ServeHTTP(rec, req)
	if rec.Header().Get("Content-Encoding") == "gzip" {
		t.Fatal("response compressed despite gzip;q=0")
	}
	if rec.Body.String() != "hello" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

// GO-09: a handler that sets Content-Length after the wrapper runs must not
// produce compressed bytes with the uncompressed length.
func TestGzipContentLengthNotLiedAbout(t *testing.T) {
	handler := Compress(gzip.DefaultCompression)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			body := "hellohellohello" // compresses well below its length
			w.Header().Set("Content-Length", "16")
			io.WriteString(w, body)
		}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	handler.ServeHTTP(rec, req)

	if enc := rec.Header().Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", enc)
	}
	if cl := rec.Header().Get("Content-Length"); cl == "16" {
		t.Fatal("uncompressed Content-Length shipped with a gzip body")
	}
	// And the body must be a valid gzip stream decoding to the original.
	zr, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("body is not valid gzip: %v", err)
	}
	decoded, _ := io.ReadAll(zr)
	if string(decoded) != "hellohellohello" {
		t.Fatalf("decoded = %q", decoded)
	}
}

// GO-10: the recorded status is the FIRST final status; a later WriteHeader
// cannot rewrite it. Observes the statusWriter directly since the slog
// logger receives the recorded value.
func TestStatusWriterRecordsFirstFinalStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec, status: http.StatusOK}
	sw.WriteHeader(http.StatusOK)
	sw.WriteHeader(http.StatusInternalServerError) // ignored by net/http
	if sw.status != http.StatusOK {
		t.Fatalf("recorded status = %d, want 200 (the status actually sent)", sw.status)
	}
}

// GO-10: a body Write with no WriteHeader records the implicit 200.
func TestStatusWriterImplicit200(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec, status: http.StatusOK}
	if _, err := sw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if sw.status != http.StatusOK {
		t.Fatalf("recorded status = %d, want 200", sw.status)
	}
}
