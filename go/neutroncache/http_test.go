package neutroncache

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// L1 only: these cover the middleware's sharing rules, which are decided
// before any L2 lookup.
func testCache(t *testing.T) *TieredCache {
	t.Helper()
	return NewTiered(64, nil)
}

// perUserHandler answers with whatever the caller's session cookie says, which
// is what a personalised page does.
func perUserHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := "anonymous"
		if c, err := r.Cookie("session"); err == nil {
			user = c.Value
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello " + user))
	})
}

func get(t *testing.T, h http.Handler, path, session string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if session != "" {
		req.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// The one that matters. Only `Authorization` was checked, while the entry was
// keyed on the URL alone — so an app authenticating with a session cookie
// stored every personalised response under a key shared by all visitors and
// served it to them.
func TestAuthenticatedResponseIsNotSharedBetweenUsers(t *testing.T) {
	h := HTTPCache(testCache(t), time.Minute)(perUserHandler())

	first := get(t, h, "/account", "alice")
	if got := first.Body.String(); got != "hello alice" {
		t.Fatalf("first response = %q", got)
	}

	second := get(t, h, "/account", "bob")
	if got := second.Body.String(); got != "hello bob" {
		t.Errorf("bob was served %q — one user's authenticated response leaked to another", got)
	}
	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("a cookie-bearing request was served from the shared cache")
	}
}

// An Authorization header must be treated the same way.
func TestBearerTokenResponseIsNotCached(t *testing.T) {
	h := HTTPCache(testCache(t), time.Minute)(perUserHandler())

	req := httptest.NewRequest(http.MethodGet, "/account", nil)
	req.Header.Set("Authorization", "Bearer token-a")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	req2 := httptest.NewRequest(http.MethodGet, "/account", nil)
	req2.Header.Set("Authorization", "Bearer token-b")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)

	if rec2.Header().Get("X-Cache") == "HIT" {
		t.Error("an Authorization-bearing request was served from the shared cache")
	}
}

// Anonymous traffic must still be cached, or the middleware does nothing.
func TestAnonymousResponseIsCached(t *testing.T) {
	h := HTTPCache(testCache(t), time.Minute)(perUserHandler())

	get(t, h, "/public", "")
	second := get(t, h, "/public", "")

	if second.Header().Get("X-Cache") != "HIT" {
		t.Error("anonymous request was not served from cache")
	}
	if got := second.Body.String(); got != "hello anonymous" {
		t.Errorf("cached body = %q", got)
	}
}

// The opt-in: a route whose response is the same for everyone stays cacheable
// even though the browser sends a session cookie with it.
func TestPublicRouteIsCachedDespiteCookie(t *testing.T) {
	calls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("marketing page"))
	})
	h := HTTPCache(testCache(t), time.Minute, WithPublicRoutes("/pricing"))(handler)

	get(t, h, "/pricing", "alice")
	second := get(t, h, "/pricing", "bob")

	if second.Header().Get("X-Cache") != "HIT" {
		t.Error("a route declared public was not cached for a cookie-bearing request")
	}
	if calls != 1 {
		t.Errorf("handler ran %d times, want 1", calls)
	}
}

// A hit must replay the response the handler actually wrote. This used to
// force application/json onto every hit, so an HTML route silently changed
// content type the moment it started being cached.
func TestCachedResponsePreservesContentType(t *testing.T) {
	h := HTTPCache(testCache(t), time.Minute)(perUserHandler())

	first := get(t, h, "/public", "")
	second := get(t, h, "/public", "")

	want := first.Header().Get("Content-Type")
	if want == "" {
		t.Fatal("origin sent no Content-Type")
	}
	if got := second.Header().Get("Content-Type"); got != want {
		t.Errorf("cached Content-Type = %q, want %q — the cache rewrote the response type", got, want)
	}
}

// A cached Set-Cookie would be replayed to everyone who hits the entry,
// handing one visitor's session to the next.
func TestResponseWithSetCookieIsNotCached(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "brand-new"})
		_, _ = w.Write([]byte("welcome"))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	get(t, h, "/login-landing", "")
	second := get(t, h, "/login-landing", "")

	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("a response setting a cookie was cached; it would hand that session to the next visitor")
	}
}

// An origin declaring the body caller-dependent must be believed.
func TestVaryCookieResponseIsNotCached(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Vary", "Cookie")
		_, _ = w.Write([]byte("depends"))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	get(t, h, "/varies", "")
	second := get(t, h, "/varies", "")

	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("a Vary: Cookie response was cached")
	}
}

func TestCacheControlPrivateIsNotCached(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, max-age=60")
		_, _ = w.Write([]byte("mine"))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	get(t, h, "/private", "")
	second := get(t, h, "/private", "")

	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("a Cache-Control: private response was cached")
	}
}

// Vary headers named by the caller must separate entries, or a gzip-encoded
// body gets served to a client that did not ask for one.
func TestVaryHeadersSeparateEntries(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("lang:" + r.Header.Get("Accept-Language")))
	})
	h := HTTPCache(testCache(t), time.Minute, WithVaryHeaders("Accept-Language"))(handler)

	req := httptest.NewRequest(http.MethodGet, "/page", nil)
	req.Header.Set("Accept-Language", "en")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	req2 := httptest.NewRequest(http.MethodGet, "/page", nil)
	req2.Header.Set("Accept-Language", "fr")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)

	if got := rec2.Body.String(); got != "lang:fr" {
		t.Errorf("body = %q, want lang:fr — the vary header did not separate cache entries", got)
	}
}

func TestNonGETIsNotCached(t *testing.T) {
	h := HTTPCache(testCache(t), time.Minute)(perUserHandler())

	req := httptest.NewRequest(http.MethodPost, "/public", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Header().Get("X-Cache") == "HIT" {
		t.Error("a POST was served from cache")
	}
}
