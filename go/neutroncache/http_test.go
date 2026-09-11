package neutroncache

import (
	"bytes"
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

func TestCacheKeyIncludesHost(t *testing.T) {
	a := httptest.NewRequest(http.MethodGet, "http://a.test/page", nil)
	b := httptest.NewRequest(http.MethodGet, "http://b.test/page", nil)
	aAgain := httptest.NewRequest(http.MethodGet, "http://a.test/page", nil)

	if cacheKeyFor(a, nil) == cacheKeyFor(b, nil) {
		t.Error("same path on different hosts produced the same cache key")
	}
	if cacheKeyFor(a, nil) != cacheKeyFor(aAgain, nil) {
		t.Error("identical requests produced different cache keys")
	}
}

func TestCacheKeyVaryStillHonoredWithHost(t *testing.T) {
	base := httptest.NewRequest(http.MethodGet, "http://a.test/page", nil)
	en := httptest.NewRequest(http.MethodGet, "http://a.test/page", nil)
	en.Header.Set("Accept-Language", "en")
	fr := httptest.NewRequest(http.MethodGet, "http://a.test/page", nil)
	fr.Header.Set("Accept-Language", "fr")

	if cacheKeyFor(en, []string{"Accept-Language"}) == cacheKeyFor(fr, []string{"Accept-Language"}) {
		t.Error("different vary header values produced the same cache key")
	}
	if cacheKeyFor(base, []string{"Accept-Language"}) == cacheKeyFor(en, []string{"Accept-Language"}) {
		t.Error("missing vary header value collided with a set one")
	}
}

// One cache across virtual hosts must not serve host A's body to host B —
// the URL alone cannot tell them apart.
func TestVirtualHostsGetDistinctEntries(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("host:" + r.Host))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	first := httptest.NewRequest(http.MethodGet, "http://a.test/shared", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, first)
	if got := rec.Body.String(); got != "host:a.test" {
		t.Fatalf("first response = %q", got)
	}

	second := httptest.NewRequest(http.MethodGet, "http://b.test/shared", nil)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, second)

	if rec2.Header().Get("X-Cache") == "HIT" {
		t.Fatal("host B was served host A's cached response")
	}
	if got := rec2.Body.String(); got != "host:b.test" {
		t.Errorf("body = %q, want host:b.test", got)
	}

	// Host A's entry must still hit for host A.
	again := httptest.NewRequest(http.MethodGet, "http://a.test/shared", nil)
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, again)
	if rec3.Header().Get("X-Cache") != "HIT" {
		t.Error("host A's own entry no longer hits after host B's request")
	}
}

// --- Vary fields not represented in the key (audit neutron-18) ---

// Without the vary header declared, a Vary: Accept-Language response must
// not be cached at all — replaying it would serve English bytes to a French
// request.
func TestVaryOnUndeclaredHeaderIsNotCached(t *testing.T) {
	calls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Vary", "Accept-Language")
		_, _ = w.Write([]byte("lang:" + r.Header.Get("Accept-Language")))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	first := httptest.NewRequest(http.MethodGet, "/page", nil)
	first.Header.Set("Accept-Language", "en")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, first)

	second := httptest.NewRequest(http.MethodGet, "/page", nil)
	second.Header.Set("Accept-Language", "fr")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, second)

	if got := rec2.Body.String(); got != "lang:fr" {
		t.Errorf("body = %q — an undeclared Vary dimension was replayed across languages", got)
	}
	if second := rec2.Header().Get("X-Cache"); second == "HIT" {
		t.Error("a Vary on an undeclared header was served from cache")
	}
	if calls != 2 {
		t.Errorf("handler ran %d times, want 2", calls)
	}
}

func TestVaryWildcardIsNeverCached(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Vary", "*")
		_, _ = w.Write([]byte("unshared"))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	get(t, h, "/wild", "")
	second := get(t, h, "/wild", "")

	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("a Vary: * response was replayed from the shared cache")
	}
}

// Multiple Vary field lines and comma lists must all be parsed, not just
// the first.
func TestMultipleVaryFieldLinesAreParsed(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Accept-Encoding")
		w.Header().Add("Vary", "Accept-Language, X-Custom")
		_, _ = w.Write([]byte("body"))
	})
	h := HTTPCache(testCache(t), time.Minute, WithVaryHeaders("Accept-Encoding"))(handler)

	get(t, h, "/multi", "")
	second := get(t, h, "/multi", "")

	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("a Vary naming undeclared Accept-Language/X-Custom was cached")
	}
}

// A declared vary dimension still separates entries.
func TestDeclaredVaryHeaderIsCachedPerValue(t *testing.T) {
	calls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Vary", "Accept-Language")
		_, _ = w.Write([]byte("lang:" + r.Header.Get("Accept-Language")))
	})
	h := HTTPCache(testCache(t), time.Minute, WithVaryHeaders("Accept-Language"))(handler)

	en := httptest.NewRequest(http.MethodGet, "/page", nil)
	en.Header.Set("Accept-Language", "en")
	h.ServeHTTP(httptest.NewRecorder(), en)
	// Same language again: the origin echoed Vary, the key represents it,
	// so this may hit.
	en2 := httptest.NewRequest(http.MethodGet, "/page", nil)
	en2.Header.Set("Accept-Language", "en")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, en2)
	if got := rec2.Body.String(); got != "lang:en" {
		t.Errorf("body = %q, want lang:en", got)
	}

	fr := httptest.NewRequest(http.MethodGet, "/page", nil)
	fr.Header.Set("Accept-Language", "fr")
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, fr)
	if got := rec3.Body.String(); got != "lang:fr" {
		t.Errorf("body = %q — declared vary values were not separated", got)
	}
}

// --- Freshness contract (audit neutron-19) ---

func TestResponseNoCacheIsNotStored(t *testing.T) {
	calls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write([]byte("must revalidate"))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	get(t, h, "/fresh", "")
	second := get(t, h, "/fresh", "")

	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("a no-cache response became a fresh cache entry")
	}
	if calls != 2 {
		t.Errorf("handler ran %d times, want 2", calls)
	}
}

func TestResponseMaxAgeZeroIsNotStored(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=0, must-revalidate")
		_, _ = w.Write([]byte("immediately stale"))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	get(t, h, "/stale", "")
	second := get(t, h, "/stale", "")

	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("max-age=0 became a fresh one-minute entry")
	}
}

func TestRequestNoCacheBypassesLookupButRefreshes(t *testing.T) {
	calls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte("origin bytes"))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	// Prime the entry.
	first := get(t, h, "/refresh", "")
	if first.Header().Get("X-Cache") == "HIT" {
		t.Fatal("priming request unexpectedly hit")
	}

	req := httptest.NewRequest(http.MethodGet, "/refresh", nil)
	req.Header.Set("Cache-Control", "no-cache")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Header().Get("X-Cache") == "HIT" {
		t.Error("a client refresh request was served from cache without reaching the origin")
	}
	if calls != 2 {
		t.Errorf("handler ran %d times, want 2", calls)
	}

	// The bypass refreshed the entry, so a plain request may hit again.
	third := get(t, h, "/refresh", "")
	if calls != 2 {
		t.Errorf("plain request after refresh re-ran the origin (calls = %d)", calls)
	}
	_ = third
}

func TestResponseMaxAgeBoundsStoredTTL(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=2")
		_, _ = w.Write([]byte("bounded"))
	})
	c := testCache(t)
	h := HTTPCache(c, time.Hour)(handler)

	get(t, h, "/bounded", "")

	c.l1.mu.Lock()
	defer c.l1.mu.Unlock()
	if len(c.l1.items) != 1 {
		t.Fatalf("stored %d entries, want 1", len(c.l1.items))
	}
	for _, elem := range c.l1.items {
		entry := elem.Value.(*lruEntry)
		if remaining := time.Until(entry.expiresAt); remaining > 5*time.Second {
			t.Errorf("stored TTL = %v, want bounded by max-age=2", remaining)
		}
	}
}

// --- Streaming through the wrapper (audit neutron-20) ---

// flushRecorder counts flushes so a test can observe flushing before
// handler completion.
type flushRecorder struct {
	*httptest.ResponseRecorder
	flushes int
}

func (f *flushRecorder) Flush() {
	f.flushes++
	f.ResponseRecorder.Flush()
}

// A streaming GET that sets no-store must be able to flush through both
// ResponseController and a direct http.Flusher assertion, and must create
// no cache entry.
func TestStreamingResponseFlushesThroughWrapper(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		for i := 0; i < 3; i++ {
			_, _ = w.Write([]byte("chunk"))
			if f, ok := w.(http.Flusher); !ok {
				t.Error("downstream writer lost direct http.Flusher support through the cache wrapper")
				return
			} else {
				f.Flush()
			}
		}
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	under := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	h.ServeHTTP(under, req)

	if under.flushes != 3 {
		t.Errorf("flushes reached the underlying writer %d times, want 3", under.flushes)
	}
	if got := under.Body.String(); got != "chunkchunkchunk" {
		t.Errorf("streamed body = %q", got)
	}
}

func TestStreamingResponseSupportsResponseController(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			t.Errorf("ResponseController.Flush through the cache wrapper failed: %v", err)
		}
		_, _ = w.Write([]byte("sse data"))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	under := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	h.ServeHTTP(under, httptest.NewRequest(http.MethodGet, "/stream", nil))

	if under.flushes != 1 {
		t.Errorf("flushes = %d, want 1", under.flushes)
	}
}

func TestFlushedResponseCreatesNoCacheEntry(t *testing.T) {
	calls := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte("tick"))
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("tock"))
	})
	h := HTTPCache(testCache(t), time.Minute)(handler)

	get(t, h, "/events", "")
	second := get(t, h, "/events", "")

	if second.Header().Get("X-Cache") == "HIT" {
		t.Error("a flushed streaming response was cached and replayed")
	}
	if calls != 2 {
		t.Errorf("handler ran %d times, want 2", calls)
	}
	if got := second.Body.String(); got != "ticktock" {
		t.Errorf("streamed body = %q, want ticktock", got)
	}
}

func TestUnwrapExposesUnderlyingWriter(t *testing.T) {
	under := httptest.NewRecorder()
	rec := &responseRecorder{ResponseWriter: under, body: &bytes.Buffer{}}

	var unwrapped http.ResponseWriter = rec
	for i := 0; i < 3; i++ {
		u, ok := unwrapped.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			t.Fatalf("depth %d: writer has no Unwrap", i)
		}
		if u.Unwrap() == nil {
			t.Fatal("Unwrap returned nil")
		}
		unwrapped = u.Unwrap()
		if unwrapped == under {
			return
		}
	}
	t.Error("Unwrap chain never reached the underlying recorder")
}

func TestHijackOnNonHijackableWriterErrors(t *testing.T) {
	rec := &responseRecorder{ResponseWriter: httptest.NewRecorder(), body: &bytes.Buffer{}}
	if _, _, err := rec.Hijack(); err == nil {
		t.Error("Hijack advertised support on a non-hijackable underlying writer")
	}
}
