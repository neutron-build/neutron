package neutroncache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/neutron-dev/neutron-go/neutron"
)

// HTTPCache returns middleware that caches full HTTP responses in the
// tiered cache. Only GET requests with 200 status are cached.
func HTTPCache(c *TieredCache, ttl time.Duration) neutron.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				next.ServeHTTP(w, r)
				return
			}

			// Never cache authenticated requests — responses may be user-specific
			if r.Header.Get("Authorization") != "" {
				next.ServeHTTP(w, r)
				return
			}

			cacheKey := "httpcache:" + hashKey(r.URL.String())

			// Check cache
			if data, ok := c.l1.Get(cacheKey); ok {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Header().Set("X-Cache", "HIT")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(data)
				return
			}

			// Capture response
			rec := &responseRecorder{
				ResponseWriter: w,
				body:           &bytes.Buffer{},
				status:         http.StatusOK,
			}
			next.ServeHTTP(rec, r)

			// Cache only 200 responses
			if rec.status == http.StatusOK {
				c.l1.Set(cacheKey, rec.body.Bytes(), ttl)
			}
		})
	}
}

type responseRecorder struct {
	http.ResponseWriter
	body   *bytes.Buffer
	status int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

func hashKey(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:16])
}
