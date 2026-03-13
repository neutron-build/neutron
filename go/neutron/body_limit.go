package neutron

import (
	"net/http"
)

// DefaultBodyLimit is the default maximum request body size (1 MB).
const DefaultBodyLimit int64 = 1 << 20

// BodyLimit returns middleware that restricts the size of request bodies.
// If the body exceeds maxBytes, http.MaxBytesReader causes the read to fail
// and the server returns 413 Request Entity Too Large.
//
// Pass 0 to use DefaultBodyLimit (1 MB).
func BodyLimit(maxBytes int64) Middleware {
	if maxBytes <= 0 {
		maxBytes = DefaultBodyLimit
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}
