package neutron

import (
	"net/http"
	"strings"
)

// TrailingSlashRedirect returns middleware that redirects requests with a
// trailing slash to the same path without it (HTTP 301). The root path "/"
// is left unchanged. Query strings are preserved across the redirect.
func TrailingSlashRedirect() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := r.URL.Path
			if p != "/" && strings.HasSuffix(p, "/") {
				target := strings.TrimRight(p, "/")
				if r.URL.RawQuery != "" {
					target += "?" + r.URL.RawQuery
				}
				http.Redirect(w, r, target, http.StatusMovedPermanently)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// TrailingSlashStrip returns middleware that silently strips a trailing slash
// from the request path without issuing a redirect. The root path "/" is left
// unchanged. The modified path is passed to the next handler in-place.
func TrailingSlashStrip() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" && strings.HasSuffix(r.URL.Path, "/") {
				r.URL.Path = strings.TrimRight(r.URL.Path, "/")
			}
			next.ServeHTTP(w, r)
		})
	}
}
