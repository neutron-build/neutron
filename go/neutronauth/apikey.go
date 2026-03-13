package neutronauth

import (
	"net/http"
	"strings"

	"github.com/neutron-dev/neutron-go/neutron"
)

// APIKeyMiddleware returns middleware that validates API keys using the
// provided validator function. The key is read from the X-API-Key header
// or the Authorization header with "ApiKey" scheme.
func APIKeyMiddleware(validator func(key string) (bool, error)) neutron.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				auth := r.Header.Get("Authorization")
				if strings.HasPrefix(auth, "ApiKey ") {
					key = strings.TrimPrefix(auth, "ApiKey ")
				}
			}
			if key == "" {
				neutron.WriteError(w, r, neutron.ErrUnauthorized("missing API key"))
				return
			}

			valid, err := validator(key)
			if err != nil {
				neutron.WriteError(w, r, neutron.ErrInternal("API key validation failed"))
				return
			}
			if !valid {
				neutron.WriteError(w, r, neutron.ErrUnauthorized("invalid API key"))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
