package neutron

import (
	"crypto/subtle"
	"net/http"
)

// BasicAuth returns middleware that enforces HTTP Basic authentication.
// The credentials map keys are usernames and values are passwords.
// Password comparison uses constant-time comparison to prevent timing attacks.
// On failure, a 401 response is returned with the WWW-Authenticate header
// and an RFC 7807 error body.
func BasicAuth(realm string, credentials map[string]string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			if !ok {
				requireAuth(w, r, realm)
				return
			}

			expectedPass, userExists := credentials[user]
			if !userExists ||
				subtle.ConstantTimeCompare([]byte(pass), []byte(expectedPass)) != 1 {
				requireAuth(w, r, realm)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func requireAuth(w http.ResponseWriter, r *http.Request, realm string) {
	w.Header().Set("WWW-Authenticate", `Basic realm="`+realm+`"`)
	WriteError(w, r, ErrUnauthorized("Valid credentials are required to access this resource"))
}
