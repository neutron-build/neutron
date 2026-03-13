package neutronauth

import (
	"net/http"

	"github.com/neutron-dev/neutron-go/neutron"
)

// RequireRole returns middleware that ensures the authenticated user has
// at least one of the specified roles. Roles are read from JWT claims["role"]
// or claims["roles"].
func RequireRole(roles ...string) neutron.Middleware {
	allowed := make(map[string]bool, len(roles))
	for _, r := range roles {
		allowed[r] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, err := ClaimsFromContext(r.Context())
			if err != nil {
				neutron.WriteError(w, r, neutron.ErrUnauthorized("authentication required"))
				return
			}

			if hasRole(claims, allowed) {
				next.ServeHTTP(w, r)
				return
			}

			neutron.WriteError(w, r, neutron.ErrForbidden("insufficient role"))
		})
	}
}

// RequirePermission returns middleware that ensures the authenticated user
// has all of the specified permissions. Permissions are read from
// claims["permissions"].
func RequirePermission(perms ...string) neutron.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, err := ClaimsFromContext(r.Context())
			if err != nil {
				neutron.WriteError(w, r, neutron.ErrUnauthorized("authentication required"))
				return
			}

			if hasPermissions(claims, perms) {
				next.ServeHTTP(w, r)
				return
			}

			neutron.WriteError(w, r, neutron.ErrForbidden("insufficient permissions"))
		})
	}
}

func hasRole(claims Claims, allowed map[string]bool) bool {
	// Check single "role" claim
	if role, ok := claims["role"].(string); ok {
		if allowed[role] {
			return true
		}
	}
	// Check "roles" array claim
	if roles, ok := claims["roles"].([]any); ok {
		for _, r := range roles {
			if s, ok := r.(string); ok && allowed[s] {
				return true
			}
		}
	}
	return false
}

func hasPermissions(claims Claims, required []string) bool {
	perms, ok := claims["permissions"].([]any)
	if !ok {
		return false
	}
	permSet := make(map[string]bool, len(perms))
	for _, p := range perms {
		if s, ok := p.(string); ok {
			permSet[s] = true
		}
	}
	for _, req := range required {
		if !permSet[req] {
			return false
		}
	}
	return true
}
