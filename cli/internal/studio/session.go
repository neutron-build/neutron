package studio

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"os"
	"strconv"
)

// Session token and exact-origin checks for mutating endpoints (S01).
//
// The Studio server is loopback-only, but loopback alone is not a CSRF
// boundary: any page open in the user's browser can address
// http://localhost:<port> and let the browser's ambient authority send the
// request. Every endpoint that can change database or Studio state therefore
// requires BOTH:
//
//   - an Origin header that, when present, matches the launch origin
//     EXACTLY (scheme + host + port — no wildcard, no other localhost port),
//     and
//   - the per-launch session token in X-Studio-Session, generated from
//     crypto/rand at server start. A token from an earlier launch never
//     validates (fresh value every start).
//
// Requests without an Origin header (curl, tests, same-origin tools) are not
// CSRF-able and still must present the token. The token is readable only
// through GET /api/session, which applies the same exact-origin check. CORS
// echoes only the exact allowed origins (never "any localhost port"), and
// the Host header must name the loopback listener, which defeats DNS
// rebinding.

const sessionHeader = "X-Studio-Session"

// devOriginEnv names the environment variable that explicitly allows one
// additional origin for the launch (e.g. a dev server proxy on another port).
const devOriginEnv = "NEUTRON_STUDIO_DEV_ORIGIN"

// newSessionToken returns a fresh 256-bit token, hex encoded.
func newSessionToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// allowedOrigins is the exact-match allowlist for this launch: the loopback
// origins on the bound port, plus the explicitly configured dev origin when
// NEUTRON_STUDIO_DEV_ORIGIN is set.
func (s *Server) allowedOrigins() []string {
	origins := []string{
		"http://localhost:" + strconv.Itoa(s.port),
		"http://127.0.0.1:" + strconv.Itoa(s.port),
		"http://[::1]:" + strconv.Itoa(s.port),
	}
	if dev := os.Getenv(devOriginEnv); dev != "" {
		origins = append(origins, dev)
	}
	return origins
}

// originAllowed reports whether the origin matches an allowed origin exactly.
func (s *Server) originAllowed(origin string) bool {
	for _, allowed := range s.allowedOrigins() {
		if origin == allowed {
			return true
		}
	}
	return false
}

// checkOrigin enforces the exact-origin rule for a request. A present Origin
// must match exactly (this includes the explicitly configured dev origin).
// Without an Origin header, a browser-marked cross-site/same-site fetch is
// refused; non-browser clients (no Origin, no Sec-Fetch-Site) pass on to the
// token check.
func (s *Server) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return !crossSiteFetch(r)
	}
	return s.originAllowed(origin)
}

// tokenValid reports whether the presented session token matches this
// launch's token, in constant time.
func (s *Server) tokenValid(presented string) bool {
	if s.sessionToken == "" || presented == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(s.sessionToken)) == 1
}

// allowedHosts is the exact Host-header allowlist: the loopback names on the
// bound port. Rejecting any other Host defeats DNS rebinding — a page on
// attacker.example that re-resolves to 127.0.0.1 is same-origin with itself,
// sends no Origin header on GETs, and would otherwise read table data and
// the session token.
func (s *Server) allowedHosts() []string {
	p := strconv.Itoa(s.port)
	return []string{"localhost:" + p, "127.0.0.1:" + p, "[::1]:" + p}
}

func (s *Server) hostAllowed(host string) bool {
	for _, h := range s.allowedHosts() {
		if host == h {
			return true
		}
	}
	return false
}

// crossSiteFetch reports whether the browser marked the request as coming
// from another site or origin (Sec-Fetch-Site). Absent header (non-browser
// clients, older browsers) is decided by the Origin and token checks alone.
func crossSiteFetch(r *http.Request) bool {
	switch r.Header.Get("Sec-Fetch-Site") {
	case "cross-site", "same-site":
		return true
	}
	return false
}

// writeAuthError writes a JSON 403 with a machine-readable auth code so the
// SPA can tell a stale session token (refetch and retry: the request was
// refused before doing anything) from a wrong origin (never retry).
func writeAuthError(w http.ResponseWriter, code, msg string) {
	writeJSON(w, http.StatusForbidden, map[string]string{"error": msg, "auth": code})
}

// requireMutationAuth guards a mutating endpoint. It writes the 403 response
// itself and returns false when the request must not proceed.
func (s *Server) requireMutationAuth(w http.ResponseWriter, r *http.Request) bool {
	if !s.checkOrigin(r) {
		writeAuthError(w, "origin", "origin not allowed")
		return false
	}
	if !s.tokenValid(r.Header.Get(sessionHeader)) {
		writeAuthError(w, "session", "missing or invalid session token")
		return false
	}
	return true
}

// isMutatingMethod reports whether an HTTP method can change state. Every
// such request to /api/ is gated centrally in the middleware (and again in
// the handlers), so a new endpoint cannot forget the check.
func isMutatingMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	return true
}

// handleSession serves the per-launch session token to the SPA. The exact
// same origin rule as mutations applies: a page on another origin may send
// the request but cannot use the answer, and browsers will not expose a
// cross-origin response body that lacks CORS headers.
func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.checkOrigin(r) {
		writeAuthError(w, "origin", "origin not allowed")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]string{"token": s.sessionToken})
}
