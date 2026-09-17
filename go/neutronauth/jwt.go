package neutronauth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/neutron-build/neutron/go/neutron"
)

// jwtHeader is the fixed header for HS256 JWTs.
var jwtHeader = base64URLEncode([]byte(`{"alg":"HS256","typ":"JWT"}`))

// Claims holds JWT claims as a generic map.
type Claims map[string]any

// jwtMinSecretLen is the minimum HS256 key size. A 32-character password is
// not a high-entropy key just because it passes a length check — generate
// keys randomly (GO-14).
const jwtMinSecretLen = 32

// GenerateToken creates a signed JWT with the given claims.
//
// The caller's map is never mutated (GO-15): timestamps are written to a
// copy, so a nil map works, reuse cannot overwrite caller data, and
// concurrent generations from one map do not race.
func GenerateToken(claims Claims, secret string, expiry time.Duration) (string, error) {
	if len(secret) < jwtMinSecretLen {
		return "", fmt.Errorf("neutronauth: HS256 key must be at least %d bytes of high-entropy material", jwtMinSecretLen)
	}
	if expiry <= 0 {
		return "", fmt.Errorf("neutronauth: token lifetime must be positive")
	}
	owned := make(Claims, len(claims)+2)
	for key, value := range claims {
		owned[key] = value
	}
	now := time.Now()
	owned["iat"] = now.Unix()
	owned["exp"] = now.Add(expiry).Unix()

	payload, err := json.Marshal(owned)
	if err != nil {
		return "", fmt.Errorf("neutronauth: marshal claims: %w", err)
	}

	encodedPayload := base64URLEncode(payload)
	signingInput := jwtHeader + "." + encodedPayload
	sig := sign(signingInput, secret)

	return signingInput + "." + sig, nil
}

// ParseToken verifies and decodes a JWT, returning the claims.
//
// Verification policy (GO-14):
//   - the JOSE header must be the fixed HS256 JWT header this package emits —
//     an attacker-supplied header is never trusted for algorithm selection
//   - claims decode with UseNumber, so large NumericDates keep their precision
//   - `exp` is REQUIRED and must be a JSON number; a missing, string, or null
//     expiration no longer bypasses the check, and a token at/past its
//     expiration is rejected (was: strictly after)
//   - `nbf` (not-before), when present, must be a valid NumericDate and is
//     honored
//   - a duplicated JSON key fails the parse rather than silently picking one
//
// Issuer/audience remain application policy (verified against the returned
// claims by the caller).
func ParseToken(tokenStr, secret string) (Claims, error) {
	if len(secret) < jwtMinSecretLen {
		return nil, fmt.Errorf("neutronauth: HS256 key must be at least %d bytes of high-entropy material", jwtMinSecretLen)
	}
	parts := strings.SplitN(tokenStr, ".", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("neutronauth: invalid token format")
	}

	signingInput := parts[0] + "." + parts[1]
	expectedSig := sign(signingInput, secret)
	if !hmac.Equal([]byte(parts[2]), []byte(expectedSig)) {
		return nil, fmt.Errorf("neutronauth: invalid signature")
	}

	if parts[0] != jwtHeader {
		return nil, fmt.Errorf("neutronauth: unsupported JWT header")
	}

	payload, err := base64URLDecode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("neutronauth: decode payload: %w", err)
	}

	claims, err := decodeClaimsObject(payload)
	if err != nil {
		return nil, fmt.Errorf("neutronauth: decode claims: %w", err)
	}

	// Expiration is mandatory for session tokens and must be a JSON number.
	exp, err := numericDate(claims["exp"])
	if err != nil {
		return nil, fmt.Errorf("neutronauth: missing or invalid expiration")
	}
	now := float64(time.Now().Unix())
	if now >= exp {
		return nil, fmt.Errorf("neutronauth: token expired")
	}

	if raw, exists := claims["nbf"]; exists {
		nbf, err := numericDate(raw)
		if err != nil {
			return nil, fmt.Errorf("neutronauth: invalid not-before claim")
		}
		if now < nbf {
			return nil, fmt.Errorf("neutronauth: token not yet valid")
		}
	}

	return claims, nil
}

// decodeClaimsObject decodes a JSON object with UseNumber semantics and
// rejects duplicate keys and trailing data.
func decodeClaimsObject(raw []byte) (Claims, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("expected JSON object")
	}
	claims := Claims{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("invalid object key")
		}
		if _, exists := claims[key]; exists {
			return nil, fmt.Errorf("duplicate JSON key %q", key)
		}
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		claims[key] = value
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') {
		return nil, fmt.Errorf("unterminated object")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON data")
	}
	return claims, nil
}

// numericDate extracts a JWT NumericDate: it must be a JSON number.
func numericDate(raw any) (float64, error) {
	number, ok := raw.(json.Number)
	if !ok {
		return 0, fmt.Errorf("NumericDate must be a JSON number")
	}
	value, err := number.Float64()
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("invalid NumericDate")
	}
	return value, nil
}

// JWTOption configures the JWT middleware.
type JWTOption func(*jwtOpts)

type jwtOpts struct {
	headerName string
	scheme     string
	skipPaths  map[string]bool
}

// WithHeaderName sets the header to read the token from (default: Authorization).
func WithHeaderName(name string) JWTOption {
	return func(o *jwtOpts) { o.headerName = name }
}

// WithScheme sets the auth scheme (default: Bearer).
func WithScheme(scheme string) JWTOption {
	return func(o *jwtOpts) { o.scheme = scheme }
}

// WithSkipPaths sets paths that skip JWT validation.
func WithSkipPaths(paths ...string) JWTOption {
	return func(o *jwtOpts) {
		for _, p := range paths {
			o.skipPaths[p] = true
		}
	}
}

// JWTMiddleware returns middleware that validates JWT tokens.
// Valid claims are stored in the request context.
func JWTMiddleware(secret string, opts ...JWTOption) neutron.Middleware {
	o := jwtOpts{
		headerName: "Authorization",
		scheme:     "Bearer",
		skipPaths:  make(map[string]bool),
	}
	for _, fn := range opts {
		fn(&o)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if o.skipPaths[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}

			header := r.Header.Get(o.headerName)
			if header == "" {
				neutron.WriteError(w, r, neutron.ErrUnauthorized("missing authorization header"))
				return
			}

			token := header
			if o.scheme != "" {
				prefix := o.scheme + " "
				if !strings.HasPrefix(header, prefix) {
					neutron.WriteError(w, r, neutron.ErrUnauthorized("invalid authorization scheme"))
					return
				}
				token = strings.TrimPrefix(header, prefix)
			}

			claims, err := ParseToken(token, secret)
			if err != nil {
				neutron.WriteError(w, r, neutron.ErrUnauthorized(err.Error()))
				return
			}

			ctx := withClaims(r.Context(), claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ClaimsFromContext extracts JWT claims from the request context.
func ClaimsFromContext(ctx interface{ Value(any) any }) (Claims, error) {
	claims, ok := ctx.Value(ctxKeyClaims).(Claims)
	if !ok {
		return nil, fmt.Errorf("neutronauth: no claims in context")
	}
	return claims, nil
}

func sign(input, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(input))
	return base64URLEncode(mac.Sum(nil))
}

func base64URLEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

func base64URLDecode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
