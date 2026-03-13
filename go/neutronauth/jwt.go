package neutronauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/neutron-dev/neutron-go/neutron"
)

// jwtHeader is the fixed header for HS256 JWTs.
var jwtHeader = base64URLEncode([]byte(`{"alg":"HS256","typ":"JWT"}`))

// Claims holds JWT claims as a generic map.
type Claims map[string]any

// GenerateToken creates a signed JWT with the given claims.
func GenerateToken(claims Claims, secret string, expiry time.Duration) (string, error) {
	now := time.Now()
	claims["iat"] = now.Unix()
	claims["exp"] = now.Add(expiry).Unix()

	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("neutronauth: marshal claims: %w", err)
	}

	encodedPayload := base64URLEncode(payload)
	signingInput := jwtHeader + "." + encodedPayload
	sig := sign(signingInput, secret)

	return signingInput + "." + sig, nil
}

// ParseToken verifies and decodes a JWT, returning the claims.
func ParseToken(tokenStr, secret string) (Claims, error) {
	parts := strings.SplitN(tokenStr, ".", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("neutronauth: invalid token format")
	}

	signingInput := parts[0] + "." + parts[1]
	expectedSig := sign(signingInput, secret)
	if !hmac.Equal([]byte(parts[2]), []byte(expectedSig)) {
		return nil, fmt.Errorf("neutronauth: invalid signature")
	}

	payload, err := base64URLDecode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("neutronauth: decode payload: %w", err)
	}

	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("neutronauth: unmarshal claims: %w", err)
	}

	// Check expiration
	if exp, ok := claims["exp"].(float64); ok {
		if time.Now().Unix() > int64(exp) {
			return nil, fmt.Errorf("neutronauth: token expired")
		}
	}

	return claims, nil
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
