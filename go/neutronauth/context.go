package neutronauth

import "context"

type ctxKey int

const (
	ctxKeyClaims ctxKey = iota
	ctxKeySession
	ctxKeyOAuthUser
)

func withClaims(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, ctxKeyClaims, claims)
}

// WithClaims stores verified claims in the request context, where
// ClaimsFromContext can read them back.
//
// Exported because the read side always was: an application that verifies a
// token in its own middleware — rather than using this package's JWT
// middleware — could read claims but had no supported way to set them, so
// ClaimsFromContext returned nothing for the whole request. Callers are
// responsible for verifying the claims first; this only carries them.
func WithClaims(ctx context.Context, claims Claims) context.Context {
	return withClaims(ctx, claims)
}

// WithOAuthUser stores an OAuthUser in the request context.
func WithOAuthUser(ctx context.Context, user *OAuthUser) context.Context {
	return context.WithValue(ctx, ctxKeyOAuthUser, user)
}

// OAuthUserFromContext extracts the OAuthUser from the request context.
func OAuthUserFromContext(ctx context.Context) *OAuthUser {
	u, _ := ctx.Value(ctxKeyOAuthUser).(*OAuthUser)
	return u
}
