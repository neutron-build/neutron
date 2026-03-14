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

// WithOAuthUser stores an OAuthUser in the request context.
func WithOAuthUser(ctx context.Context, user *OAuthUser) context.Context {
	return context.WithValue(ctx, ctxKeyOAuthUser, user)
}

// OAuthUserFromContext extracts the OAuthUser from the request context.
func OAuthUserFromContext(ctx context.Context) *OAuthUser {
	u, _ := ctx.Value(ctxKeyOAuthUser).(*OAuthUser)
	return u
}
