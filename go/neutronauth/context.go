package neutronauth

import "context"

type ctxKey int

const (
	ctxKeyClaims ctxKey = iota
	ctxKeySession
)

func withClaims(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, ctxKeyClaims, claims)
}
