package neutronmcp

import (
	"context"
	"net/http"
	"strings"
)

// Tool is one callable operation a server exposes.
//
// Scope is the capability a caller must hold to see or invoke it. An empty
// Scope means unrestricted — which is the right default only for reads that
// are already safe for anyone the Authorizer let through.
//
// ReadOnly and Destructive are advertised to clients as hints and, for
// ReadOnly, enforced: a read-only principal cannot call a tool that is not
// marked ReadOnly. The hints exist so an agent runtime can decide whether a
// call needs a human first, which is the same judgment needsApproval encodes
// on the TypeScript side.
type Tool struct {
	Name        string
	Description string

	// InputSchema is JSON Schema for the arguments object. Clients use it to
	// build calls, so an absent schema makes a tool effectively undiscoverable
	// even though it is listed.
	InputSchema map[string]any

	Scope       string
	ReadOnly    bool
	Destructive bool

	// Run executes the tool. Returning an error becomes an isError result
	// rather than a protocol error: the call reached the tool and the tool
	// failed, which is a different fact from "the call was malformed", and
	// conflating them makes a client retry things it should not.
	Run func(ctx context.Context, args map[string]any) (string, error)
}

// Principal is the authenticated caller and what it is allowed to do.
//
// Scopes is an allow-list, not a deny-list. A tool whose Scope is absent from
// this set is refused, because the failure mode of the other arrangement is a
// monitoring integration restarting production.
type Principal struct {
	Name     string
	Scopes   []string
	ReadOnly bool

	// Extra carries whatever the host needs to pass to its own tools — an org
	// id, a tenant, a user id. The package never reads it; tools retrieve it
	// with PrincipalFrom.
	Extra map[string]any
}

// Allows reports whether the principal may use a tool.
func (p Principal) Allows(t Tool) bool {
	if p.ReadOnly && !t.ReadOnly {
		return false
	}
	if t.Scope == "" {
		return true
	}
	for _, s := range p.Scopes {
		if s == t.Scope {
			return true
		}
	}
	return false
}

// Authorizer resolves a request to a principal. Returning false rejects the
// request with 401 before anything is parsed.
//
// It takes the whole request rather than a token string because deployments
// disagree about where the credential lives — a header, a cookie, a signed
// body, mutual TLS — and a package that assumed one of those would be unusable
// for the others.
type Authorizer func(r *http.Request) (Principal, bool)

// BearerAuthorizer adapts a token-verifying function into an Authorizer. Most
// callers want this; the Authorizer type is there for the ones that do not.
func BearerAuthorizer(verify func(token string) (Principal, bool)) Authorizer {
	return func(r *http.Request) (Principal, bool) {
		const scheme = "Bearer "
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, scheme) {
			return Principal{}, false
		}
		token := strings.TrimSpace(strings.TrimPrefix(auth, scheme))
		if token == "" {
			return Principal{}, false
		}
		return verify(token)
	}
}

// principalKey types the context value so nothing else can collide with it.
type principalKey struct{}

// PrincipalFrom returns the calling principal inside a Tool.Run. The second
// result is false outside a tool call, which is how a tool can tell it is being
// exercised directly in a test rather than served.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

func withPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// ToolInfo is a tool as a client sees it in tools/list.
type ToolInfo struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations *Annotations   `json:"annotations,omitempty"`
}

// Annotations are the protocol's advisory hints about what a tool does.
type Annotations struct {
	ReadOnlyHint    bool `json:"readOnlyHint"`
	DestructiveHint bool `json:"destructiveHint"`
}

// ObjectSchema is a small helper for the common case: an object with named
// properties, some required. Hand-writing JSON Schema maps inline is where
// typos become tools clients cannot call.
func ObjectSchema(properties map[string]any, required ...string) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

// StringProp, IntProp and BoolProp describe one property of an ObjectSchema.
func StringProp(description string) map[string]any {
	return map[string]any{"type": "string", "description": description}
}
func IntProp(description string) map[string]any {
	return map[string]any{"type": "integer", "description": description}
}
func BoolProp(description string) map[string]any {
	return map[string]any{"type": "boolean", "description": description}
}
