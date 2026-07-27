// Package neutronmcp speaks the Model Context Protocol, both ends.
//
// MCP is how an agent runtime reaches a system it does not contain: the system
// exposes tools, the runtime calls them, and neither has to know how the other
// is built. That makes it the interop layer for a framework whose whole premise
// is that the pieces are separable.
//
// Scope is deliberately the useful minimum: initialize, ping, tools/list and
// tools/call over streamable HTTP (request/response only). Resources, prompts,
// sampling and server-initiated messages are absent because nothing needs them
// yet, and a protocol surface nobody exercises is a surface nobody maintains.
// SSE is absent on purpose — it is the transport the specification has since
// moved away from, and inheriting it now would mean carrying it forever.
//
// Two things are pluggable because they are the two things every deployment
// disagrees about: authentication (a public platform wants OAuth, a tailnet
// wants a bearer token, an app wants its own session) and capability scoping
// (which caller may invoke which tool). Neither is decided here.
//
// The package holds no state between requests: no sessions, no subscriptions.
// Every call carries its own credential, so a server can be restarted, load
// balanced, or run in a dozen replicas without any of them agreeing on anything.
package neutronmcp

import "encoding/json"

// ProtocolLatest is the revision this package prefers when a client offers a
// choice or offers nothing.
const ProtocolLatest = "2025-06-18"

// protocolSupported is the set a server will agree to speak. Older revisions
// stay listed because a client pinned to one is common and the differences
// across these three do not reach the four methods implemented here — refusing
// them would break working clients for no behavioural gain.
var protocolSupported = map[string]bool{
	"2025-06-18": true,
	"2025-03-26": true,
	"2024-11-05": true,
}

// ProtocolSupported reports whether a revision is one this package will speak.
func ProtocolSupported(version string) bool { return protocolSupported[version] }

// negotiateProtocol picks the revision to answer with: the client's if we speak
// it, otherwise ours. A mismatch is not an error — the client decides whether
// what it got back is acceptable.
func negotiateProtocol(requested string) string {
	if protocolSupported[requested] {
		return requested
	}
	return ProtocolLatest
}

// JSON-RPC 2.0 error codes used here. The transport-level ones are from the
// specification; tool failures deliberately do not use them (see Result).
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
)

// rpcRequest is an inbound call. ID is kept raw because JSON-RPC permits a
// string or a number and re-encoding it as either would change the value the
// caller has to match against.
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// isNotification reports whether a request expects no reply. JSON-RPC says a
// message without an id is a notification; an explicit null counts, because
// some clients send it rather than omitting the field.
func (r rpcRequest) isNotification() bool {
	return len(r.ID) == 0 || string(r.ID) == "null"
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func newResponse(id json.RawMessage) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: id}
}

func errorResponse(id json.RawMessage, code int, message string) rpcResponse {
	return rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: message}}
}

// Content is one piece of a tool's output. Only text is produced today; the
// type field exists because it is on the wire and a client switches on it.
type Content struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// TextContent wraps a string as the single content block of a result.
func TextContent(text string) []Content { return []Content{{Type: "text", Text: text}} }

// toolResult is the wire shape of tools/call.
//
// The isError field is the protocol's sharpest edge: a tool that fails returns
// a *successful* JSON-RPC response carrying isError true. A client that only
// checks the JSON-RPC error field reads a failed action as a success, which for
// something like a rollback is the worst possible misreading. Client.CallTool
// checks both, and there is a test for exactly that.
type toolResult struct {
	Content []Content `json:"content"`
	IsError bool      `json:"isError,omitempty"`
}
