package neutronmcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// DefaultTimeout is the per-call ceiling when none is given. It is short on
// purpose: a caller doing something slow should say so, because one timeout
// generous enough for the slowest tool lets a hung read hold a request open for
// minutes.
const DefaultTimeout = 30 * time.Second

// maxResponseBody bounds a server's reply. A tool returning logs can be large;
// a server returning gigabytes is broken or hostile, and decoding it either way
// costs the caller its memory.
const maxResponseBody = 8 << 20 // 8 MiB

// Client calls one MCP server.
//
// Safe for concurrent use. The request id is atomic and nothing else is
// mutated after construction, which matters because a single server connection
// is typically shared by every request handler in a process.
type Client struct {
	baseURL  string
	token    string
	http     *http.Client
	protocol string
	nextID   atomic.Int64
}

// ClientOption adjusts a Client at construction.
type ClientOption func(*Client)

// WithHTTPClient supplies the transport, which is how a caller sets a timeout
// suited to what it is calling, or injects a test server's client.
func WithHTTPClient(h *http.Client) ClientOption {
	return func(c *Client) {
		if h != nil {
			c.http = h
		}
	}
}

// WithProtocolVersion pins the revision offered at initialize.
func WithProtocolVersion(version string) ClientOption {
	return func(c *Client) {
		if version != "" {
			c.protocol = version
		}
	}
}

// NewClient builds a client for an MCP endpoint. token may be empty for a
// server that authenticates some other way.
func NewClient(endpoint, token string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL:  strings.TrimRight(endpoint, "/"),
		token:    token,
		http:     &http.Client{Timeout: DefaultTimeout},
		protocol: ProtocolLatest,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ValidateEndpoint checks an MCP endpoint URL, returning the cleaned value or a
// reason it is unusable.
//
// It deliberately does not reject private or loopback addresses. An MCP server
// in a private deployment normally *is* on a tailnet or localhost, and the URL
// is entered by an operator pointing at their own infrastructure rather than
// supplied by an untrusted caller — so the SSRF reasoning that would justify a
// blocklist does not apply, and applying one anyway would reject every correct
// value. A host that needs that guard should apply it before calling this.
func ValidateEndpoint(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("endpoint is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", errors.New("endpoint must be an absolute URL, e.g. http://100.64.0.1:3456/api/mcp")
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return "", errors.New("endpoint scheme must be http or https")
	}
	return strings.TrimRight(raw, "/"), nil
}

// ServerInfo is what a server reports at initialize.
type ServerInfo struct {
	ProtocolVersion string         `json:"protocolVersion"`
	Capabilities    map[string]any `json:"capabilities"`
	Instructions    string         `json:"instructions"`
	ServerInfo      struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"serverInfo"`
}

// ToolError is a tool-level failure: the protocol call succeeded and the tool
// reported that what it was asked to do did not happen.
//
// It is a distinct type because the distinction is load-bearing. A transport
// error means "we do not know whether anything happened" and may be worth
// retrying; a ToolError means "it definitely did not happen, and here is why",
// which usually is not.
type ToolError struct {
	Tool string
	Text string
}

func (e *ToolError) Error() string {
	if e.Text == "" {
		return e.Tool + " failed"
	}
	return e.Tool + ": " + e.Text
}

// IsToolError reports whether err came from a tool rather than the transport.
func IsToolError(err error) bool {
	var te *ToolError
	return errors.As(err, &te)
}

// Initialize performs the handshake and returns what the server says it is.
// Calling it is optional for a server that does not require it, but it is the
// only way to learn the negotiated revision and the instructions.
func (c *Client) Initialize(ctx context.Context) (ServerInfo, error) {
	var info ServerInfo
	raw, err := c.call(ctx, "initialize", map[string]any{
		"protocolVersion": c.protocol,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]string{"name": "neutron-go", "version": ProtocolLatest},
	})
	if err != nil {
		return info, err
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return info, fmt.Errorf("initialize returned an unreadable result: %w", err)
	}
	return info, nil
}

// Ping checks the endpoint is alive and authenticating this client.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.call(ctx, "ping", map[string]any{})
	return err
}

// ListTools returns the tools this client is allowed to see.
func (c *Client) ListTools(ctx context.Context) ([]ToolInfo, error) {
	raw, err := c.call(ctx, "tools/list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var out struct {
		Tools []ToolInfo `json:"tools"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("tools/list returned an unreadable result: %w", err)
	}
	return out.Tools, nil
}

// CallTool invokes a tool and returns its text output.
//
// A tool that reports failure comes back as a *ToolError, not as text. This is
// the one place the protocol invites a serious mistake: a failed tool is a
// successful JSON-RPC response whose result carries isError, so a client that
// checks only the JSON-RPC error field reads a failed deploy as a success.
func (c *Client) CallTool(ctx context.Context, name string, args map[string]any) (string, error) {
	if args == nil {
		args = map[string]any{}
	}
	raw, err := c.call(ctx, "tools/call", map[string]any{"name": name, "arguments": args})
	if err != nil {
		return "", err
	}
	var result toolResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("%s returned an unreadable result: %w", name, err)
	}
	text := joinContent(result.Content)
	if result.IsError {
		return "", &ToolError{Tool: name, Text: text}
	}
	return text, nil
}

func joinContent(content []Content) string {
	parts := make([]string, 0, len(content))
	for _, c := range content {
		if c.Text != "" {
			parts = append(parts, c.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// call sends one JSON-RPC request and returns its raw result.
func (c *Client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      c.nextID.Add(1),
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// Both are sent because servers disagree about which they read: the header
	// is the 2025-06-18 way, the initialize parameter is how earlier revisions
	// negotiated, and offering both costs nothing.
	req.Header.Set("MCP-Protocol-Version", c.protocol)
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling %s: %w", method, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("calling %s: the server rejected this client's credential", method)
	}
	if resp.StatusCode == http.StatusAccepted {
		// The server treated this as a notification. Nothing is coming back.
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("calling %s: server returned %s", method, resp.Status)
	}

	var out struct {
		Error  *rpcError       `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBody)).Decode(&out); err != nil {
		return nil, fmt.Errorf("calling %s: unreadable response: %w", method, err)
	}
	if out.Error != nil {
		return nil, fmt.Errorf("calling %s: %s (code %d)", method, out.Error.Message, out.Error.Code)
	}
	return out.Result, nil
}
