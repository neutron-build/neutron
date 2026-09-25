// Package mcp implements a Model Context Protocol (MCP) server for Nucleus.
// It exposes all 14 Nucleus data models as MCP tools over stdio (JSON-RPC 2.0)
// or HTTP, allowing any MCP-compatible or OpenAI-compatible AI to query the
// database directly.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/inspect"
)

// isLocalhostOrigin checks whether an Origin header refers to a localhost address.
func isLocalhostOrigin(origin string) bool {
	return strings.HasPrefix(origin, "http://localhost:") ||
		strings.HasPrefix(origin, "http://127.0.0.1:") ||
		strings.HasPrefix(origin, "http://[::1]:") ||
		origin == "http://localhost" ||
		origin == "http://127.0.0.1"
}

// allowedHost reports whether a request's Host header names this server in
// a form DNS rebinding cannot forge: an IP literal, localhost, or the
// configured bind host. A rebinding page reaches the listener under its own
// domain name, which is none of these.
func allowedHost(hostHeader, bindHost string) bool {
	h := hostHeader
	if hh, _, err := net.SplitHostPort(hostHeader); err == nil {
		h = hh
	}
	h = strings.TrimSuffix(strings.TrimPrefix(h, "["), "]")
	switch {
	case h == "":
		return false
	case strings.EqualFold(h, "localhost"), net.ParseIP(h) != nil:
		return true
	}
	return bindHost != "" && net.ParseIP(bindHost) == nil && strings.EqualFold(h, bindHost)
}

// allowedOrigin reports whether a browser Origin may call this server: a
// localhost origin, or the same origin the request is addressed to (a
// Host that already passed allowedHost).
func allowedOrigin(origin, hostHeader string) bool {
	if origin == "" || isLocalhostOrigin(origin) {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && strings.EqualFold(u.Host, hostHeader)
}

const protocolVersion = "2024-11-05"

// Server is the MCP server.
type Server struct {
	client  *db.Client
	version string
	env     *toolEnv
}

// Options are the operator's settings. The zero value is the default:
// read-only, redaction on.
type Options struct {
	// AllowWrites offers the write tool (execute_sql). Off by default.
	AllowWrites bool
	// NoRedact disables value redaction under secret-looking names.
	NoRedact bool
	// MigrationsDir is the application's migrations directory.
	MigrationsDir string
}

// NewServer creates a new MCP server connected to the given database URL
// and identifies the engine behind it.
func NewServer(ctx context.Context, dbURL, version string) (*Server, error) {
	client, err := db.Connect(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %s", inspect.RedactText(err.Error()))
	}
	engine, err := inspect.DetectEngine(ctx, client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("identify engine: %s", inspect.RedactText(err.Error()))
	}
	s := &Server{client: client, version: version}
	s.env = &toolEnv{client: client, engine: engine, redactor: inspect.Redactor{Enabled: true}, migrationsDir: "migrations"}
	return s, nil
}

// Configure applies the operator's options.
func (s *Server) Configure(o Options) {
	s.env.allowWrites = o.AllowWrites
	s.env.redactor = inspect.Redactor{Enabled: !o.NoRedact}
	if o.MigrationsDir != "" {
		s.env.migrationsDir = o.MigrationsDir
	}
}

// Engine reports the identified engine.
func (s *Server) Engine() inspect.Engine { return s.env.engine }

// ToolCount is the number of tools this server offers.
func (s *Server) ToolCount() int { return len(toolList(s.env.allowWrites)) }

// Close releases database resources.
func (s *Server) Close() {
	s.client.Close()
}

// rpcRequest is an incoming JSON-RPC 2.0 message.
type rpcRequest struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Method  string           `json:"method"`
	Params  json.RawMessage  `json:"params,omitempty"`
}

// rpcResponse is an outgoing JSON-RPC 2.0 message.
type rpcResponse struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Result  any              `json:"result,omitempty"`
	Error   *rpcError        `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Run reads JSON-RPC messages from stdin and writes responses to stdout.
// It blocks until ctx is cancelled or stdin is closed.
func (s *Server) Run(ctx context.Context) {
	enc := json.NewEncoder(os.Stdout)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024) // 4MB per message

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req rpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			log.Printf("mcp: invalid JSON: %v", err)
			continue
		}

		// Notifications (no id) — don't respond
		if req.ID == nil {
			continue
		}

		resp := s.dispatch(ctx, &req)
		resp.JSONRPC = "2.0"
		resp.ID = req.ID

		if err := enc.Encode(resp); err != nil {
			log.Printf("mcp: write error: %v", err)
			return
		}
	}
}

func (s *Server) dispatch(ctx context.Context, req *rpcRequest) rpcResponse {
	switch req.Method {
	case "initialize":
		return s.handleInitialize()
	case "tools/list":
		return rpcResponse{Result: map[string]any{"tools": toolList(s.env.allowWrites)}}
	case "tools/call":
		return s.handleToolCall(ctx, req.Params)
	case "ping":
		return rpcResponse{Result: map[string]any{}}
	default:
		return rpcResponse{Error: &rpcError{Code: -32601, Message: "method not found: " + req.Method}}
	}
}

func (s *Server) handleInitialize() rpcResponse {
	return rpcResponse{
		Result: map[string]any{
			"protocolVersion": protocolVersion,
			"capabilities": map[string]any{
				"tools": map[string]any{},
			},
			"serverInfo": map[string]any{
				"name":    "nucleus-mcp",
				"version": s.version,
			},
		},
	}
}

type toolCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// RunHTTP starts an HTTP server on addr (e.g. ":7700") exposing three API surfaces:
//
//   POST /mcp              — MCP over HTTP (JSON-RPC 2.0, same protocol as stdio)
//   GET  /openai/tools     — OpenAI function definitions (paste into any OpenAI SDK call)
//   POST /openai/tools/call — OpenAI-compatible tool execution
//   GET  /tools            — plain JSON tool list (generic REST)
//   POST /tools/{name}     — plain REST tool call with JSON body arguments
func (s *Server) RunHTTP(ctx context.Context, addr string) error {
	handler, err := s.httpHandler(addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	log.Printf("mcp: HTTP server listening on %s", addr)

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	case err := <-errCh:
		return err
	}
}

// httpHandler builds the HTTP surfaces for a listener bound to addr.
func (s *Server) httpHandler(addr string) (http.Handler, error) {
	mux := http.NewServeMux()

	// Optional bearer token auth via NEUTRON_MCP_TOKEN. Write tools over
	// HTTP are only offered behind it: an unauthenticated network listener
	// must never be a mutation path.
	mcpToken := os.Getenv("NEUTRON_MCP_TOKEN")
	if s.env.allowWrites && mcpToken == "" {
		return nil, fmt.Errorf("--allow-writes over the HTTP transport requires NEUTRON_MCP_TOKEN (bearer authentication)")
	}

	bindHost, _, _ := net.SplitHostPort(addr)

	// Middleware wrapping all handlers: Host/Origin checks against DNS
	// rebinding, CORS for localhost origins only, optional auth.
	wrap := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// The unauthenticated listener answers only requests addressed
			// to it by IP, localhost or its bind name, and only browser
			// origins that are localhost or that same address.
			if !allowedHost(r.Host, bindHost) {
				http.Error(w, "Forbidden: Host must be localhost, an IP address or the --host name", http.StatusForbidden)
				return
			}
			origin := r.Header.Get("Origin")
			if !allowedOrigin(origin, r.Host) {
				http.Error(w, "Forbidden: cross-origin request", http.StatusForbidden)
				return
			}
			if origin == "" || isLocalhostOrigin(origin) {
				if origin != "" {
					w.Header().Set("Access-Control-Allow-Origin", origin)
				}
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			// Enforce bearer token if NEUTRON_MCP_TOKEN is set
			if mcpToken != "" {
				authHeader := r.Header.Get("Authorization")
				if authHeader != "Bearer "+mcpToken {
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
			}
			h(w, r)
		}
	}

	// MCP over HTTP — same JSON-RPC dispatch as stdio
	mux.HandleFunc("/mcp", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		id := json.RawMessage(`null`)
		if req.ID != nil {
			id = *req.ID
		}
		resp := s.dispatch(r.Context(), &req)
		resp.JSONRPC = "2.0"
		raw := id
		resp.ID = &raw
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))

	// OpenAI function definitions
	mux.HandleFunc("/openai/tools", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(openAIToolDefs(s.env.allowWrites))
	}))

	// OpenAI-compatible tool call: {"name":"query_sql","arguments":{"sql":"SELECT 1"}}
	mux.HandleFunc("/openai/tools/call", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		s.writeRESTResult(w, r.Context(), body.Name, body.Arguments)
	}))

	// Plain REST — GET /tools
	mux.HandleFunc("/tools", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toolList(s.env.allowWrites))
	}))

	// Plain REST — POST /tools/{name}  body = JSON arguments object
	mux.HandleFunc("/tools/", wrap(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/tools/")
		if name == "" {
			http.Error(w, "tool name required", http.StatusBadRequest)
			return
		}
		var args map[string]any
		if r.ContentLength != 0 {
			if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
				http.Error(w, "invalid JSON body", http.StatusBadRequest)
				return
			}
		}
		s.writeRESTResult(w, r.Context(), name, args)
	}))

	return mux, nil
}

// writeRESTResult answers the plain REST and OpenAI-compatible surfaces:
// "result" is the data as JSON text (the pre-X06 shape), "structured" the
// full envelope with engine, access, limits and redactions.
func (s *Server) writeRESTResult(w http.ResponseWriter, ctx context.Context, name string, args map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	env, err := callTool(ctx, s.env, name, args)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errUnknownTool) {
			status = http.StatusNotFound
		}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"result": dataText(env.Data), "structured": env})
}

func dataText(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

func (s *Server) handleToolCall(ctx context.Context, raw json.RawMessage) rpcResponse {
	var p toolCallParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return rpcResponse{Error: &rpcError{Code: -32602, Message: "invalid params: " + err.Error()}}
	}

	env, toolErr := callTool(ctx, s.env, p.Name, p.Arguments)
	if errors.Is(toolErr, errUnknownTool) {
		return rpcResponse{Error: &rpcError{Code: -32601, Message: toolErr.Error()}}
	}
	if toolErr != nil {
		return rpcResponse{
			Result: map[string]any{
				"content": []map[string]any{{"type": "text", "text": toolErr.Error()}},
				"isError": true,
			},
		}
	}
	// The text content carries the whole envelope so clients without
	// structuredContent support still see access, limits and redactions.
	return rpcResponse{
		Result: map[string]any{
			"content":           []map[string]any{{"type": "text", "text": dataText(env)}},
			"structuredContent": env,
			"isError":           false,
		},
	}
}
