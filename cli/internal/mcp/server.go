// Package mcp implements a Model Context Protocol (MCP) server for Nucleus.
// It exposes all 14 Nucleus data models as MCP tools over stdio (JSON-RPC 2.0)
// or HTTP, allowing any MCP-compatible or OpenAI-compatible AI to query the
// database directly.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// isLocalhostOrigin checks whether an Origin header refers to a localhost address.
func isLocalhostOrigin(origin string) bool {
	return strings.HasPrefix(origin, "http://localhost:") ||
		strings.HasPrefix(origin, "http://127.0.0.1:") ||
		strings.HasPrefix(origin, "http://[::1]:") ||
		origin == "http://localhost" ||
		origin == "http://127.0.0.1"
}

const protocolVersion = "2024-11-05"

// Server is the MCP stdio server.
type Server struct {
	client  *db.Client
	version string
}

// NewServer creates a new MCP server connected to the given database URL.
func NewServer(ctx context.Context, dbURL, version string) (*Server, error) {
	client, err := db.Connect(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return &Server{client: client, version: version}, nil
}

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
		return rpcResponse{Result: map[string]any{"tools": toolList()}}
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
	mux := http.NewServeMux()

	// Optional bearer token auth via NEUTRON_MCP_TOKEN
	mcpToken := os.Getenv("NEUTRON_MCP_TOKEN")

	// CORS middleware wrapping all handlers (localhost origins only + optional auth)
	wrap := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// Restrict CORS to localhost origins only
			origin := r.Header.Get("Origin")
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
		json.NewEncoder(w).Encode(openAIToolDefs())
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
		handler, ok := toolHandlers[body.Name]
		if !ok {
			http.Error(w, "unknown tool: "+body.Name, http.StatusNotFound)
			return
		}
		result, err := handler(r.Context(), s.client, body.Arguments)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"result": result})
	}))

	// Plain REST — GET /tools
	mux.HandleFunc("/tools", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toolList())
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
		handler, ok := toolHandlers[name]
		if !ok {
			http.Error(w, "unknown tool: "+name, http.StatusNotFound)
			return
		}
		var args map[string]any
		if r.ContentLength != 0 {
			if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
				http.Error(w, "invalid JSON body", http.StatusBadRequest)
				return
			}
		}
		result, err := handler(r.Context(), s.client, args)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"result": result})
	}))

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
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

func (s *Server) handleToolCall(ctx context.Context, raw json.RawMessage) rpcResponse {
	var p toolCallParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return rpcResponse{Error: &rpcError{Code: -32602, Message: "invalid params: " + err.Error()}}
	}

	handler, ok := toolHandlers[p.Name]
	if !ok {
		return rpcResponse{Error: &rpcError{Code: -32601, Message: "unknown tool: " + p.Name}}
	}

	text, toolErr := handler(ctx, s.client, p.Arguments)

	isError := toolErr != nil
	content := text
	if isError {
		content = toolErr.Error()
	}

	return rpcResponse{
		Result: map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": content},
			},
			"isError": isError,
		},
	}
}
