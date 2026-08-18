package neutronmcp

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"

	"github.com/neutron-dev/neutron-go/neutron"
)

// maxRequestBody bounds an inbound JSON-RPC message. Tool arguments are small;
// anything approaching this is a mistake or an attack, and an unbounded decode
// on an authenticated endpoint is still a way to spend all of a server's memory.
const maxRequestBody = 1 << 20 // 1 MiB

// Server serves MCP over streamable HTTP as an http.Handler.
//
// It is request/response only: POST carries a JSON-RPC message, GET is refused.
// Statelessness is the point — there is no session to pin a client to a replica,
// so mounting this behind any load balancer works without configuration.
type Server struct {
	// Name and Version identify this server to clients in initialize.
	Name    string
	Version string

	// Instructions is optional prose telling an agent what this server is for.
	// It is worth writing: it is the only place to say what the tools mean
	// together, as opposed to what each one does alone.
	Instructions string

	// Authorize gates every request. A nil Authorizer refuses everything,
	// because the alternative default — allowing everything — would turn a
	// forgotten line into an open remote-execution endpoint.
	Authorize Authorizer

	Logger *slog.Logger

	tools map[string]Tool
	order []string
}

// NewServer builds a server with no tools. Register adds them.
func NewServer(name, version string, authorize Authorizer) *Server {
	return &Server{
		Name:      name,
		Version:   version,
		Authorize: authorize,
		tools:     map[string]Tool{},
	}
}

// Register adds tools. A duplicate name replaces the earlier tool rather than
// silently serving whichever the map iteration reached first.
func (s *Server) Register(tools ...Tool) *Server {
	if s.tools == nil {
		s.tools = map[string]Tool{}
	}
	for _, t := range tools {
		if t.Name == "" || t.Run == nil {
			continue
		}
		if _, seen := s.tools[t.Name]; !seen {
			s.order = append(s.order, t.Name)
		}
		s.tools[t.Name] = t
	}
	return s
}

// Tools returns the registered tools in registration order. Used by hosts that
// want to document or test their own surface.
func (s *Server) Tools() []Tool {
	out := make([]Tool, 0, len(s.order))
	for _, name := range s.order {
		out = append(out, s.tools[name])
	}
	return out
}

func (s *Server) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		// HTTP-layer failure, before any JSON-RPC is spoken, so it carries
		// the framework's RFC 7807 shape rather than a JSON-RPC error object.
		w.Header().Set("Allow", "POST")
		neutron.WriteError(w, r, neutron.ErrMethodNotAllowed("the MCP endpoint accepts POST only"))
		return
	}

	// Authorization comes before the body is read, let alone parsed: an
	// unauthenticated caller should not be able to reach the decoder at all.
	if s.Authorize == nil {
		s.logger().Error("neutronmcp: refusing every request, no Authorizer is set", "server", s.Name)
		neutron.WriteError(w, r, neutron.ErrInternal("server misconfigured"))
		return
	}
	principal, ok := s.Authorize(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Bearer realm=%q", s.Name))
		neutron.WriteError(w, r, neutron.ErrUnauthorized("unauthorized"))
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil || len(body) > maxRequestBody {
		writeRPC(w, errorResponse(nil, codeInvalidRequest, "request body too large"))
		return
	}

	var req rpcRequest
	if json.Unmarshal(body, &req) != nil {
		writeRPC(w, errorResponse(nil, codeParseError, "parse error"))
		return
	}

	// A notification expects no body back. Streamable HTTP wants 202 for these,
	// and answering one with a result confuses clients that are not waiting.
	if req.isNotification() {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	writeRPC(w, s.dispatch(r, req, principal))
}

func (s *Server) dispatch(r *http.Request, req rpcRequest, principal Principal) rpcResponse {
	resp := newResponse(req.ID)

	switch req.Method {
	case "initialize":
		var params struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &params)
		result := map[string]any{
			"protocolVersion": negotiateProtocol(params.ProtocolVersion),
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]string{"name": s.Name, "version": s.Version},
		}
		if s.Instructions != "" {
			result["instructions"] = s.Instructions
		}
		resp.Result = result

	case "ping":
		resp.Result = map[string]any{}

	case "tools/list":
		resp.Result = map[string]any{"tools": s.visibleTools(principal)}

	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if json.Unmarshal(req.Params, &params) != nil {
			return errorResponse(req.ID, codeInvalidParams, "invalid params")
		}
		resp.Result = s.callTool(r, params.Name, params.Arguments, principal)

	default:
		return errorResponse(req.ID, codeMethodNotFound, "method not found: "+req.Method)
	}
	return resp
}

// visibleTools lists what this principal may use. Hiding a tool it could not
// call anyway keeps an agent from planning around something it will be refused.
func (s *Server) visibleTools(principal Principal) []ToolInfo {
	names := append([]string(nil), s.order...)
	if len(names) == 0 {
		// A server registered through struct literals rather than Register has
		// no order; fall back to a stable sort so listings do not shuffle.
		for name := range s.tools {
			names = append(names, name)
		}
		sort.Strings(names)
	}
	out := make([]ToolInfo, 0, len(names))
	for _, name := range names {
		t := s.tools[name]
		if !principal.Allows(t) {
			continue
		}
		out = append(out, ToolInfo{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: t.InputSchema,
			Annotations: &Annotations{ReadOnlyHint: t.ReadOnly, DestructiveHint: t.Destructive},
		})
	}
	return out
}

// callTool runs one tool and always returns a tool result.
//
// Permission is re-checked here and not merely at listing time. Hiding a tool
// is a courtesy to well-behaved clients; it is not access control, because
// nothing stops a caller naming a tool it never saw.
func (s *Server) callTool(r *http.Request, name string, args map[string]any, principal Principal) toolResult {
	t, found := s.tools[name]
	if !found {
		return errorResult("unknown tool: " + name)
	}
	if !principal.Allows(t) {
		s.logger().Warn("neutronmcp: tool refused",
			"server", s.Name, "tool", name, "principal", principal.Name)
		return errorResult(fmt.Sprintf("%q is not permitted for %q", name, principal.Name))
	}

	s.logger().Info("neutronmcp: tool call",
		"server", s.Name, "tool", name, "principal", principal.Name, "destructive", t.Destructive)

	out, err := t.Run(withPrincipal(r.Context(), principal), args)
	if err != nil {
		// A tool that failed is reported in-band. See toolResult for why this
		// must not become a JSON-RPC error.
		return errorResult(err.Error())
	}
	return toolResult{Content: TextContent(out)}
}

func errorResult(message string) toolResult {
	return toolResult{Content: TextContent(message), IsError: true}
}

func writeRPC(w http.ResponseWriter, resp rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
