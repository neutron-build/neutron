package neutronmcp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fixture builds a server with a read tool and a destructive one, plus a client
// wired to it. principal is what the Authorizer will return.
func fixture(t *testing.T, principal Principal, allow bool) (*Client, *httptest.Server, *int) {
	t.Helper()
	calls := 0
	srv := NewServer("test-server", "1.0.0", func(*http.Request) (Principal, bool) {
		return principal, allow
	})
	srv.Instructions = "A server for exercising the protocol."
	srv.Register(
		Tool{
			Name:        "read_thing",
			Description: "Read a thing",
			InputSchema: ObjectSchema(map[string]any{"id": StringProp("which thing")}, "id"),
			ReadOnly:    true,
			Run: func(ctx context.Context, args map[string]any) (string, error) {
				calls++
				if p, ok := PrincipalFrom(ctx); ok {
					return "read by " + p.Name, nil
				}
				return "read", nil
			},
		},
		Tool{
			Name:        "destroy_thing",
			Description: "Destroy a thing",
			Scope:       "destroy",
			Destructive: true,
			Run: func(ctx context.Context, args map[string]any) (string, error) {
				calls++
				return "destroyed", nil
			},
		},
		Tool{
			Name:        "failing_thing",
			Description: "Always fails",
			ReadOnly:    true,
			Run: func(ctx context.Context, args map[string]any) (string, error) {
				calls++
				return "", errors.New("the disk is on fire")
			},
		},
	)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return NewClient(ts.URL, "tok", WithHTTPClient(ts.Client())), ts, &calls
}

func TestHandshakeAndListing(t *testing.T) {
	client, _, _ := fixture(t, Principal{Name: "owner", Scopes: []string{"destroy"}}, true)
	ctx := context.Background()

	info, err := client.Initialize(ctx)
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if info.ServerInfo.Name != "test-server" || info.ServerInfo.Version != "1.0.0" {
		t.Fatalf("server did not identify itself: %+v", info.ServerInfo)
	}
	if info.ProtocolVersion != ProtocolLatest {
		t.Fatalf("expected the latest revision back, got %q", info.ProtocolVersion)
	}
	if info.Instructions == "" {
		t.Fatal("instructions are the only place to say what the tools mean together; they must survive")
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	tools, err := client.ListTools(ctx)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	if len(tools) != 3 {
		t.Fatalf("a fully scoped principal should see every tool, got %d", len(tools))
	}
	// Registration order is the listing order, so a client's view is stable.
	if tools[0].Name != "read_thing" {
		t.Fatalf("listing should follow registration order, got %q first", tools[0].Name)
	}
	if tools[0].InputSchema == nil {
		t.Fatal("a tool with no schema cannot be called; the schema must be advertised")
	}
	if tools[1].Annotations == nil || !tools[1].Annotations.DestructiveHint {
		t.Fatal("destructive tools must say so, or a runtime cannot know to ask a human first")
	}
}

// The isError trap: a failing tool is a *successful* JSON-RPC response. A client
// that reads only the JSON-RPC error field would treat this as a success, which
// for a rollback or a deploy is the worst available misreading.
func TestFailingToolIsNotReadAsSuccess(t *testing.T) {
	client, _, _ := fixture(t, Principal{Name: "owner", Scopes: []string{"destroy"}}, true)

	out, err := client.CallTool(context.Background(), "failing_thing", nil)
	if err == nil {
		t.Fatal("a tool that failed must surface as an error, never as ordinary output")
	}
	if out != "" {
		t.Fatalf("a failed call must not also return text as though it worked, got %q", out)
	}
	if !IsToolError(err) {
		t.Fatalf("a tool failure must be distinguishable from a transport failure, got %T", err)
	}
	if !strings.Contains(err.Error(), "the disk is on fire") {
		t.Fatalf("the tool's reason must reach the caller, got %q", err.Error())
	}
}

func TestToolErrorIsDistinctFromTransportError(t *testing.T) {
	// A transport failure must NOT look like a tool failure: the two mean
	// different things about whether the action happened.
	client := NewClient("http://127.0.0.1:1/nothing-listening", "tok")
	_, err := client.CallTool(context.Background(), "read_thing", nil)
	if err == nil {
		t.Fatal("an unreachable server must be an error")
	}
	if IsToolError(err) {
		t.Fatal("an unreachable server is not a tool failure; conflating them makes a caller retry the wrong things")
	}
}

// Hiding a tool is a courtesy to well-behaved clients. It is not access
// control, because nothing stops a caller naming a tool it never saw.
func TestScopeIsEnforcedOnCallNotOnlyOnListing(t *testing.T) {
	client, _, calls := fixture(t, Principal{Name: "viewer"}, true) // no scopes

	tools, err := client.ListTools(context.Background())
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	for _, tool := range tools {
		if tool.Name == "destroy_thing" {
			t.Fatal("a tool the caller cannot use must not be listed to it")
		}
	}

	// Now name it anyway.
	if _, err := client.CallTool(context.Background(), "destroy_thing", nil); err == nil {
		t.Fatal("an unscoped caller naming a hidden tool must still be refused")
	}
	if *calls != 0 {
		t.Fatalf("the refused tool must not have run, ran %d times", *calls)
	}
}

func TestReadOnlyPrincipalCannotMutate(t *testing.T) {
	client, _, calls := fixture(t,
		Principal{Name: "readonly", Scopes: []string{"destroy"}, ReadOnly: true}, true)

	// The scope is granted, but the principal is read-only: a mutating tool is
	// still refused. Holding a scope is not the same as being allowed to write.
	if _, err := client.CallTool(context.Background(), "destroy_thing", nil); err == nil {
		t.Fatal("a read-only principal must not call a mutating tool even when scoped for it")
	}
	if *calls != 0 {
		t.Fatalf("the refused tool must not have run, ran %d times", *calls)
	}
	if _, err := client.CallTool(context.Background(), "read_thing", map[string]any{"id": "x"}); err != nil {
		t.Fatalf("a read-only principal must still be able to read: %v", err)
	}
}

func TestPrincipalReachesTheTool(t *testing.T) {
	client, _, _ := fixture(t, Principal{Name: "alice", ReadOnly: true}, true)
	out, err := client.CallTool(context.Background(), "read_thing", map[string]any{"id": "x"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if out != "read by alice" {
		t.Fatalf("a tool must be able to see who is calling it, got %q", out)
	}
}

func TestUnauthorizedIsRefusedBeforeAnythingIsParsed(t *testing.T) {
	client, _, calls := fixture(t, Principal{}, false) // Authorizer refuses
	_, err := client.CallTool(context.Background(), "read_thing", nil)
	if err == nil {
		t.Fatal("a rejected credential must fail the call")
	}
	if !strings.Contains(err.Error(), "credential") {
		t.Fatalf("the caller should be told its credential was refused, got %q", err.Error())
	}
	if *calls != 0 {
		t.Fatalf("nothing may run for an unauthorized caller, ran %d times", *calls)
	}
}

// A server with no Authorizer must refuse everything. The alternative default
// would turn one forgotten line into an open remote-execution endpoint.
func TestMissingAuthorizerRefusesEverything(t *testing.T) {
	srv := &Server{Name: "unconfigured", Version: "0"}
	srv.Register(Tool{Name: "t", ReadOnly: true, Run: func(context.Context, map[string]any) (string, error) {
		t.Fatal("a server with no Authorizer must never reach a tool")
		return "", nil
	}})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL, "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("a misconfigured server must fail closed, got %s", resp.Status)
	}
}

func TestGetIsRefusedAndNotificationsAreAccepted(t *testing.T) {
	_, ts, _ := fixture(t, Principal{Name: "owner"}, true)

	resp, err := ts.Client().Get(ts.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("this transport is request/response only; GET must be refused, got %s", resp.Status)
	}
	if resp.Header.Get("Allow") != "POST" {
		t.Fatal("a 405 should say what is allowed")
	}

	// A notification has no id and expects no body back.
	resp, err = ts.Client().Post(ts.URL, "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	if err != nil {
		t.Fatalf("post notification: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("a notification should be accepted with 202, got %s", resp.Status)
	}
}

func TestProtocolNegotiation(t *testing.T) {
	if got := negotiateProtocol("2024-11-05"); got != "2024-11-05" {
		t.Fatalf("a supported revision the client asked for must be honoured, got %q", got)
	}
	if got := negotiateProtocol("1999-01-01"); got != ProtocolLatest {
		t.Fatalf("an unknown revision must fall back to ours, got %q", got)
	}
	if got := negotiateProtocol(""); got != ProtocolLatest {
		t.Fatalf("a client offering nothing must get ours, got %q", got)
	}
	if !ProtocolSupported(ProtocolLatest) {
		t.Fatal("the latest revision must be in the supported set")
	}
}

func TestUnknownMethodAndUnknownTool(t *testing.T) {
	client, ts, _ := fixture(t, Principal{Name: "owner"}, true)

	// An unknown *method* is a protocol error.
	resp, err := ts.Client().Post(ts.URL, "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"resources/list"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	var decoded struct {
		Error *rpcError `json:"error"`
	}
	if json.NewDecoder(resp.Body).Decode(&decoded) != nil || decoded.Error == nil {
		t.Fatal("an unimplemented method must come back as a JSON-RPC error")
	}
	if decoded.Error.Code != codeMethodNotFound {
		t.Fatalf("expected method-not-found, got code %d", decoded.Error.Code)
	}

	// An unknown *tool* is a tool error, not a protocol error: the call was
	// well formed and the server understood it.
	_, err = client.CallTool(context.Background(), "no_such_tool", nil)
	if !IsToolError(err) {
		t.Fatalf("naming a missing tool is a tool failure, got %T (%v)", err, err)
	}
}

func TestBearerAuthorizer(t *testing.T) {
	auth := BearerAuthorizer(func(token string) (Principal, bool) {
		if token == "good" {
			return Principal{Name: "ok"}, true
		}
		return Principal{}, false
	})

	for _, tc := range []struct {
		header string
		want   bool
		why    string
	}{
		{"Bearer good", true, "a valid bearer token must pass"},
		{"Bearer bad", false, "an unknown token must fail"},
		{"Bearer ", false, "an empty token must fail rather than reach verify as \"\""},
		{"good", false, "a token without the scheme must fail"},
		{"Basic good", false, "a different scheme must fail"},
		{"", false, "no header must fail, never be treated as absent-therefore-fine"},
	} {
		r := httptest.NewRequest("POST", "/", nil)
		if tc.header != "" {
			r.Header.Set("Authorization", tc.header)
		}
		if _, ok := auth(r); ok != tc.want {
			t.Errorf("%s (header %q)", tc.why, tc.header)
		}
	}
}

func TestValidateEndpoint(t *testing.T) {
	// A private or loopback address is the normal case for a self-hosted MCP
	// server and must be accepted.
	for _, ok := range []string{"http://localhost:3456/api/mcp", "https://100.64.0.1/api/mcp", "http://10.0.0.5:8080"} {
		if _, err := ValidateEndpoint(ok); err != nil {
			t.Errorf("%q should be accepted: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "   ", "not a url", "ftp://host/x", "/relative/path"} {
		if _, err := ValidateEndpoint(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
	if got, _ := ValidateEndpoint("  http://host/api/mcp/  "); got != "http://host/api/mcp" {
		t.Fatalf("endpoint should be trimmed of space and trailing slash, got %q", got)
	}
}

func TestObjectSchemaHelpers(t *testing.T) {
	s := ObjectSchema(map[string]any{
		"name":  StringProp("a name"),
		"count": IntProp("how many"),
		"force": BoolProp("whether to force"),
	}, "name")
	if s["type"] != "object" {
		t.Fatal("ObjectSchema must produce an object schema")
	}
	req, _ := s["required"].([]string)
	if len(req) != 1 || req[0] != "name" {
		t.Fatalf("required fields must survive, got %v", s["required"])
	}
	if ObjectSchema(nil)["properties"] == nil {
		t.Fatal("a schema with no properties still needs the properties key, or clients see no arguments object")
	}
	if _, hasRequired := ObjectSchema(nil)["required"]; hasRequired {
		t.Fatal("an empty required list must be omitted, not sent as []")
	}
}
