// Package neutronmcp_test verifies the exported in-process dispatch surface
// (NA-11) from outside the package: a private helper could accidentally
// satisfy the same assertions, so the API is exercised as a downstream
// application would use it.
package neutronmcp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/neutron-build/neutron/go/neutronmcp"
)

func counterTool(name string, calls *int) neutronmcp.Tool {
	return neutronmcp.Tool{
		Name:        name,
		Description: "counting tool",
		Run: func(ctx context.Context, args map[string]any) (string, error) {
			*calls++
			p, ok := neutronmcp.PrincipalFrom(ctx)
			if !ok {
				return "no principal", nil
			}
			return "principal=" + p.Name, nil
		},
	}
}

func TestCallToolExportsTheDispatchPath(t *testing.T) {
	calls := 0
	srv := neutronmcp.NewServer("test", "1.0", nil)
	srv.Register(counterTool("count", &calls))

	// Successful invocation, with the principal attached to the handler's
	// context exactly as it is over HTTP.
	res := srv.CallTool(context.Background(), "count", nil, neutronmcp.Principal{Name: "app-user"})
	if res.IsError {
		t.Fatalf("in-process call errored: %+v", res)
	}
	if len(res.Content) != 1 || res.Content[0].Text != "principal=app-user" {
		t.Errorf("content = %+v, want principal=app-user", res.Content)
	}
	if calls != 1 {
		t.Errorf("handler ran %d times, want 1", calls)
	}

	// Unknown tool is an in-band error result, not a panic.
	if res := srv.CallTool(context.Background(), "nope", nil, neutronmcp.Principal{Name: "u"}); !res.IsError {
		t.Errorf("unknown tool: IsError = false, want true")
	}

	// A canceled context refuses without calling the handler.
	before := calls
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if res := srv.CallTool(canceled, "count", nil, neutronmcp.Principal{Name: "u"}); !res.IsError {
		t.Error("canceled context: IsError = false, want true")
	}
	if calls != before {
		t.Errorf("canceled context still invoked the handler (%d -> %d)", before, calls)
	}

	// A nil context is refused rather than panicking.
	if res := srv.CallTool(nil, "count", nil, neutronmcp.Principal{Name: "u"}); !res.IsError {
		t.Error("nil context: IsError = false, want true")
	}
}

func TestCallToolEnforcesPermissions(t *testing.T) {
	calls := 0
	scoped := counterTool("scoped", &calls)
	scoped.Scope = "ops:run"
	destructive := counterTool("drop", &calls)
	destructive.ReadOnly = false

	srv := neutronmcp.NewServer("test", "1.0", nil)
	srv.Register(scoped, destructive)

	// Missing scope.
	if res := srv.CallTool(context.Background(), "scoped", nil, neutronmcp.Principal{Name: "u"}); !res.IsError {
		t.Error("missing scope: IsError = false, want true")
	}
	if calls != 0 {
		t.Errorf("refused call still ran the handler (%d calls)", calls)
	}

	// Granted scope works.
	if res := srv.CallTool(context.Background(), "scoped", nil, neutronmcp.Principal{Name: "u", Scopes: []string{"ops:run"}}); res.IsError {
		t.Errorf("granted scope refused: %+v", res)
	}

	// A read-only principal may not run a destructive tool.
	if res := srv.CallTool(context.Background(), "drop", nil, neutronmcp.Principal{Name: "viewer", ReadOnly: true}); !res.IsError {
		t.Error("read-only principal ran a destructive tool")
	}
}

func TestCallToolReportsToolErrorsInBand(t *testing.T) {
	srv := neutronmcp.NewServer("test", "1.0", nil)
	srv.Register(neutronmcp.Tool{
		Name: "boom",
		Run: func(ctx context.Context, args map[string]any) (string, error) {
			return "", context.DeadlineExceeded
		},
	})
	res := srv.CallTool(context.Background(), "boom", nil, neutronmcp.Principal{Name: "u"})
	if !res.IsError {
		t.Fatal("tool error: IsError = false, want true (in-band failure)")
	}
	if res.Content[0].Text != context.DeadlineExceeded.Error() {
		t.Errorf("error text = %q", res.Content[0].Text)
	}
}

// The two transports must not drift: the same call over HTTP and in process
// returns the same result (permission refusal shape included).
func TestCallToolMatchesHTTPDispatch(t *testing.T) {
	calls := 0
	tool := counterTool("count", &calls)
	tool.Scope = "ops:run"
	srv := neutronmcp.NewServer("test", "1.0", func(r *http.Request) (neutronmcp.Principal, bool) {
		return neutronmcp.Principal{Name: "http-user", Scopes: []string{"ops:run"}}, true
	})
	srv.Register(tool)

	w := httptest.NewRecorder()
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"count","arguments":{}}}`
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/mcp", strReader(body)))

	if w.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d", w.Code)
	}
	var envelope struct {
		Result neutronmcp.ToolResult `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode HTTP response: %v", err)
	}

	direct := srv.CallTool(context.Background(), "count", nil, neutronmcp.Principal{Name: "http-user", Scopes: []string{"ops:run"}})
	if direct.IsError != envelope.Result.IsError || len(direct.Content) != len(envelope.Result.Content) {
		t.Errorf("transports drifted: http=%+v in-process=%+v", envelope.Result, direct)
	}
	if len(direct.Content) == 1 && envelope.Result.Content[0].Text != direct.Content[0].Text {
		t.Errorf("content differs: http=%q in-process=%q", envelope.Result.Content[0].Text, direct.Content[0].Text)
	}

	// The refusal shape also matches.
	w2 := httptest.NewRecorder()
	srv2 := neutronmcp.NewServer("test", "1.0", func(r *http.Request) (neutronmcp.Principal, bool) {
		return neutronmcp.Principal{Name: "http-user"}, true // no scopes
	})
	srv2.Register(tool)
	srv2.ServeHTTP(w2, httptest.NewRequest(http.MethodPost, "/mcp", strReader(body)))
	var env2 struct {
		Result neutronmcp.ToolResult `json:"result"`
	}
	_ = json.Unmarshal(w2.Body.Bytes(), &env2)
	directRefusal := srv2.CallTool(context.Background(), "count", nil, neutronmcp.Principal{Name: "http-user"})
	if directRefusal.IsError != env2.Result.IsError || directRefusal.Content[0].Text != env2.Result.Content[0].Text {
		t.Errorf("refusal drifted: http=%+v in-process=%+v", env2.Result, directRefusal)
	}
}

func strReader(s string) *strings.Reader { return strings.NewReader(s) }
