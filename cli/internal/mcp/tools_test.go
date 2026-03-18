package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestToolList(t *testing.T) {
	tools := toolList()
	if len(tools) == 0 {
		t.Fatal("toolList() returned empty")
	}

	// Should have exactly 17 tools
	if len(tools) != 17 {
		t.Errorf("toolList() returned %d tools, want 17", len(tools))
	}
}

func TestToolListNames(t *testing.T) {
	expectedNames := []string{
		"list_tables", "describe_table", "list_nucleus_models",
		"query_sql", "kv_get", "kv_scan", "fts_search",
		"vector_search", "cypher_query", "doc_find",
		"ts_range", "geo_radius", "blob_list",
		"stream_range", "datalog_eval", "cdc_changes",
		"pubsub_list",
	}

	tools := toolList()
	nameSet := make(map[string]bool)
	for _, t := range tools {
		nameSet[t.Name] = true
	}

	for _, name := range expectedNames {
		if !nameSet[name] {
			t.Errorf("toolList() missing tool %q", name)
		}
	}
}

func TestToolListHasDescriptions(t *testing.T) {
	for _, tool := range toolList() {
		if tool.Description == "" {
			t.Errorf("tool %q has empty description", tool.Name)
		}
	}
}

func TestToolListHasInputSchemas(t *testing.T) {
	for _, tool := range toolList() {
		if tool.InputSchema == nil {
			t.Errorf("tool %q has nil inputSchema", tool.Name)
		}
		schemaType, _ := tool.InputSchema["type"].(string)
		if schemaType != "object" {
			t.Errorf("tool %q schema type = %q, want %q", tool.Name, schemaType, "object")
		}
	}
}

func TestToolHandlersRegistered(t *testing.T) {
	tools := toolList()
	for _, tool := range tools {
		if _, ok := toolHandlers[tool.Name]; !ok {
			t.Errorf("tool %q has no handler registered", tool.Name)
		}
	}
}

func TestToolHandlerCount(t *testing.T) {
	if len(toolHandlers) != 17 {
		t.Errorf("toolHandlers has %d entries, want 17", len(toolHandlers))
	}
}

func TestDumpSchemaOpenAI(t *testing.T) {
	out, err := DumpSchema("openai")
	if err != nil {
		t.Fatalf("DumpSchema(openai) error: %v", err)
	}
	if out == "" {
		t.Fatal("DumpSchema(openai) returned empty string")
	}

	// Should be valid JSON
	var result []map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("DumpSchema(openai) returned invalid JSON: %v", err)
	}

	if len(result) != 17 {
		t.Errorf("OpenAI schema has %d tools, want 17", len(result))
	}

	// Each entry should have type=function
	for i, tool := range result {
		if tool["type"] != "function" {
			t.Errorf("tool[%d] type = %v, want function", i, tool["type"])
		}
		fn, ok := tool["function"].(map[string]any)
		if !ok {
			t.Errorf("tool[%d] missing function object", i)
			continue
		}
		if fn["name"] == nil || fn["name"] == "" {
			t.Errorf("tool[%d] function.name is empty", i)
		}
	}
}

func TestDumpSchemaMCP(t *testing.T) {
	out, err := DumpSchema("mcp")
	if err != nil {
		t.Fatalf("DumpSchema(mcp) error: %v", err)
	}
	if out == "" {
		t.Fatal("DumpSchema(mcp) returned empty string")
	}

	// Should be valid JSON with a "tools" key
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("DumpSchema(mcp) returned invalid JSON: %v", err)
	}
	if _, ok := result["tools"]; !ok {
		t.Error("DumpSchema(mcp) missing 'tools' key")
	}
}

func TestDumpSchemaMarkdown(t *testing.T) {
	out, err := DumpSchema("markdown")
	if err != nil {
		t.Fatalf("DumpSchema(markdown) error: %v", err)
	}
	if out == "" {
		t.Fatal("DumpSchema(markdown) returned empty string")
	}
	if !strings.Contains(out, "# Nucleus MCP Tools") {
		t.Error("markdown output missing header")
	}
	if !strings.Contains(out, "query_sql") {
		t.Error("markdown output missing query_sql tool")
	}
}

func TestDumpSchemaInvalidFormat(t *testing.T) {
	_, err := DumpSchema("invalid")
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
}

func TestIntArg(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]any
		key     string
		def     int
		want    int
	}{
		{"float64 value", map[string]any{"limit": float64(42)}, "limit", 10, 42},
		{"int value", map[string]any{"limit": 42}, "limit", 10, 42},
		{"int64 value", map[string]any{"limit": int64(42)}, "limit", 10, 42},
		{"missing key", map[string]any{}, "limit", 10, 10},
		{"nil args", nil, "limit", 10, 10},
		{"wrong type", map[string]any{"limit": "not a number"}, "limit", 10, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := intArg(tt.args, tt.key, tt.def)
			if got != tt.want {
				t.Errorf("intArg() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSchemaHelper(t *testing.T) {
	s := schema(props{
		"name": strProp("User name"),
		"age":  numProp("User age"),
	}, []string{"name"})

	if s["type"] != "object" {
		t.Errorf("schema type = %v, want object", s["type"])
	}

	required, ok := s["required"].([]string)
	if !ok {
		t.Fatal("required is not []string")
	}
	if len(required) != 1 || required[0] != "name" {
		t.Errorf("required = %v, want [name]", required)
	}

	props, ok := s["properties"].(map[string]any)
	if !ok {
		t.Fatal("properties is not map[string]any")
	}
	if _, ok := props["name"]; !ok {
		t.Error("properties missing 'name'")
	}
	if _, ok := props["age"]; !ok {
		t.Error("properties missing 'age'")
	}
}

func TestSchemaNoRequired(t *testing.T) {
	s := schema(props{}, nil)
	if _, ok := s["required"]; ok {
		t.Error("schema with nil required should not have required key")
	}
}

func TestStrProp(t *testing.T) {
	p := strProp("test description")
	if p["type"] != "string" {
		t.Errorf("type = %v, want string", p["type"])
	}
	if p["description"] != "test description" {
		t.Errorf("description = %v", p["description"])
	}
}

func TestNumProp(t *testing.T) {
	p := numProp("test number")
	if p["type"] != "number" {
		t.Errorf("type = %v, want number", p["type"])
	}
}

func TestBoolProp(t *testing.T) {
	p := boolProp("test boolean")
	if p["type"] != "boolean" {
		t.Errorf("type = %v, want boolean", p["type"])
	}
}

func TestOpenAIToolDefs(t *testing.T) {
	defs := openAIToolDefs()
	if len(defs) != 17 {
		t.Errorf("openAIToolDefs() returned %d defs, want 17", len(defs))
	}

	for _, def := range defs {
		if def["type"] != "function" {
			t.Errorf("tool type = %v, want function", def["type"])
		}
		fn, ok := def["function"].(map[string]any)
		if !ok {
			t.Error("function field missing")
			continue
		}
		if fn["name"] == nil {
			t.Error("function.name is nil")
		}
		if fn["description"] == nil {
			t.Error("function.description is nil")
		}
		if fn["parameters"] == nil {
			t.Error("function.parameters is nil")
		}
	}
}

func TestProtocolVersion(t *testing.T) {
	if protocolVersion == "" {
		t.Error("protocolVersion is empty")
	}
}
