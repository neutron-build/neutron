package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

// readOnlyToolCount is the number of tools a default (read-only) server
// offers; --allow-writes adds execute_sql.
const readOnlyToolCount = 24

func TestToolList(t *testing.T) {
	if n := len(toolList(false)); n != readOnlyToolCount {
		t.Errorf("toolList(false) returned %d tools, want %d", n, readOnlyToolCount)
	}
	if n := len(toolList(true)); n != readOnlyToolCount+1 {
		t.Errorf("toolList(true) returned %d tools, want %d", n, readOnlyToolCount+1)
	}
}

func TestToolListNames(t *testing.T) {
	expectedNames := []string{
		"list_tables", "describe_table", "list_nucleus_models",
		"query_sql", "kv_get", "kv_scan", "fts_search",
		"vector_search", "cypher_query", "doc_find",
		"ts_range", "geo_distance", "blob_list",
		"stream_range", "datalog_query", "cdc_changes",
		"pubsub_list", "search_docs", "get_doc",
		"engine_limits", "inspect_table", "migration_status",
		"explain_sql", "plan_schema_changes",
	}
	nameSet := make(map[string]bool)
	for _, t := range toolList(false) {
		nameSet[t.Name] = true
	}
	for _, name := range expectedNames {
		if !nameSet[name] {
			t.Errorf("toolList() missing tool %q", name)
		}
	}
	if nameSet["execute_sql"] {
		t.Error("a read-only server lists execute_sql")
	}
	withWrites := map[string]bool{}
	for _, t := range toolList(true) {
		withWrites[t.Name] = true
	}
	if !withWrites["execute_sql"] {
		t.Error("--allow-writes server does not list execute_sql")
	}
}

func TestToolListHasDescriptions(t *testing.T) {
	for _, tool := range toolList(true) {
		if tool.Description == "" {
			t.Errorf("tool %q has empty description", tool.Name)
		}
	}
}

func TestToolListHasInputSchemas(t *testing.T) {
	for _, tool := range toolList(true) {
		if tool.InputSchema == nil {
			t.Errorf("tool %q has nil inputSchema", tool.Name)
		}
		schemaType, _ := tool.InputSchema["type"].(string)
		if schemaType != "object" {
			t.Errorf("tool %q schema type = %q, want %q", tool.Name, schemaType, "object")
		}
	}
}

// Only the explicit write tool may drop the read-only hint.
func TestToolAnnotations(t *testing.T) {
	for _, spec := range toolSpecs() {
		if spec.handler == nil {
			t.Errorf("tool %q has no handler", spec.def.Name)
		}
		ro, _ := spec.def.Annotations["readOnlyHint"].(bool)
		destructive, _ := spec.def.Annotations["destructiveHint"].(bool)
		if spec.access == accessWrite {
			if ro || !destructive {
				t.Errorf("write tool %q annotated readOnly=%v destructive=%v", spec.def.Name, ro, destructive)
			}
			if !strings.HasPrefix(spec.def.Description, "WRITE:") {
				t.Errorf("write tool %q description does not lead with WRITE:", spec.def.Name)
			}
			continue
		}
		if !ro || destructive {
			t.Errorf("tool %q annotated readOnly=%v destructive=%v", spec.def.Name, ro, destructive)
		}
	}
}

func TestDumpSchemaOpenAI(t *testing.T) {
	out, err := DumpSchema("openai", false)
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

	if len(result) != readOnlyToolCount {
		t.Errorf("OpenAI schema has %d tools, want %d", len(result), readOnlyToolCount)
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
	out, err := DumpSchema("mcp", false)
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
	out, err := DumpSchema("markdown", false)
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
	_, err := DumpSchema("invalid", false)
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
}

func TestIntArg(t *testing.T) {
	tests := []struct {
		name string
		args map[string]any
		key  string
		def  int
		want int
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
	defs := openAIToolDefs(false)
	if len(defs) != readOnlyToolCount {
		t.Errorf("openAIToolDefs() returned %d defs, want %d", len(defs), readOnlyToolCount)
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
