package studio

import (
	"strings"
	"testing"
)

func TestToCamelCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"user_profiles", "UserProfiles"},
		{"id", "Id"},
		{"created_at", "CreatedAt"},
		{"user", "User"},
		{"a_b_c", "ABC"},
		{"hello-world", "HelloWorld"},
		{"hello world", "HelloWorld"},
		{"", ""},
		{"already_CamelCase", "AlreadyCamelCase"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := toCamelCase(tt.input)
			if got != tt.want {
				t.Errorf("toCamelCase(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestResolveDisplayType(t *testing.T) {
	tests := []struct {
		dataType string
		udtName  string
		maxLen   *int
		want     string
	}{
		{"character varying", "", nil, "varchar"},
		{"character varying", "", intPtr(255), "varchar(255)"},
		{"character", "", nil, "char"},
		{"character", "", intPtr(10), "char(10)"},
		{"USER-DEFINED", "citext", nil, "citext"},
		{"ARRAY", "_text", nil, "_text[]"},
		{"integer", "", nil, "integer"},
		{"boolean", "", nil, "boolean"},
		{"text", "", nil, "text"},
	}
	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := resolveDisplayType(tt.dataType, tt.udtName, tt.maxLen)
			if got != tt.want {
				t.Errorf("resolveDisplayType(%q, %q, %v) = %q, want %q",
					tt.dataType, tt.udtName, tt.maxLen, got, tt.want)
			}
		})
	}
}

func intPtr(v int) *int {
	return &v
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"users", `"users"`},
		{"public", `"public"`},
		{`has"quotes`, `"has""quotes"`},
		{"", `""`},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := quoteIdent(tt.input)
			if got != tt.want {
				t.Errorf("quoteIdent(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		s   string
		def int
		want int
	}{
		{"42", 10, 42},
		{"", 10, 10},
		{"abc", 10, 10},
		{"0", 10, 0},
		{"-1", 10, -1},
	}
	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			got := parseInt(tt.s, tt.def)
			if got != tt.want {
				t.Errorf("parseInt(%q, %d) = %d, want %d", tt.s, tt.def, got, tt.want)
			}
		})
	}
}

func TestNormalizeType(t *testing.T) {
	tests := []struct {
		dataType string
		udtName  string
		want     string
	}{
		{"USER-DEFINED", "citext", "citext"},
		{"integer", "", "integer"},
		{"BOOLEAN", "", "boolean"},
		{"USER-DEFINED", "GEOMETRY", "geometry"},
	}
	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := normalizeType(tt.dataType, tt.udtName)
			if got != tt.want {
				t.Errorf("normalizeType(%q, %q) = %q, want %q", tt.dataType, tt.udtName, got, tt.want)
			}
		})
	}
}

func TestPgToGoType(t *testing.T) {
	tests := []struct {
		dataType string
		udtName  string
		nullable bool
		want     string
	}{
		{"integer", "int4", false, "int32"},
		{"integer", "int4", true, "*int32"},
		{"bigint", "int8", false, "int64"},
		{"boolean", "bool", false, "bool"},
		{"boolean", "bool", true, "*bool"},
		{"text", "text", false, "string"},
		{"text", "text", true, "*string"},
		{"timestamp with time zone", "timestamptz", false, "time.Time"},
		{"USER-DEFINED", "jsonb", false, "json.RawMessage"},
		{"USER-DEFINED", "jsonb", true, "json.RawMessage"}, // jsonb nullable stays json.RawMessage
		{"USER-DEFINED", "bytea", false, "[]byte"},
	}
	for _, tt := range tests {
		name := tt.dataType
		if tt.nullable {
			name += "_nullable"
		}
		t.Run(name, func(t *testing.T) {
			got := pgToGoType(tt.dataType, tt.udtName, tt.nullable)
			if got != tt.want {
				t.Errorf("pgToGoType(%q, %q, %v) = %q, want %q",
					tt.dataType, tt.udtName, tt.nullable, got, tt.want)
			}
		})
	}
}

func TestPgToTSType(t *testing.T) {
	tests := []struct {
		dataType string
		udtName  string
		want     string
	}{
		{"integer", "int4", "number"},
		{"bigint", "int8", "number"},
		{"boolean", "bool", "boolean"},
		{"text", "text", "string"},
		{"USER-DEFINED", "jsonb", "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := pgToTSType(tt.dataType, tt.udtName)
			if got != tt.want {
				t.Errorf("pgToTSType(%q, %q) = %q, want %q", tt.dataType, tt.udtName, got, tt.want)
			}
		})
	}
}

func TestPgToRustType(t *testing.T) {
	tests := []struct {
		dataType string
		udtName  string
		nullable bool
		want     string
	}{
		{"integer", "int4", false, "i32"},
		{"integer", "int4", true, "Option<i32>"},
		{"bigint", "int8", false, "i64"},
		{"boolean", "bool", false, "bool"},
		{"text", "text", false, "String"},
	}
	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := pgToRustType(tt.dataType, tt.udtName, tt.nullable)
			if got != tt.want {
				t.Errorf("pgToRustType(%q, %q, %v) = %q, want %q",
					tt.dataType, tt.udtName, tt.nullable, got, tt.want)
			}
		})
	}
}

func TestPgToPythonType(t *testing.T) {
	tests := []struct {
		dataType string
		udtName  string
		nullable bool
		want     string
	}{
		{"integer", "int4", false, "int"},
		{"integer", "int4", true, "Optional[int]"},
		{"boolean", "bool", false, "bool"},
		{"text", "text", false, "str"},
		{"timestamp with time zone", "timestamptz", false, "datetime"},
	}
	for _, tt := range tests {
		t.Run(tt.dataType, func(t *testing.T) {
			got := pgToPythonType(tt.dataType, tt.udtName, tt.nullable)
			if got != tt.want {
				t.Errorf("pgToPythonType(%q, %q, %v) = %q, want %q",
					tt.dataType, tt.udtName, tt.nullable, got, tt.want)
			}
		})
	}
}

func TestGenGo(t *testing.T) {
	cols := []colInfo{
		{name: "id", dataType: "integer", udtName: "int4", nullable: false, isPK: true},
		{name: "name", dataType: "text", udtName: "text", nullable: false},
		{name: "email", dataType: "text", udtName: "text", nullable: true},
	}

	code := genGo("users", cols)
	if !strings.Contains(code, "type Users struct") {
		t.Errorf("genGo missing struct declaration, got:\n%s", code)
	}
	if !strings.Contains(code, "int32") {
		t.Error("genGo missing int32 type for id")
	}
	if !strings.Contains(code, "*string") {
		t.Error("genGo missing *string for nullable email")
	}
	if !strings.Contains(code, `db:"id"`) {
		t.Error("genGo missing db tag")
	}
	if !strings.Contains(code, `json:"id"`) {
		t.Error("genGo missing json tag")
	}
}

func TestGenTypeScript(t *testing.T) {
	cols := []colInfo{
		{name: "id", dataType: "integer", udtName: "int4"},
		{name: "name", dataType: "text", udtName: "text"},
		{name: "bio", dataType: "text", udtName: "text", nullable: true},
	}

	code := genTypeScript("users", cols)
	if !strings.Contains(code, "export interface Users") {
		t.Errorf("genTypeScript missing interface, got:\n%s", code)
	}
	if !strings.Contains(code, "number") {
		t.Error("genTypeScript missing number type")
	}
	if !strings.Contains(code, "bio?") {
		t.Error("genTypeScript missing optional marker for nullable field")
	}
}

func TestGenRust(t *testing.T) {
	cols := []colInfo{
		{name: "id", dataType: "integer", udtName: "int4"},
		{name: "name", dataType: "text", udtName: "text"},
	}

	code := genRust("users", cols)
	if !strings.Contains(code, "pub struct Users") {
		t.Errorf("genRust missing struct, got:\n%s", code)
	}
	if !strings.Contains(code, "#[derive") {
		t.Error("genRust missing derive attribute")
	}
	if !strings.Contains(code, "i32") {
		t.Error("genRust missing i32")
	}
}

func TestGenPython(t *testing.T) {
	cols := []colInfo{
		{name: "id", dataType: "integer", udtName: "int4"},
		{name: "name", dataType: "text", udtName: "text", nullable: true},
	}

	code := genPython("users", cols)
	if !strings.Contains(code, "class Users(BaseModel)") {
		t.Errorf("genPython missing class, got:\n%s", code)
	}
	if !strings.Contains(code, "int") {
		t.Error("genPython missing int type")
	}
	if !strings.Contains(code, "Optional[str]") {
		t.Error("genPython missing Optional for nullable field")
	}
}
