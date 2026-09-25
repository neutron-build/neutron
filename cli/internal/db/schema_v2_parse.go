package db

// Designer-facing text parsing for schema document v2 (S05).
//
// Studio's schema designer describes visual changes as structured edits
// whose NEW values arrive as text (a column type like "varchar(80)", a
// default like "42" or "now()"). The plan endpoint turns those edits into a
// desired schema document and plans it with DiffV2Document — the same diff
// the CLI uses — so these parsers accept exactly the vocabulary the contract
// represents and refuse everything else with a precise message. Parsing is
// therefore deliberately conservative: a spelling outside the representable
// set is an error, never a guess (a "serial" column, for instance, has no
// contract form here because its sequence identity cannot be invented from
// text; the SQL editor remains the honest surface for such DDL).

import (
	"fmt"
	"regexp"
	"strings"
)

// designerTypeSynonyms maps SQL spellings to contract type names. The
// contract names come from introspection (pg type names); the synonyms are
// the ordinary SQL spellings a person types.
var designerTypeSynonyms = map[string]string{
	"smallint": "int2", "int2": "int2",
	"integer": "int4", "int": "int4", "int4": "int4",
	"bigint": "int8", "int8": "int8",
	"real": "float4", "float4": "float4",
	"double precision": "float8", "double": "float8", "float8": "float8",
	"numeric": "numeric", "decimal": "numeric",
	"boolean": "bool", "bool": "bool",
	"text":    "text",
	"varchar": "varchar", "character varying": "varchar",
	"timestamp": "timestamp", "timestamp without time zone": "timestamp",
	"timestamptz": "timestamptz", "timestamp with time zone": "timestamptz",
	"date":  "date",
	"bytea": "bytea",
	"uuid":  "uuid",
	"json":  "json", "jsonb": "jsonb",
	"tsvector": "tsvector",
	"vector":   "vector",
}

var typeParamRe = regexp.MustCompile(`^(.*?)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)$`)

// ParseV2TypeText parses a user-supplied column type into its contract form.
// Accepts the representable base types (with varchar length, numeric
// precision[,scale], timestamp precision and vector dimensions parameters)
// and one trailing [] array marker. Rejects everything else — serial,
// char, time, interval and unknown names — with a message naming the
// accepted vocabulary.
func ParseV2TypeText(text string) (V2ColumnType, error) {
	s := strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
	if s == "" {
		return V2ColumnType{}, fmt.Errorf("column type is empty")
	}
	array := false
	if strings.HasSuffix(s, "[]") {
		array = true
		s = strings.TrimSpace(strings.TrimSuffix(s, "[]"))
	}
	name := strings.ToLower(s)
	var params map[string]int64
	if m := typeParamRe.FindStringSubmatch(name); m != nil {
		name = strings.TrimSpace(m[1])
		var a, b int64
		if _, err := fmt.Sscanf(m[2], "%d", &a); err != nil {
			return V2ColumnType{}, fmt.Errorf("column type %q has an unparseable parameter", text)
		}
		if m[3] != "" {
			if _, err := fmt.Sscanf(m[3], "%d", &b); err != nil {
				return V2ColumnType{}, fmt.Errorf("column type %q has an unparseable parameter", text)
			}
		}
		switch designerTypeSynonyms[name] {
		case "varchar":
			if m[3] != "" || a <= 0 {
				return V2ColumnType{}, fmt.Errorf("varchar takes a single positive length: %q", text)
			}
			params = map[string]int64{"length": a}
		case "numeric":
			if m[3] == "" {
				return V2ColumnType{}, fmt.Errorf("numeric takes precision and scale: %q (e.g. numeric(12,4)); use plain \"numeric\" without parentheses for unconstrained", text)
			}
			if a < 1 || b < 0 || a > 1000 || b > a {
				return V2ColumnType{}, fmt.Errorf("numeric precision/scale out of range: %q", text)
			}
			params = map[string]int64{"precision": a, "scale": b}
		case "timestamp", "timestamptz":
			if m[3] != "" || a < 0 || a > 6 {
				return V2ColumnType{}, fmt.Errorf("timestamp precision is a single value 0-6: %q", text)
			}
			params = map[string]int64{"precision": a}
		case "vector":
			if m[3] != "" || a <= 0 || a > 2000 {
				return V2ColumnType{}, fmt.Errorf("vector takes a single positive dimension count: %q", text)
			}
			params = map[string]int64{"dimensions": a}
		default:
			return V2ColumnType{}, fmt.Errorf("type %q accepts no parameters (contract type %q)", text, name)
		}
	}
	base, ok := designerTypeSynonyms[name]
	if !ok {
		return V2ColumnType{}, fmt.Errorf(
			"column type %q is not representable in schema document v2; the designer supports %s (plus one trailing [] for arrays) — other types belong in the SQL editor, where the migration planner never guesses",
			text, "smallint, integer, bigint, real, double precision, numeric(p,s), boolean, text, varchar(n), timestamp(p), timestamptz(p), date, bytea, uuid, json, jsonb, vector(n), tsvector")
	}
	codec := v2TypeCodecs[base]
	if array {
		codec = "array"
	}
	return V2ColumnType{Name: base, Params: params, Array: array, Codec: codec}, nil
}

// V2DefaultFromText classifies a user-supplied column default into the
// contract's tagged shape using the SAME rules introspection applies to the
// catalog's deparsed defaults: a single literal token becomes kind literal,
// anything else that survives SQL-text validation becomes kind expression.
// Identity and sequence defaults cannot be invented from text and are
// refused.
func V2DefaultFromText(text string) (*V2ColumnDefault, error) {
	s := strings.TrimSpace(text)
	if s == "" {
		return nil, fmt.Errorf("default text is empty")
	}
	compacted := compactPGCasts(s)
	if nextvalDefaultRe.MatchString(compacted) {
		return nil, fmt.Errorf("default %q references a sequence, which the designer cannot invent from text; sequence-backed columns are created in the SQL editor", s)
	}
	if v2LiteralRegexp.MatchString(compacted) {
		return &V2ColumnDefault{Kind: "literal", SQL: strPtrV2(compacted)}, nil
	}
	if err := v2CheckSQLText(compacted, "default", true); err != nil {
		return nil, fmt.Errorf("default %q is not representable (%v)", s, err)
	}
	return &V2ColumnDefault{Kind: "expression", SQL: strPtrV2(compacted)}, nil
}

// RenderV2TypeDDL renders a contract column type as its PostgreSQL DDL
// spelling — the same rendering the diff planner emits (S05: the Studio
// object detail shows exactly what the planner would write).
func RenderV2TypeDDL(t V2ColumnType) (string, error) {
	return v2TypeDDL(t)
}

// IsProtectedTableName reports whether a table name is neutron-internal
// metadata (excluded from every diff, plan and Studio schema operation).
func IsProtectedTableName(name string) bool {
	return isProtectedTableName(name)
}
