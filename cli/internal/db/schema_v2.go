package db

// Schema document v2 — the cross-language schema contract.
//
// contracts/data/schema-v2.json is the normative shape; this file implements
// its Go-side validation, canonical serialization and hashing exactly as
// specified by contracts/data/CANONICAL.md. The TypeScript reference consumer
// (contracts/data/consumer.ts) mirrors these rules; the golden fixtures in
// contracts/data/golden/ pin agreement between the two.
//
// The document is deliberately modeled as a generic parsed JSON tree rather
// than decoded Go structs: canonical bytes must not depend on struct field
// order, omitempty quirks or number decoding differences between languages.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
)

// SchemaDocumentVersionV2 is the schema document contract version implemented
// by this file.
const SchemaDocumentVersionV2 = 2

// v2SafeIntegerLimit is the magnitude bound for contract integers: values
// beyond ±(2^53-1) are not exactly representable as ECMascript numbers, so
// both language implementations would have to switch to bignums. The schema
// document never needs magnitudes this large.
const v2SafeIntegerLimit = float64(1<<53 - 1)

// ContractError is a schema-contract violation with a stable rejection code.
// The bracketed code ("[duplicate-table]") is the cross-language agreement
// surface: golden invalid fixtures match on it.
type ContractError struct {
	Code   string
	Path   string
	Detail string
}

func (e *ContractError) Error() string {
	return fmt.Sprintf("[%s] %s: %s", e.Code, e.Path, e.Detail)
}

func contractErr(code, path, format string, args ...any) error {
	return &ContractError{Code: code, Path: path, Detail: fmt.Sprintf(format, args...)}
}

// DetectSchemaVersion peeks at the top-level "version" field of a schema
// document without full validation. It returns an error only when the input
// is not valid JSON or the version is not an integer (reported with the
// invalid-number contract code, matching ParseV2Document's own spelling
// check); semantic checks belong to the per-version validators. A missing
// version reads as 0 (the version 1 validator rejects it with its existing
// message).
func DetectSchemaVersion(raw []byte) (int, error) {
	var probe struct {
		Version *json.Number `json:"version"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	if probe.Version == nil {
		return 0, nil
	}
	v, err := probe.Version.Int64()
	if err != nil {
		return 0, contractErr("invalid-number", "$.version", "schema version %s is not an integer", probe.Version.String())
	}
	return int(v), nil
}

// DocumentCheck reports what a ValidateSchemaDocument run accepted.
type DocumentCheck struct {
	Version   int
	SHA256    string // canonical-form hash; empty for version 1 documents
	Canonical []byte // canonical bytes; nil for version 1 documents
}

// ValidateSchemaDocument detects the document version and validates against
// the matching contract: version 2 documents are parsed, validated and
// canonicalized (hash available in the returned check); version 1 documents
// run the existing version 1 unmarshal + ValidateSchema path. Unknown
// versions are rejected with the unknown-version contract code.
func ValidateSchemaDocument(raw []byte) (DocumentCheck, error) {
	version, err := DetectSchemaVersion(raw)
	if err != nil {
		var ce *ContractError
		if errors.As(err, &ce) {
			return DocumentCheck{}, err // typed version rejection (invalid-number)
		}
		return DocumentCheck{}, contractErr("invalid-json", "$", "%v", err)
	}
	switch version {
	case SchemaVersion:
		var s Schema
		if err := json.Unmarshal(raw, &s); err != nil {
			return DocumentCheck{}, err
		}
		if err := ValidateSchema(&s); err != nil {
			return DocumentCheck{}, err
		}
		return DocumentCheck{Version: SchemaVersion}, nil
	case SchemaDocumentVersionV2:
		doc, err := ParseV2Document(raw)
		if err != nil {
			return DocumentCheck{}, err
		}
		return DocumentCheck{Version: SchemaDocumentVersionV2, SHA256: doc.SHA256Hex, Canonical: doc.Canonical}, nil
	default:
		return DocumentCheck{}, contractErr("unknown-version", "$.version", "schema document declares version %d; this CLI understands versions 1 and 2 only", version)
	}
}

// V2Document is a validated schema document v2 with its canonical form.
type V2Document struct {
	Root      map[string]any
	Canonical []byte
	SHA256Hex string
}

// ParseV2Document parses, validates and canonicalizes a schema document v2.
func ParseV2Document(raw []byte) (*V2Document, error) {
	if err := v2RejectDuplicateKeys(raw); err != nil {
		return nil, err
	}
	var root any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, contractErr("invalid-json", "$", "%v", err)
	}
	obj, ok := root.(map[string]any)
	if !ok {
		return nil, contractErr("not-object", "$", "schema document must be a JSON object")
	}
	if err := v2ScanStrings(root, "$"); err != nil {
		return nil, err
	}
	if err := v2ValidateDocument(obj); err != nil {
		return nil, err
	}
	canonical := v2CanonicalBytes(obj)
	sum := sha256.Sum256(canonical)
	return &V2Document{Root: obj, Canonical: canonical, SHA256Hex: hex.EncodeToString(sum[:])}, nil
}

// ---------------------------------------------------------------------------
// Raw-input hygiene
// ---------------------------------------------------------------------------

// v2RejectDuplicateKeys walks the raw token stream and rejects JSON objects
// that specify the same key twice. encoding/json and JSON.parse both silently
// keep the last duplicate, which would let two tools disagree about intent;
// the contract refuses the ambiguity.
func v2RejectDuplicateKeys(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	var scan func(path string) error
	scan = func(path string) error {
		tok, err := dec.Token()
		if err != nil {
			return contractErr("invalid-json", path, "%v", err)
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return contractErr("invalid-json", path, "%v", err)
				}
				key, ok := keyTok.(string)
				if !ok {
					return contractErr("invalid-json", path, "object key is not a string")
				}
				if seen[key] {
					return contractErr("duplicate-key", path+"."+key, "object specifies key %q twice", key)
				}
				seen[key] = true
				if err := scan(path + "." + key); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // consume '}'
				return contractErr("invalid-json", path, "%v", err)
			}
			return nil
		case '[':
			i := 0
			for dec.More() {
				if err := scan(fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
				i++
			}
			if _, err := dec.Token(); err != nil { // consume ']'
				return contractErr("invalid-json", path, "%v", err)
			}
			return nil
		default:
			return nil
		}
	}
	if err := scan("$"); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return contractErr("invalid-json", "$", "trailing content after the document object")
	}
	return nil
}

// v2ScanStrings rejects strings containing U+FFFD. Go decodes lone surrogate
// escapes (and invalid UTF-8) to U+FFFD during unmarshal, so this is a strict
// over-approximation of the canonical-form rule that strings must be
// sequences of Unicode scalar values (see CANONICAL.md §1).
func v2ScanStrings(v any, path string) error {
	switch x := v.(type) {
	case string:
		if strings.ContainsRune(x, 0xFFFD) {
			return contractErr("invalid-value", path, "string contains U+FFFD; unpaired surrogates and invalid UTF-8 are not representable")
		}
		return nil
	case []any:
		for i, item := range x {
			if err := v2ScanStrings(item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := v2ScanStrings(x[k], path+"."+k); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

var v2RootFields = []string{"version", "dialect", "capabilities", "schemas", "tables", "enums", "views", "opaque"}

var v2Capabilities = map[string]bool{"pgvector": true, "nucleus": true}

var v2TypeCodecs = map[string]string{
	"bool": "boolean", "int2": "number", "int4": "number", "int8": "bigint",
	"float4": "number", "float8": "number", "numeric": "decimal-string",
	"text": "string", "varchar": "string",
	"timestamp": "timestamp-string", "timestamptz": "timestamptz-string", "date": "date-string",
	"bytea": "binary", "uuid": "uuid", "json": "json", "jsonb": "json",
	"vector": "vector", "enum": "enum",
}

// v2TypeParams lists the type-parameter keys each type accepts. Types absent
// from the map accept no parameters at all.
var v2TypeParams = map[string]map[string]bool{
	"varchar":     {"length": true},
	"numeric":     {"precision": true, "scale": true},
	"timestamp":   {"precision": true},
	"timestamptz": {"precision": true},
	"vector":      {"dimensions": true},
}

var v2ReferentialActions = map[string]bool{"cascade": true, "restrict": true, "no action": true, "set null": true, "set default": true}
var v2FKMatches    = map[string]bool{"simple": true, "full": true, "partial": true}
var v2IndexMethods = map[string]bool{"btree": true, "hash": true, "gin": true, "gist": true, "spgist": true, "brin": true}

// Default-operator-class facts for the index methods and column types in
// the contract vocabulary: exactly these combinations apply on PostgreSQL
// without naming an operator class (built-in pg_opclass defaults; arrays
// uniformly via array_ops). Everything else is refused by the server with
// SQLSTATE 42704, so the validator rejects it at definition time — the
// contract has no operator-class slot to spell an explicit one. Derived
// from a live probe on PostgreSQL 17 and the REL_15_STABLE pg_opclass
// catalog source; the two agree on every vocabulary combination.
var v2IndexMethodScalars = map[string]map[string]bool{
	"btree":  {"text": true, "varchar": true, "bool": true, "int2": true, "int4": true, "int8": true, "float4": true, "float8": true, "numeric": true, "timestamp": true, "timestamptz": true, "date": true, "uuid": true, "bytea": true, "enum": true, "jsonb": true},
	"hash":   {"text": true, "varchar": true, "bool": true, "int2": true, "int4": true, "int8": true, "float4": true, "float8": true, "numeric": true, "timestamp": true, "timestamptz": true, "date": true, "uuid": true, "bytea": true, "enum": true, "jsonb": true},
	"gin":    {"jsonb": true},
	"gist":   {},
	"spgist": {"text": true, "varchar": true},
	"brin":   {"text": true, "varchar": true, "int2": true, "int4": true, "int8": true, "float4": true, "float8": true, "numeric": true, "timestamp": true, "timestamptz": true, "date": true, "uuid": true, "bytea": true},
}
var v2IndexMethodArrays = map[string]bool{"btree": true, "hash": true, "gin": true}

var v2OpaqueKinds  = map[string]bool{"extension-table": true, "extension-object": true, "unsupported-table": true, "unsupported-object": true}

// v2LiteralPattern matches exactly one SQL literal token, optionally cast:
// a single-quoted string with '' doubling, a numeric literal, true, false or
// null. Kept in lockstep with the pattern in schema-v2.json and consumer.ts.
const v2LiteralPattern = `^('([^']|'')*'(::[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?(\[\])*)?|-?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?|true|false|null)$`

var v2LiteralRegexp = regexp.MustCompile(v2LiteralPattern)

// v2LiteralCast splits an optional cast off a spelled literal default into
// its element type name (schema qualification dropped) and array depth:
// '{a}'::public.text[] -> ("text", 1, true). Only called on strings the
// literal pattern already accepted.
func v2LiteralCast(sql string) (string, int, bool) {
	i := strings.Index(sql, "::")
	if i < 0 {
		return "", 0, false
	}
	cast := sql[i+2:]
	depth := 0
	for strings.HasSuffix(cast, "[]") {
		depth++
		cast = cast[:len(cast)-2]
	}
	if j := strings.LastIndex(cast, "."); j >= 0 {
		cast = cast[j+1:]
	}
	return cast, depth, true
}

// v2TypeAliases maps the spellable aliases of the vocabulary's type names.
var v2TypeAliases = map[string]string{
	"boolean": "bool", "int": "int4", "integer": "int4", "smallint": "int2",
	"bigint": "int8", "real": "float4", "decimal": "numeric",
}

func v2NormalizeTypeName(name string) string {
	if alias, ok := v2TypeAliases[name]; ok {
		return alias
	}
	return name
}

var v2IntegerTypes = map[string]bool{"int2": true, "int4": true, "int8": true}

// v2ColumnType carries what the definition-time applicability and
// default-cast checks need to know about a column.
type v2ColumnType struct {
	Name    string
	IsArray bool
}

// v2State accumulates what cross-reference checks need across collections.
type v2State struct {
	capabilities   map[string]bool
	schemas        map[string]bool
	tables         map[string]map[string]any // "schema.name" -> table object
	tableColumns   map[string]map[string]bool
	tableKeyTuples map[string][][]string // exact PK/unique column tuples
	enums          map[string]bool
	views          map[string]bool
	indexIdents    map[string]string
	opaqueIdents   map[string]bool
	opaqueSchemas  []string
	sequenceSchemas []string
	enumRefs       []v2EnumRef
}

type v2EnumRef struct {
	Path string
	Key  string
}

func v2TableKey(schema, name string) string { return schema + "." + name }

func v2ValidateDocument(root map[string]any) error {
	st := &v2State{
		capabilities:   map[string]bool{},
		schemas:        map[string]bool{},
		tables:         map[string]map[string]any{},
		tableColumns:   map[string]map[string]bool{},
		tableKeyTuples: map[string][][]string{},
		enums:          map[string]bool{},
		views:          map[string]bool{},
		indexIdents:    map[string]string{},
		opaqueIdents:   map[string]bool{},
	}

	for _, k := range v2RootFields {
		if _, ok := root[k]; !ok {
			return contractErr("missing-field", "$."+k, "required field %q is absent", k)
		}
	}
	for k := range root {
		known := false
		for _, f := range v2RootFields {
			if f == k {
				known = true
				break
			}
		}
		if !known {
			return contractErr("unknown-field", "$."+k, "unknown field %q", k)
		}
	}

	if v, err := v2Int(root["version"], "$.version"); err != nil {
		return err
	} else if v != SchemaDocumentVersionV2 {
		return contractErr("unknown-version", "$.version", "schema document declares version %d; this contract is version 2", v)
	}
	dialect, err := v2String(root["dialect"], "$.dialect")
	if err != nil {
		return err
	}
	if dialect != "postgresql" {
		return contractErr("invalid-value", "$.dialect", "dialect must be \"postgresql\"")
	}

	caps, err := v2Array(root["capabilities"], "$.capabilities")
	if err != nil {
		return err
	}
	for i, c := range caps {
		s, err := v2String(c, fmt.Sprintf("$.capabilities[%d]", i))
		if err != nil {
			return err
		}
		if !v2Capabilities[s] {
			return contractErr("invalid-value", fmt.Sprintf("$.capabilities[%d]", i), "unknown capability %q", s)
		}
		if st.capabilities[s] {
			return contractErr("invalid-value", fmt.Sprintf("$.capabilities[%d]", i), "duplicate capability %q", s)
		}
		st.capabilities[s] = true
	}

	if err := v2ValidateSchemas(root, st); err != nil {
		return err
	}
	if err := v2ValidateEnums(root, st); err != nil {
		return err
	}
	if err := v2ValidateTables(root, st); err != nil {
		return err
	}
	if err := v2ValidateViews(root, st); err != nil {
		return err
	}
	if err := v2ValidateOpaque(root, st); err != nil {
		return err
	}
	return v2CrossReferences(st)
}

func v2Object(v any, path string) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, contractErr("invalid-value", path, "expected an object")
	}
	return m, nil
}

func v2Array(v any, path string) ([]any, error) {
	a, ok := v.([]any)
	if !ok {
		return nil, contractErr("invalid-value", path, "expected an array")
	}
	return a, nil
}

func v2String(v any, path string) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", contractErr("invalid-value", path, "expected a string")
	}
	return s, nil
}

func v2Bool(v any, path string) (bool, error) {
	b, ok := v.(bool)
	if !ok {
		return false, contractErr("invalid-value", path, "expected a boolean")
	}
	return b, nil
}

func v2Int(v any, path string) (int64, error) {
	f, ok := v.(float64)
	if !ok {
		return 0, contractErr("invalid-number", path, "expected an integer")
	}
	if math.Trunc(f) != f || math.Abs(f) > v2SafeIntegerLimit {
		return 0, contractErr("invalid-number", path, "value %v is not a safe integer (must be integral and within ±(2^53-1))", f)
	}
	return int64(f), nil
}

// v2CheckName rejects empty and control-character names.
func v2CheckName(s, path string) error {
	if s == "" {
		return contractErr("invalid-identity", path, "name must not be empty")
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7F {
			return contractErr("invalid-identity", path, "name must not contain control characters")
		}
	}
	return nil
}

// v2CheckSQLText enforces the shared rules for SQL text fields (schema-v2.json
// sqlText): non-empty, no NUL, and for expression positions no statement
// separator.
func v2CheckSQLText(s, path string, expression bool) error {
	if s == "" {
		return contractErr("invalid-value", path, "SQL text must not be empty")
	}
	if strings.ContainsRune(s, 0) {
		return contractErr("invalid-value", path, "SQL text must not contain NUL")
	}
	if expression && strings.Contains(s, ";") {
		return contractErr("invalid-value", path, "expression must not contain a statement separator")
	}
	return nil
}

func v2CheckIdentity(v any, path string) (schema, name string, err error) {
	m, err := v2Object(v, path)
	if err != nil {
		return "", "", err
	}
	for k := range m {
		if k != "schema" && k != "name" {
			return "", "", contractErr("unknown-field", path+"."+k, "unknown identity field %q", k)
		}
	}
	for _, k := range []string{"schema", "name"} {
		if _, ok := m[k]; !ok {
			return "", "", contractErr("missing-field", path+"."+k, "identity requires %q", k)
		}
	}
	schema, err = v2String(m["schema"], path+".schema")
	if err != nil {
		return "", "", err
	}
	name, err = v2String(m["name"], path+".name")
	if err != nil {
		return "", "", err
	}
	if err := v2CheckName(schema, path+".schema"); err != nil {
		return "", "", err
	}
	if err := v2CheckName(name, path+".name"); err != nil {
		return "", "", err
	}
	return schema, name, nil
}

func v2ValidateSchemas(root map[string]any, st *v2State) error {
	arr, err := v2Array(root["schemas"], "$.schemas")
	if err != nil {
		return err
	}
	for i, item := range arr {
		path := fmt.Sprintf("$.schemas[%d]", i)
		m, err := v2Object(item, path)
		if err != nil {
			return err
		}
		for k := range m {
			if k != "name" {
				return contractErr("unknown-field", path+"."+k, "unknown field %q", k)
			}
		}
		if _, ok := m["name"]; !ok {
			return contractErr("missing-field", path+".name", "schema object requires \"name\"")
		}
		name, err := v2String(m["name"], path+".name")
		if err != nil {
			return err
		}
		if err := v2CheckName(name, path+".name"); err != nil {
			return err
		}
		if st.schemas[name] {
			return contractErr("duplicate-schema", path, "schema %q is declared twice", name)
		}
		st.schemas[name] = true
	}
	return nil
}

func v2ValidateEnums(root map[string]any, st *v2State) error {
	arr, err := v2Array(root["enums"], "$.enums")
	if err != nil {
		return err
	}
	for i, item := range arr {
		path := fmt.Sprintf("$.enums[%d]", i)
		m, err := v2Object(item, path)
		if err != nil {
			return err
		}
		for k := range m {
			switch k {
			case "identity", "managed", "values":
			default:
				return contractErr("unknown-field", path+"."+k, "unknown field %q", k)
			}
		}
		for _, k := range []string{"identity", "managed", "values"} {
			if _, ok := m[k]; !ok {
				return contractErr("missing-field", path+"."+k, "enum requires %q", k)
			}
		}
		schema, name, err := v2CheckIdentity(m["identity"], path+".identity")
		if err != nil {
			return err
		}
		if _, err := v2Bool(m["managed"], path+".managed"); err != nil {
			return err
		}
		values, err := v2Array(m["values"], path+".values")
		if err != nil {
			return err
		}
		if len(values) == 0 {
			return contractErr("invalid-value", path+".values", "enum must declare at least one value")
		}
		seen := map[string]bool{}
		for j, v := range values {
			vs, err := v2String(v, fmt.Sprintf("%s.values[%d]", path, j))
			if err != nil {
				return err
			}
			if err := v2CheckName(vs, fmt.Sprintf("%s.values[%d]", path, j)); err != nil {
				return err
			}
			if seen[vs] {
				return contractErr("invalid-value", fmt.Sprintf("%s.values[%d]", path, j), "duplicate enum value %q", vs)
			}
			seen[vs] = true
		}
		key := v2TableKey(schema, name)
		if st.enums[key] {
			return contractErr("duplicate-enum", path+".identity", "enum %q is declared twice", key)
		}
		st.enums[key] = true
	}
	return nil
}

func v2ValidateTables(root map[string]any, st *v2State) error {
	arr, err := v2Array(root["tables"], "$.tables")
	if err != nil {
		return err
	}
	for i, item := range arr {
		if err := v2ValidateTable(item, fmt.Sprintf("$.tables[%d]", i), st); err != nil {
			return err
		}
	}
	return nil
}

func v2ValidateTable(item any, path string, st *v2State) error {
	m, err := v2Object(item, path)
	if err != nil {
		return err
	}
	for k := range m {
		switch k {
		case "identity", "managed", "columns", "constraints", "indexes":
		default:
			return contractErr("unknown-field", path+"."+k, "unknown field %q", k)
		}
	}
	for _, k := range []string{"identity", "managed", "columns", "constraints", "indexes"} {
		if _, ok := m[k]; !ok {
			return contractErr("missing-field", path+"."+k, "table requires %q", k)
		}
	}
	schema, name, err := v2CheckIdentity(m["identity"], path+".identity")
	if err != nil {
		return err
	}
	if _, err := v2Bool(m["managed"], path+".managed"); err != nil {
		return err
	}
	key := v2TableKey(schema, name)
	if _, dup := st.tables[key]; dup {
		return contractErr("duplicate-table", path+".identity", "table %q is declared twice", key)
	}

	columns, err := v2Array(m["columns"], path+".columns")
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return contractErr("invalid-value", path+".columns", "table %q must declare at least one column", key)
	}
	colNames := map[string]bool{}
	colTypes := map[string]v2ColumnType{}
	for j, col := range columns {
		colName, colType, err := v2ValidateColumn(col, fmt.Sprintf("%s.columns[%d]", path, j), st)
		if err != nil {
			return err
		}
		if colNames[colName] {
			return contractErr("duplicate-column", fmt.Sprintf("%s.columns[%d].name", path, j), "column %q is declared twice on table %q", colName, key)
		}
		colNames[colName] = true
		colTypes[colName] = colType
	}

	constraints, err := v2Array(m["constraints"], path+".constraints")
	if err != nil {
		return err
	}
	consNames := map[string]bool{}
	pkCount := 0
	for j, con := range constraints {
		cname, isPK, err := v2ValidateConstraint(con, fmt.Sprintf("%s.constraints[%d]", path, j), colNames, key)
		if err != nil {
			return err
		}
		if consNames[cname] {
			return contractErr("duplicate-constraint", fmt.Sprintf("%s.constraints[%d].name", path, j), "constraint %q is declared twice on table %q", cname, key)
		}
		consNames[cname] = true
		if isPK {
			pkCount++
		}
	}
	if pkCount > 1 {
		return contractErr("multiple-primary-key", path+".constraints", "table %q declares %d primary-key constraints; at most one is allowed", key, pkCount)
	}

	indexes, err := v2Array(m["indexes"], path+".indexes")
	if err != nil {
		return err
	}
	for j, idx := range indexes {
		if err := v2ValidateIndex(idx, fmt.Sprintf("%s.indexes[%d]", path, j), colNames, colTypes, key, st); err != nil {
			return err
		}
	}

	st.tables[key] = m
	st.tableColumns[key] = colNames
	return nil
}

func v2ValidateColumn(item any, path string, st *v2State) (string, v2ColumnType, error) {
	m, err := v2Object(item, path)
	if err != nil {
		return "", v2ColumnType{}, err
	}
	for k := range m {
		switch k {
		case "name", "type", "notNull", "default", "generated":
		default:
			return "", v2ColumnType{}, contractErr("unknown-field", path+"."+k, "unknown field %q", k)
		}
	}
	for _, k := range []string{"name", "type", "notNull"} {
		if _, ok := m[k]; !ok {
			return "", v2ColumnType{}, contractErr("missing-field", path+"."+k, "column requires %q", k)
		}
	}
	name, err := v2String(m["name"], path+".name")
	if err != nil {
		return "", v2ColumnType{}, err
	}
	if err := v2CheckName(name, path+".name"); err != nil {
		return "", v2ColumnType{}, err
	}
	if _, err := v2Bool(m["notNull"], path+".notNull"); err != nil {
		return "", v2ColumnType{}, err
	}

	if gen, ok := m["generated"]; ok {
		gm, err := v2Object(gen, path+".generated")
		if err != nil {
			return "", v2ColumnType{}, err
		}
		for k := range gm {
			if k != "expression" {
				return "", v2ColumnType{}, contractErr("unknown-field", path+".generated."+k, "unknown field %q", k)
			}
		}
		es, err := v2String(gm["expression"], path+".generated.expression")
		if err != nil {
			return "", v2ColumnType{}, err
		}
		if err := v2CheckSQLText(es, path+".generated.expression", true); err != nil {
			return "", v2ColumnType{}, err
		}
		if _, hasDefault := m["default"]; hasDefault {
			return "", v2ColumnType{}, contractErr("invalid-default", path+".default", "a generated column cannot also declare a default")
		}
	}

	typeObj, err := v2Object(m["type"], path+".type")
	if err != nil {
		return "", v2ColumnType{}, err
	}
	typeName, isArray, err := v2ValidateColumnType(typeObj, path+".type", st)
	if err != nil {
		return "", v2ColumnType{}, err
	}

	if def, ok := m["default"]; ok {
		if err := v2ValidateDefault(def, path+".default", typeName, isArray); err != nil {
			return "", v2ColumnType{}, err
		}
		defObj, _ := v2Object(def, path+".default")
		var seqRef any
		switch defObj["kind"] {
		case "sequence":
			seqRef = defObj["sequence"]
		case "identity":
			seqRef = defObj["sequence"] // optional
		}
		if seqRef != nil {
			seqSchema, _, err := v2CheckIdentity(seqRef, path+".default.sequence")
			if err != nil {
				return "", v2ColumnType{}, err
			}
			st.sequenceSchemas = append(st.sequenceSchemas, seqSchema)
		}
	}
	return name, v2ColumnType{Name: typeName, IsArray: isArray}, nil
}

func v2ValidateColumnType(typeObj map[string]any, path string, st *v2State) (typeName string, isArray bool, err error) {
	for k := range typeObj {
		switch k {
		case "name", "params", "array", "codec", "enum":
		default:
			return "", false, contractErr("unknown-field", path+"."+k, "unknown field %q", k)
		}
	}
	if _, ok := typeObj["name"]; !ok {
		return "", false, contractErr("missing-field", path+".name", "type requires \"name\"")
	}
	if _, ok := typeObj["codec"]; !ok {
		return "", false, contractErr("missing-field", path+".codec", "type requires \"codec\"")
	}
	typeName, err = v2String(typeObj["name"], path+".name")
	if err != nil {
		return "", false, err
	}
	codec, err := v2String(typeObj["codec"], path+".codec")
	if err != nil {
		return "", false, err
	}
	defaultCodec, known := v2TypeCodecs[typeName]
	if !known {
		return "", false, contractErr("unknown-type", path+".name", "unknown column type %q", typeName)
	}

	isArray = false
	if arr, ok := typeObj["array"]; ok {
		isArray, err = v2Bool(arr, path+".array")
		if err != nil {
			return "", false, err
		}
	}
	wantCodec := defaultCodec
	if isArray {
		wantCodec = "array"
	}
	if codec != wantCodec {
		return "", false, contractErr("codec-mismatch", path+".codec", "type %s (array=%v) requires codec %q, got %q", typeName, isArray, wantCodec, codec)
	}

	if typeName == "enum" {
		enumRef, ok := typeObj["enum"]
		if !ok {
			return "", false, contractErr("missing-field", path+".enum", "type name \"enum\" requires an \"enum\" identity reference")
		}
		schema, name, err := v2CheckIdentity(enumRef, path+".enum")
		if err != nil {
			return "", false, err
		}
		st.enumRefs = append(st.enumRefs, v2EnumRef{Path: path + ".enum", Key: v2TableKey(schema, name)})
	} else if _, ok := typeObj["enum"]; ok {
		return "", false, contractErr("unknown-field", path+".enum", "enum reference is only valid on type \"enum\"")
	}

	if params, ok := typeObj["params"]; ok {
		pm, err := v2Object(params, path+".params")
		if err != nil {
			return "", false, err
		}
		allowed := v2TypeParams[typeName]
		keys := make([]string, 0, len(pm))
		for k := range pm {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if allowed == nil || !allowed[k] {
				return "", false, contractErr("invalid-type-params", path+".params."+k, "type %s accepts no parameter %q", typeName, k)
			}
			iv, err := v2Int(pm[k], path+".params."+k)
			if err != nil {
				return "", false, err
			}
			switch k {
			case "length":
				if iv < 1 || iv > 10485760 {
					return "", false, contractErr("invalid-type-params", path+".params.length", "length must be within 1..10485760")
				}
			case "precision":
				if typeName == "numeric" {
					if iv < 1 || iv > 1000 {
						return "", false, contractErr("invalid-type-params", path+".params.precision", "numeric precision must be within 1..1000")
					}
				} else if iv < 0 || iv > 6 {
					return "", false, contractErr("invalid-type-params", path+".params.precision", "timestamp precision must be within 0..6")
				}
			case "scale":
				prec, hasPrec := pm["precision"]
				if !hasPrec {
					return "", false, contractErr("invalid-type-params", path+".params.scale", "scale requires precision")
				}
				precV, err := v2Int(prec, path+".params.precision")
				if err != nil {
					return "", false, err
				}
				if iv < 0 || iv > precV {
					return "", false, contractErr("invalid-type-params", path+".params.scale", "scale must be within 0..precision")
				}
			case "dimensions":
				if iv < 1 || iv > 16000 {
					return "", false, contractErr("invalid-type-params", path+".params.dimensions", "vector dimensions must be within 1..16000")
				}
			}
		}
		if typeName == "vector" {
			if _, ok := pm["dimensions"]; !ok {
				return "", false, contractErr("invalid-type-params", path+".params.dimensions", "vector type requires dimensions")
			}
		}
	} else if typeName == "vector" {
		return "", false, contractErr("invalid-type-params", path+".params", "vector type requires params.dimensions")
	}

	if typeName == "vector" {
		if !st.capabilities["pgvector"] && !st.capabilities["nucleus"] {
			return "", false, contractErr("vector-capability", path+".name", "vector columns require capability \"pgvector\" or \"nucleus\" in the document capabilities")
		}
	}
	return typeName, isArray, nil
}

func v2ValidateDefault(item any, path string, typeName string, isArray bool) error {
	m, err := v2Object(item, path)
	if err != nil {
		return err
	}
	kind, err := v2String(m["kind"], path+".kind")
	if err != nil {
		return err
	}
	switch kind {
	case "literal":
		for k := range m {
			if k != "kind" && k != "sql" {
				return contractErr("unknown-field", path+"."+k, "unknown field %q on literal default", k)
			}
		}
		if _, ok := m["sql"]; !ok {
			return contractErr("missing-field", path+".sql", "literal default requires \"sql\"")
		}
		sql, err := v2String(m["sql"], path+".sql")
		if err != nil {
			return err
		}
		if !v2LiteralRegexp.MatchString(sql) {
			return contractErr("invalid-literal", path+".sql", "default tagged literal must be exactly one SQL literal token (quoted string, numeric, true, false or null), got %q — expression-looking text must be tagged expression", sql)
		}
		if isArray && typeName == "enum" {
			return contractErr("invalid-default", path, "enum array column defaults are explicitly unsupported — the enum element cast cannot be spelled as a contract literal")
		}
		elem, depth, hasCast := v2LiteralCast(sql)
		if hasCast && (isArray || depth > 0) {
			if !isArray || depth == 0 {
				return contractErr("invalid-default", path+".sql", "default cast ::%s does not match the array-ness of column type %s (PostgreSQL refuses it with SQLSTATE 42804 at apply)", elem, typeName)
			}
			if v2NormalizeTypeName(elem) != typeName {
				return contractErr("invalid-default", path+".sql", "array default cast ::%s does not match the column element type %s (PostgreSQL refuses it with SQLSTATE 42804 at apply)", elem, typeName)
			}
		}
		return nil
	case "expression":
		for k := range m {
			if k != "kind" && k != "sql" {
				return contractErr("unknown-field", path+"."+k, "unknown field %q on expression default", k)
			}
		}
		if _, ok := m["sql"]; !ok {
			return contractErr("missing-field", path+".sql", "expression default requires \"sql\"")
		}
		sql, err := v2String(m["sql"], path+".sql")
		if err != nil {
			return err
		}
		return v2CheckSQLText(sql, path+".sql", true)
	case "identity":
		for k := range m {
			if k != "kind" && k != "generated" && k != "sequence" {
				return contractErr("unknown-field", path+"."+k, "unknown field %q on identity default", k)
			}
		}
		if !v2IntegerTypes[typeName] {
			return contractErr("invalid-default", path, "identity defaults are only valid on integer columns (int2/int4/int8), not %s", typeName)
		}
		gen, err := v2String(m["generated"], path+".generated")
		if err != nil {
			return err
		}
		if gen != "always" && gen != "by default" {
			return contractErr("invalid-value", path+".generated", "generated must be \"always\" or \"by default\"")
		}
		return nil
	case "sequence":
		for k := range m {
			if k != "kind" && k != "sequence" {
				return contractErr("unknown-field", path+"."+k, "unknown field %q on sequence default", k)
			}
		}
		if !v2IntegerTypes[typeName] {
			return contractErr("invalid-default", path, "sequence defaults are only valid on integer columns (int2/int4/int8), not %s", typeName)
		}
		if _, _, err := v2CheckIdentity(m["sequence"], path+".sequence"); err != nil {
			return err
		}
		return nil
	default:
		return contractErr("invalid-value", path+".kind", "unknown default kind %q", kind)
	}
}

func v2ValidateConstraint(item any, path string, colNames map[string]bool, tableKey string) (name string, isPK bool, err error) {
	m, err := v2Object(item, path)
	if err != nil {
		return "", false, err
	}
	ctype, err := v2String(m["type"], path+".type")
	if err != nil {
		return "", false, err
	}
	switch ctype {
	case "primary-key", "unique", "check", "foreign-key":
	default:
		return "", false, contractErr("invalid-value", path+".type", "unknown constraint type %q", ctype)
	}
	allowed := map[string]bool{"name": true, "type": true, "deferrable": true, "initiallyDeferred": true}
	switch ctype {
	case "check":
		allowed["expression"] = true
	case "foreign-key":
		allowed["columns"] = true
		allowed["references"] = true
	default:
		allowed["columns"] = true
	}
	for k := range m {
		if !allowed[k] {
			return "", false, contractErr("unknown-field", path+"."+k, "unknown field %q on %s constraint", k, ctype)
		}
	}
	for _, k := range []string{"name", "type"} {
		if _, ok := m[k]; !ok {
			return "", false, contractErr("missing-field", path+"."+k, "constraint requires %q", k)
		}
	}
	name, err = v2String(m["name"], path+".name")
	if err != nil {
		return "", false, err
	}
	if err := v2CheckName(name, path+".name"); err != nil {
		return "", false, err
	}

	if err := v2CheckDeferrable(m, path); err != nil {
		return "", false, err
	}

	checkColumns := func() ([]string, error) {
		arr, err := v2Array(m["columns"], path+".columns")
		if err != nil {
			return nil, err
		}
		if len(arr) == 0 {
			return nil, contractErr("invalid-value", path+".columns", "constraint must list at least one column")
		}
		cols := make([]string, 0, len(arr))
		seen := map[string]bool{}
		for i, c := range arr {
			cs, err := v2String(c, fmt.Sprintf("%s.columns[%d]", path, i))
			if err != nil {
				return nil, err
			}
			if !colNames[cs] {
				return nil, contractErr("constraint-column", fmt.Sprintf("%s.columns[%d]", path, i), "constraint %q references unknown column %q on table %q", name, cs, tableKey)
			}
			if seen[cs] {
				return nil, contractErr("constraint-column", fmt.Sprintf("%s.columns[%d]", path, i), "constraint %q lists column %q twice", name, cs)
			}
			seen[cs] = true
			cols = append(cols, cs)
		}
		return cols, nil
	}

	switch ctype {
	case "primary-key":
		if _, err := checkColumns(); err != nil {
			return "", false, err
		}
		return name, true, nil
	case "unique":
		if _, err := checkColumns(); err != nil {
			return "", false, err
		}
		return name, false, nil
	case "check":
		expr, err := v2String(m["expression"], path+".expression")
		if err != nil {
			return "", false, err
		}
		if expr == "" || strings.Contains(expr, ";") {
			return "", false, contractErr("check-expression", path+".expression", "check expression must be non-empty and must not contain a statement separator")
		}
		return name, false, nil
	case "foreign-key":
		if _, err := checkColumns(); err != nil {
			return "", false, err
		}
		refObj, err := v2Object(m["references"], path+".references")
		if err != nil {
			return "", false, err
		}
		for k := range refObj {
			switch k {
			case "table", "columns", "onDelete", "onUpdate", "match":
			default:
				return "", false, contractErr("unknown-field", path+".references."+k, "unknown field %q on foreign-key references", k)
			}
		}
		for _, k := range []string{"table", "columns"} {
			if _, ok := refObj[k]; !ok {
				return "", false, contractErr("missing-field", path+".references."+k, "foreign-key references requires %q", k)
			}
		}
		if _, _, err := v2CheckIdentity(refObj["table"], path+".references.table"); err != nil {
			return "", false, err
		}
		refCols, err := v2Array(refObj["columns"], path+".references.columns")
		if err != nil {
			return "", false, err
		}
		if len(refCols) == 0 {
			return "", false, contractErr("invalid-value", path+".references.columns", "foreign-key references must list at least one column")
		}
		for i, c := range refCols {
			if _, err := v2String(c, fmt.Sprintf("%s.references.columns[%d]", path, i)); err != nil {
				return "", false, err
			}
		}
		fkCols, _ := v2Array(m["columns"], path+".columns")
		if len(refCols) != len(fkCols) {
			return "", false, contractErr("invalid-value", path+".references.columns", "foreign-key references %d columns but the constraint lists %d", len(refCols), len(fkCols))
		}
		for _, k := range []string{"onDelete", "onUpdate"} {
			if v, ok := refObj[k]; ok {
				s, err := v2String(v, path+".references."+k)
				if err != nil {
					return "", false, err
				}
				if !v2ReferentialActions[s] {
					return "", false, contractErr("invalid-value", path+".references."+k, "unknown referential action %q", s)
				}
			}
		}
		if v, ok := refObj["match"]; ok {
			s, err := v2String(v, path+".references.match")
			if err != nil {
				return "", false, err
			}
			if !v2FKMatches[s] {
				return "", false, contractErr("invalid-value", path+".references.match", "unknown match type %q", s)
			}
		}
		return name, false, nil
	}
	return name, false, nil
}

func v2CheckDeferrable(m map[string]any, path string) error {
	def, hasDef := m["deferrable"]
	if hasDef {
		if _, err := v2Bool(def, path+".deferrable"); err != nil {
			return err
		}
	}
	init, hasInit := m["initiallyDeferred"]
	if hasInit {
		if _, err := v2Bool(init, path+".initiallyDeferred"); err != nil {
			return err
		}
		if !hasDef {
			return contractErr("invalid-value", path+".initiallyDeferred", "initiallyDeferred requires deferrable")
		}
	}
	return nil
}

func v2ValidateIndex(item any, path string, colNames map[string]bool, colTypes map[string]v2ColumnType, tableKey string, st *v2State) error {
	m, err := v2Object(item, path)
	if err != nil {
		return err
	}
	for k := range m {
		switch k {
		case "identity", "unique", "method", "key", "where", "include":
		default:
			return contractErr("unknown-field", path+"."+k, "unknown field %q on index", k)
		}
	}
	for _, k := range []string{"identity", "unique", "method", "key"} {
		if _, ok := m[k]; !ok {
			return contractErr("missing-field", path+"."+k, "index requires %q", k)
		}
	}
	schema, name, err := v2CheckIdentity(m["identity"], path+".identity")
	if err != nil {
		return err
	}
	ident := v2TableKey(schema, name)
	if owner, dup := st.indexIdents[ident]; dup {
		return contractErr("duplicate-index", path+".identity", "index %q is declared twice (also on table %q); index names are schema-global", ident, owner)
	}
	if _, err := v2Bool(m["unique"], path+".unique"); err != nil {
		return err
	}
	method, err := v2String(m["method"], path+".method")
	if err != nil {
		return err
	}
	if !v2IndexMethods[method] {
		return contractErr("invalid-value", path+".method", "unknown index method %q", method)
	}
	key, err := v2Array(m["key"], path+".key")
	if err != nil {
		return err
	}
	if len(key) == 0 {
		return contractErr("index-key", path+".key", "index must declare at least one key part")
	}
	for i, part := range key {
		ppath := fmt.Sprintf("%s.key[%d]", path, i)
		pm, err := v2Object(part, ppath)
		if err != nil {
			return err
		}
		col, hasCol := pm["column"]
		expr, hasExpr := pm["expression"]
		switch {
		case hasCol && hasExpr:
			return contractErr("index-key", ppath, "key part sets both column and expression")
		case hasCol:
			cs, err := v2String(col, ppath+".column")
			if err != nil {
				return err
			}
			if !colNames[cs] {
				return contractErr("constraint-column", ppath+".column", "index %q references unknown column %q on table %q", ident, cs, tableKey)
			}
			if ct, ok := colTypes[cs]; ok {
				applicable := ct.IsArray && v2IndexMethodArrays[method] || !ct.IsArray && v2IndexMethodScalars[method][ct.Name]
				if !applicable {
					desc := ct.Name
					if ct.IsArray {
						desc += "[]"
					}
					return contractErr("invalid-index", ppath, "index method %q over column %q (%s) has no default operator class on any supported server (PostgreSQL refuses it with SQLSTATE 42704) — explicitly unsupported: the contract has no operator-class slot", method, cs, desc)
				}
			}
		case hasExpr:
			es, err := v2String(expr, ppath+".expression")
			if err != nil {
				return err
			}
			if err := v2CheckSQLText(es, ppath+".expression", true); err != nil {
				return err
			}
			if method != "btree" {
				return contractErr("invalid-index", ppath, "expression keys are only definable with method \"btree\" — the default operator class of an expression result type cannot be verified at definition time (the contract has no operator-class slot)")
			}
			if _, hasOrder := pm["order"]; hasOrder {
				return contractErr("index-key", ppath, "ordered expression keys are explicitly unsupported (per-part expression deparse is engine-dependent)")
			}
			if _, hasNulls := pm["nulls"]; hasNulls {
				return contractErr("index-key", ppath, "ordered expression keys are explicitly unsupported (per-part expression deparse is engine-dependent)")
			}
		default:
			return contractErr("index-key", ppath, "key part needs either column or expression")
		}
		for k := range pm {
			if k != "column" && k != "expression" && k != "order" && k != "nulls" {
				return contractErr("unknown-field", ppath+"."+k, "unknown field %q on index key part", k)
			}
		}
		if o, ok := pm["order"]; ok {
			os, err := v2String(o, ppath+".order")
			if err != nil {
				return err
			}
			if os != "asc" && os != "desc" {
				return contractErr("invalid-value", ppath+".order", "index key part order must be \"asc\" or \"desc\", got %q", os)
			}
		}
		if n, ok := pm["nulls"]; ok {
			ns, err := v2String(n, ppath+".nulls")
			if err != nil {
				return err
			}
			if ns != "first" && ns != "last" {
				return contractErr("invalid-value", ppath+".nulls", "index key part nulls must be \"first\" or \"last\", got %q", ns)
			}
		}
	}
	if where, ok := m["where"]; ok {
		ws, err := v2String(where, path+".where")
		if err != nil {
			return err
		}
		if err := v2CheckSQLText(ws, path+".where", true); err != nil {
			return err
		}
	}
	if include, ok := m["include"]; ok {
		arr, err := v2Array(include, path+".include")
		if err != nil {
			return err
		}
		seen := map[string]bool{}
		for i, c := range arr {
			cs, err := v2String(c, fmt.Sprintf("%s.include[%d]", path, i))
			if err != nil {
				return err
			}
			if !colNames[cs] {
				return contractErr("constraint-column", fmt.Sprintf("%s.include[%d]", path, i), "index %q includes unknown column %q on table %q", ident, cs, tableKey)
			}
			if seen[cs] {
				return contractErr("invalid-value", fmt.Sprintf("%s.include[%d]", path, i), "index %q includes column %q twice", ident, cs)
			}
			seen[cs] = true
		}
	}
	st.indexIdents[ident] = tableKey
	return nil
}

func v2ValidateViews(root map[string]any, st *v2State) error {
	arr, err := v2Array(root["views"], "$.views")
	if err != nil {
		return err
	}
	for i, item := range arr {
		path := fmt.Sprintf("$.views[%d]", i)
		m, err := v2Object(item, path)
		if err != nil {
			return err
		}
		for k := range m {
			switch k {
			case "identity", "managed", "definition", "checkOption", "securityInvoker":
			default:
				return contractErr("unknown-field", path+"."+k, "unknown field %q on view", k)
			}
		}
		for _, k := range []string{"identity", "managed", "definition"} {
			if _, ok := m[k]; !ok {
				return contractErr("missing-field", path+"."+k, "view requires %q", k)
			}
		}
		schema, name, err := v2CheckIdentity(m["identity"], path+".identity")
		if err != nil {
			return err
		}
		if _, err := v2Bool(m["managed"], path+".managed"); err != nil {
			return err
		}
		def, err := v2String(m["definition"], path+".definition")
		if err != nil {
			return err
		}
		if err := v2CheckSQLText(def, path+".definition", false); err != nil {
			return err
		}
		if co, ok := m["checkOption"]; ok {
			s, err := v2String(co, path+".checkOption")
			if err != nil {
				return err
			}
			if s != "local" && s != "cascaded" {
				return contractErr("invalid-value", path+".checkOption", "checkOption must be \"local\" or \"cascaded\"")
			}
		}
		if si, ok := m["securityInvoker"]; ok {
			if _, err := v2Bool(si, path+".securityInvoker"); err != nil {
				return err
			}
		}
		key := v2TableKey(schema, name)
		if _, dup := st.views[key]; dup {
			return contractErr("duplicate-view", path+".identity", "view %q is declared twice", key)
		}
		st.views[key] = true
	}
	return nil
}

func v2ValidateOpaque(root map[string]any, st *v2State) error {
	arr, err := v2Array(root["opaque"], "$.opaque")
	if err != nil {
		return err
	}
	for i, item := range arr {
		path := fmt.Sprintf("$.opaque[%d]", i)
		m, err := v2Object(item, path)
		if err != nil {
			return err
		}
		for k := range m {
			switch k {
			case "kind", "identity", "owner", "reason":
			default:
				return contractErr("unknown-field", path+"."+k, "unknown field %q on opaque entry", k)
			}
		}
		for _, k := range []string{"kind", "identity", "reason"} {
			if _, ok := m[k]; !ok {
				return contractErr("missing-field", path+"."+k, "opaque entry requires %q", k)
			}
		}
		kind, err := v2String(m["kind"], path+".kind")
		if err != nil {
			return err
		}
		if !v2OpaqueKinds[kind] {
			return contractErr("invalid-value", path+".kind", "unknown opaque kind %q", kind)
		}
		schema, name, err := v2CheckIdentity(m["identity"], path+".identity")
		if err != nil {
			return err
		}
		if owner, ok := m["owner"]; ok {
			os, err := v2String(owner, path+".owner")
			if err != nil {
				return err
			}
			if err := v2CheckName(os, path+".owner"); err != nil {
				return err
			}
		}
		reason, err := v2String(m["reason"], path+".reason")
		if err != nil {
			return err
		}
		if reason == "" {
			return contractErr("invalid-value", path+".reason", "opaque entries must state a reason")
		}
		triple := kind + "|" + v2TableKey(schema, name)
		if st.opaqueIdents[triple] {
			return contractErr("invalid-value", path, "opaque entry %s on %q is declared twice", kind, v2TableKey(schema, name))
		}
		st.opaqueIdents[triple] = true
		st.opaqueSchemas = append(st.opaqueSchemas, schema)
	}
	return nil
}

// v2CrossReferences resolves everything that needs the whole document:
// declared schemas, enum references, sequence schemas, PK notNull,
// foreign-key targets and PK/unique coverage of FK target tuples.
func v2CrossReferences(st *v2State) error {
	schemaOf := func(key string) string { return key[:strings.IndexByte(key, '.')] }
	for key := range st.tables {
		if !st.schemas[schemaOf(key)] {
			return contractErr("undeclared-schema", "$.tables", "table %q lives in undeclared schema %q", key, schemaOf(key))
		}
	}
	for key := range st.enums {
		if !st.schemas[schemaOf(key)] {
			return contractErr("undeclared-schema", "$.enums", "enum %q lives in undeclared schema %q", key, schemaOf(key))
		}
	}
	for key := range st.views {
		if !st.schemas[schemaOf(key)] {
			return contractErr("undeclared-schema", "$.views", "view %q lives in undeclared schema %q", key, schemaOf(key))
		}
	}
	for ident, owner := range st.indexIdents {
		if !st.schemas[schemaOf(ident)] {
			return contractErr("undeclared-schema", "$.tables", "index %q (on table %q) lives in undeclared schema %q", ident, owner, schemaOf(ident))
		}
	}
	for _, schema := range st.opaqueSchemas {
		if !st.schemas[schema] {
			return contractErr("undeclared-schema", "$.opaque", "opaque entry lives in undeclared schema %q", schema)
		}
	}
	for _, schema := range st.sequenceSchemas {
		if !st.schemas[schema] {
			return contractErr("undeclared-schema", "$.tables", "default sequence lives in undeclared schema %q", schema)
		}
	}
	for _, ref := range st.enumRefs {
		if !st.enums[ref.Key] {
			return contractErr("enum-unresolved", ref.Path, "column references enum %q, which is not declared", ref.Key)
		}
	}

	// Pass 1: PK notNull and key-tuple collection. Pass 2: FK target
	// existence and coverage (a target sorted later than the referencing
	// table must already have its tuples registered). Iterated in
	// deterministic (sorted) table order so error selection never depends on
	// Go map iteration.
	tableKeys := make([]string, 0, len(st.tables))
	for key := range st.tables {
		tableKeys = append(tableKeys, key)
	}
	sort.Strings(tableKeys)

	for _, tableKey := range tableKeys {
		table := st.tables[tableKey]
		columns := map[string]map[string]any{}
		cols, _ := v2Array(table["columns"], "$.tables")
		for _, c := range cols {
			cm, _ := v2Object(c, "$")
			name, _ := v2String(cm["name"], "$")
			columns[name] = cm
		}
		constraints, _ := v2Array(table["constraints"], "$.tables")
		for _, con := range constraints {
			cm, _ := v2Object(con, "$")
			ctype, _ := v2String(cm["type"], "$")
			switch ctype {
			case "primary-key":
				pkCols, _ := v2Array(cm["columns"], "$")
				for _, pc := range pkCols {
					colName, _ := pc.(string)
					col := columns[colName]
					if col == nil {
						continue
					}
					if nn, _ := col["notNull"].(bool); !nn {
						return contractErr("pk-not-null", "$.tables", "primary-key column %q on table %q must declare notNull: true (v2 performs no silent normalization)", colName, tableKey)
					}
				}
				st.tableKeyTuples[tableKey] = append(st.tableKeyTuples[tableKey], stringSlice(pkCols))
			case "unique":
				ucs, _ := v2Array(cm["columns"], "$")
				st.tableKeyTuples[tableKey] = append(st.tableKeyTuples[tableKey], stringSlice(ucs))
			}
		}
	}

	for _, tableKey := range tableKeys {
		table := st.tables[tableKey]
		constraints, _ := v2Array(table["constraints"], "$.tables")
		for _, con := range constraints {
			cm, _ := v2Object(con, "$")
			ctype, _ := v2String(cm["type"], "$")
			if ctype != "foreign-key" {
				continue
			}
			refObj, _ := v2Object(cm["references"], "$")
			refTable, _ := v2Object(refObj["table"], "$")
			rschema, _ := refTable["schema"].(string)
			rname, _ := refTable["name"].(string)
			targetKey := v2TableKey(rschema, rname)
			if _, ok := st.tables[targetKey]; !ok {
				return contractErr("fk-target", "$.tables", "foreign key on %q references table %q, which is not declared", tableKey, targetKey)
			}
			refColsRaw, _ := v2Array(refObj["columns"], "$")
			refCols := stringSlice(refColsRaw)
			targetCols := st.tableColumns[targetKey]
			for _, rc := range refCols {
				if !targetCols[rc] {
					return contractErr("fk-column", "$.tables", "foreign key on %q references column %q, which does not exist on %q", tableKey, rc, targetKey)
				}
			}
			covered := false
			for _, tuple := range st.tableKeyTuples[targetKey] {
				if len(tuple) == len(refCols) && equalStrings(tuple, refCols) {
					covered = true
					break
				}
			}
			if !covered {
				return contractErr("fk-not-unique", "$.tables", "foreign key on %q references (%s) on %q, which is not covered by a primary-key or unique constraint", tableKey, strings.Join(refCols, ", "), targetKey)
			}
		}
	}
	return nil
}

func stringSlice(arr []any) []string {
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		s, _ := v.(string)
		out = append(out, s)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Canonical form (see contracts/data/CANONICAL.md)
// ---------------------------------------------------------------------------

// v2CanonicalBytes builds the canonical serialization of a validated
// document: unordered sets sorted, ordered tuples preserved, compact output,
// bytewise-sorted object keys.
func v2CanonicalBytes(root map[string]any) []byte {
	out := make(map[string]any, len(root))
	for k, v := range root {
		switch k {
		case "capabilities":
			out[k] = v2SortedStrings(v)
		case "schemas":
			out[k] = v2SortByField(v, "name")
		case "tables":
			out[k] = v2CanonicalTables(v)
		case "enums", "views":
			out[k] = v2SortByIdentity(v)
		case "opaque":
			out[k] = v2SortOpaque(v)
		default:
			out[k] = v
		}
	}
	return v2AppendValue(nil, out)
}

func v2CanonicalTables(tables any) []any {
	sorted := v2SortByIdentity(tables)
	out := make([]any, 0, len(sorted))
	for _, t := range sorted {
		tm, _ := t.(map[string]any)
		cp := make(map[string]any, len(tm))
		for k, v := range tm {
			cp[k] = v
		}
		// columns are an ordered tuple: physical column order (attnum) is
		// semantic and preserved verbatim (CANONICAL.md §2).
		cp["constraints"] = v2SortByField(cp["constraints"], "name")
		idxs := v2SortByIdentity(cp["indexes"])
		canonIdxs := make([]any, 0, len(idxs))
		for _, idx := range idxs {
			im, _ := idx.(map[string]any)
			icp := make(map[string]any, len(im))
			for k, v := range im {
				icp[k] = v
			}
			if inc, ok := icp["include"]; ok {
				icp["include"] = v2SortedStrings(inc)
			}
			canonIdxs = append(canonIdxs, icp)
		}
		cp["indexes"] = canonIdxs
		out = append(out, cp)
	}
	return out
}

func v2SortByField(v any, field string) []any {
	arr, _ := v.([]any)
	out := append([]any(nil), arr...)
	sort.SliceStable(out, func(i, j int) bool {
		a, _ := out[i].(map[string]any)
		b, _ := out[j].(map[string]any)
		return v2FieldKey(a, field) < v2FieldKey(b, field)
	})
	return out
}

func v2FieldKey(m map[string]any, field string) string {
	v, ok := m[field]
	if !ok {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		schema, _ := x["schema"].(string)
		name, _ := x["name"].(string)
		return schema + "|" + name
	default:
		return ""
	}
}

func v2SortByIdentity(v any) []any {
	return v2SortByField(v, "identity")
}

func v2SortOpaque(v any) []any {
	arr, _ := v.([]any)
	out := append([]any(nil), arr...)
	sort.SliceStable(out, func(i, j int) bool {
		a, _ := out[i].(map[string]any)
		b, _ := out[j].(map[string]any)
		return v2OpaqueKey(a) < v2OpaqueKey(b)
	})
	return out
}

func v2OpaqueKey(m map[string]any) string {
	id, _ := m["identity"].(map[string]any)
	schema, _ := id["schema"].(string)
	name, _ := id["name"].(string)
	kind, _ := m["kind"].(string)
	return schema + "|" + name + "|" + kind
}

func v2SortedStrings(v any) []any {
	arr, _ := v.([]any)
	out := append([]any(nil), arr...)
	sort.SliceStable(out, func(i, j int) bool {
		a, _ := out[i].(string)
		b, _ := out[j].(string)
		return a < b
	})
	return out
}

// v2AppendValue serializes a validated tree in canonical JSON form. All
// numbers in a validated document are safe integers, so float64 values
// convert to int64 losslessly.
func v2AppendValue(dst []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...)
	case bool:
		if x {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)
	case string:
		return v2AppendString(dst, x)
	case float64:
		return strconvAppendInt(dst, int64(x))
	case int64:
		return strconvAppendInt(dst, x)
	case []any:
		dst = append(dst, '[')
		for i, item := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = v2AppendValue(dst, item)
		}
		return append(dst, ']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		dst = append(dst, '{')
		for i, k := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = v2AppendString(dst, k)
			dst = append(dst, ':')
			dst = v2AppendValue(dst, x[k])
		}
		return append(dst, '}')
	default:
		// A validated document contains only the types above.
		return append(dst, "null"...)
	}
}

func strconvAppendInt(dst []byte, v int64) []byte {
	return append(dst, []byte(fmt.Sprintf("%d", v))...)
}

// v2AppendString applies the canonical escaping rules: the two JSON mandatory
// escapes, short forms for the five common C0 controls, \u00xx (lowercase
// hex) for the rest, and raw UTF-8 for everything else. This matches
// ECMascript JSON.stringify output byte for byte and deliberately differs
// from encoding/json (no HTML escaping, no U+2028/U+2029 escaping).
func v2AppendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for _, r := range s {
		switch {
		case r == '"':
			dst = append(dst, '\\', '"')
		case r == '\\':
			dst = append(dst, '\\', '\\')
		case r == '\b':
			dst = append(dst, '\\', 'b')
		case r == '\f':
			dst = append(dst, '\\', 'f')
		case r == '\n':
			dst = append(dst, '\\', 'n')
		case r == '\r':
			dst = append(dst, '\\', 'r')
		case r == '\t':
			dst = append(dst, '\\', 't')
		case r < 0x20:
			dst = append(dst, []byte(fmt.Sprintf("\\u%04x", r))...)
		default:
			dst = append(dst, []byte(string(r))...)
		}
	}
	return append(dst, '"')
}
