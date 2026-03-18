package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
)

// ColumnDetail is the detailed schema info returned by /api/columns.
type ColumnDetail struct {
	Name         string  `json:"name"`
	DataType     string  `json:"dataType"`
	IsNullable   bool    `json:"isNullable"`
	Default      *string `json:"default"`
	IsPrimaryKey bool    `json:"isPrimaryKey"`
	Ordinal      int     `json:"ordinal"`
}

// IndexDetail describes a non-PK index on a table.
type IndexDetail struct {
	Name     string   `json:"name"`
	Columns  []string `json:"columns"`
	IsUnique bool     `json:"isUnique"`
}

// colInfo is used internally by the code generators.
type colInfo struct {
	name     string
	dataType string
	udtName  string
	nullable bool
	isPK     bool
}

// --- /api/columns ---

func (s *Server) handleColumns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	q := r.URL.Query()
	connID := q.Get("connectionId")
	schemaName := q.Get("schema")
	tableName := q.Get("table")
	if connID == "" || tableName == "" {
		writeError(w, http.StatusBadRequest, "connectionId and table are required")
		return
	}
	if schemaName == "" {
		schemaName = "public"
	}
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}

	rows, err := client.Query(r.Context(), `
		SELECT
			c.column_name,
			c.data_type,
			c.udt_name,
			c.character_maximum_length,
			(c.is_nullable = 'YES') AS is_nullable,
			c.column_default,
			c.ordinal_position,
			COALESCE(pk.is_pk, false) AS is_primary_key
		FROM information_schema.columns c
		LEFT JOIN (
			SELECT ku.column_name, true AS is_pk
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage ku
				ON tc.constraint_name = ku.constraint_name
				AND tc.table_schema = ku.table_schema
				AND tc.table_name = ku.table_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
			  AND tc.table_schema = $1
			  AND tc.table_name = $2
		) pk ON pk.column_name = c.column_name
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position
	`, schemaName, tableName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var cols []ColumnDetail
	for rows.Next() {
		var (
			name     string
			dataType string
			udtName  string
			maxLen   *int
			nullable bool
			defVal   *string
			ordinal  int
			isPK     bool
		)
		if err := rows.Scan(&name, &dataType, &udtName, &maxLen, &nullable, &defVal, &ordinal, &isPK); err != nil {
			continue
		}
		cols = append(cols, ColumnDetail{
			Name:         name,
			DataType:     resolveDisplayType(dataType, udtName, maxLen),
			IsNullable:   nullable,
			Default:      defVal,
			IsPrimaryKey: isPK,
			Ordinal:      ordinal,
		})
	}
	if cols == nil {
		cols = []ColumnDetail{}
	}

	// Fetch non-PK indexes
	idxRows, err := client.Query(r.Context(), `
		SELECT i.relname, ix.indisunique,
		       array_agg(a.attname ORDER BY u.k) AS columns
		FROM pg_class t
		JOIN pg_index ix ON t.oid = ix.indrelid
		JOIN pg_class i  ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS u(attnum, k) ON true
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
		WHERE t.relname = $1 AND n.nspname = $2 AND NOT ix.indisprimary
		GROUP BY i.relname, ix.indisunique
		ORDER BY i.relname
	`, tableName, schemaName)

	var indexes []IndexDetail
	if err == nil {
		defer idxRows.Close()
		for idxRows.Next() {
			var (
				name    string
				unique  bool
				colsArr []string
			)
			if err := idxRows.Scan(&name, &unique, &colsArr); err != nil {
				continue
			}
			indexes = append(indexes, IndexDetail{
				Name:     name,
				Columns:  colsArr,
				IsUnique: unique,
			})
		}
	}
	if indexes == nil {
		indexes = []IndexDetail{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"columns": cols,
		"indexes": indexes,
	})
}

func resolveDisplayType(dataType, udtName string, maxLen *int) string {
	switch dataType {
	case "character varying":
		if maxLen != nil {
			return fmt.Sprintf("varchar(%d)", *maxLen)
		}
		return "varchar"
	case "character":
		if maxLen != nil {
			return fmt.Sprintf("char(%d)", *maxLen)
		}
		return "char"
	case "USER-DEFINED":
		return udtName
	case "ARRAY":
		return udtName + "[]"
	default:
		return dataType
	}
}

// --- /api/ddl ---

func (s *Server) handleDDL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	var body struct {
		SQL          string `json:"sql"`
		ConnectionID string `json:"connectionId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if body.SQL == "" || body.ConnectionID == "" {
		writeError(w, http.StatusBadRequest, "sql and connectionId are required")
		return
	}
	client, ok := s.clientFor(body.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}

	start := time.Now()
	if err := client.Exec(r.Context(), body.SQL); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":       false,
			"duration": time.Since(start).Milliseconds(),
			"error":    err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"duration": time.Since(start).Milliseconds(),
	})
}

// --- /api/codegen ---

func (s *Server) handleCodegen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET required")
		return
	}
	q := r.URL.Query()
	connID := q.Get("connectionId")
	schemaName := q.Get("schema")
	tableName := q.Get("table")
	lang := q.Get("lang")
	if connID == "" || tableName == "" {
		writeError(w, http.StatusBadRequest, "connectionId and table are required")
		return
	}
	if schemaName == "" {
		schemaName = "public"
	}
	if lang == "" {
		lang = "go"
	}

	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}

	rows, err := client.Query(r.Context(), `
		SELECT c.column_name, c.data_type, c.udt_name,
		       (c.is_nullable = 'YES'), COALESCE(pk.is_pk, false)
		FROM information_schema.columns c
		LEFT JOIN (
			SELECT ku.column_name, true AS is_pk
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage ku
				ON tc.constraint_name = ku.constraint_name
				AND tc.table_schema = ku.table_schema
				AND tc.table_name = ku.table_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
			  AND tc.table_schema = $1 AND tc.table_name = $2
		) pk ON pk.column_name = c.column_name
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position
	`, schemaName, tableName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var cols []colInfo
	for rows.Next() {
		var c colInfo
		if err := rows.Scan(&c.name, &c.dataType, &c.udtName, &c.nullable, &c.isPK); err != nil {
			continue
		}
		cols = append(cols, c)
	}

	var code string
	switch lang {
	case "go":
		code = genGo(tableName, cols)
	case "ts":
		code = genTypeScript(tableName, cols)
	case "rust":
		code = genRust(tableName, cols)
	case "python":
		code = genPython(tableName, cols)
	case "elixir":
		code = genElixir(tableName, cols)
	case "zig":
		code = genZig(tableName, cols)
	default:
		writeError(w, http.StatusBadRequest, "unknown lang: "+lang)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"code": code})
}

// --- Code generators ---

func genGo(table string, cols []colInfo) string {
	typeName := toCamelCase(table)
	var sb strings.Builder
	sb.WriteString("// " + typeName + " maps to the " + table + " table.\n")
	sb.WriteString("type " + typeName + " struct {\n")
	for _, c := range cols {
		field := toCamelCase(c.name)
		goType := pgToGoType(c.dataType, c.udtName, c.nullable)
		tag := fmt.Sprintf("`db:\"%s\" json:\"%s\"`", c.name, c.name)
		sb.WriteString(fmt.Sprintf("\t%-20s %-20s %s\n", field, goType, tag))
	}
	sb.WriteString("}\n")
	return sb.String()
}

func genTypeScript(table string, cols []colInfo) string {
	typeName := toCamelCase(table)
	var sb strings.Builder
	sb.WriteString("export interface " + typeName + " {\n")
	for _, c := range cols {
		tsType := pgToTSType(c.dataType, c.udtName)
		opt := ""
		if c.nullable {
			opt = "?"
		}
		sb.WriteString(fmt.Sprintf("  %s%s: %s\n", c.name, opt, tsType))
	}
	sb.WriteString("}\n")
	return sb.String()
}

func genRust(table string, cols []colInfo) string {
	typeName := toCamelCase(table)
	var sb strings.Builder

	// Scan for needed imports
	needsChrono := false
	needsJsonValue := false
	for _, c := range cols {
		rustType := pgToRustType(c.dataType, c.udtName, c.nullable)
		if strings.Contains(rustType, "chrono::") {
			needsChrono = true
		}
		if strings.Contains(rustType, "serde_json::Value") {
			needsJsonValue = true
		}
	}

	// Emit imports
	if needsChrono {
		sb.WriteString("use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};\n")
	}
	if needsJsonValue {
		sb.WriteString("use serde_json::Value;\n")
	}
	if needsChrono || needsJsonValue {
		sb.WriteString("\n")
	}

	sb.WriteString("#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]\n")
	sb.WriteString("pub struct " + typeName + " {\n")
	for _, c := range cols {
		rustType := pgToRustType(c.dataType, c.udtName, c.nullable)
		sb.WriteString(fmt.Sprintf("    pub %s: %s,\n", c.name, rustType))
	}
	sb.WriteString("}\n")
	return sb.String()
}

func genPython(table string, cols []colInfo) string {
	typeName := toCamelCase(table)
	var sb strings.Builder

	// Scan for needed imports
	needsDatetime := false
	needsList := false
	for _, c := range cols {
		pyType := pgToPythonType(c.dataType, c.udtName, c.nullable)
		if strings.Contains(pyType, "datetime") || strings.Contains(pyType, "date") {
			needsDatetime = true
		}
		if strings.Contains(pyType, "List[") {
			needsList = true
		}
	}

	sb.WriteString("from __future__ import annotations\n")
	sb.WriteString("from typing import Optional")
	if needsList {
		sb.WriteString(", List")
	}
	sb.WriteString("\n")
	if needsDatetime {
		sb.WriteString("from datetime import datetime, date\n")
	}
	sb.WriteString("from pydantic import BaseModel\n\n\n")
	sb.WriteString("class " + typeName + "(BaseModel):\n")
	for _, c := range cols {
		pyType := pgToPythonType(c.dataType, c.udtName, c.nullable)
		sb.WriteString(fmt.Sprintf("    %s: %s\n", c.name, pyType))
	}
	return sb.String()
}

func genElixir(table string, cols []colInfo) string {
	typeName := toCamelCase(table)
	var sb strings.Builder
	sb.WriteString("defmodule MyApp." + typeName + " do\n")
	sb.WriteString("  use Ecto.Schema\n\n")
	sb.WriteString("  schema \"" + table + "\" do\n")
	for _, c := range cols {
		if c.isPK {
			continue // Ecto handles primary key automatically
		}
		elixirType := pgToElixirType(c.dataType, c.udtName)
		fieldLine := "    field :" + c.name + ", " + elixirType
		if c.nullable {
			fieldLine += ", default: nil"
		}
		sb.WriteString(fieldLine + "\n")
	}
	sb.WriteString("  end\n")
	sb.WriteString("end\n")
	return sb.String()
}

func genZig(table string, cols []colInfo) string {
	typeName := toCamelCase(table)
	var sb strings.Builder
	sb.WriteString("pub const " + typeName + " = struct {\n")
	for _, c := range cols {
		zigType := pgToZigType(c.dataType, c.udtName, c.nullable)
		sb.WriteString(fmt.Sprintf("    %s: %s,\n", c.name, zigType))
	}
	sb.WriteString("};\n")
	return sb.String()
}

// --- Type mappings ---

func normalizeType(dataType, udtName string) string {
	if dataType == "USER-DEFINED" {
		return strings.ToLower(udtName)
	}
	return strings.ToLower(dataType)
}

func pgToGoType(dataType, udtName string, nullable bool) string {
	// Handle arrays first
	if dataType == "ARRAY" {
		elemType := strings.TrimPrefix(udtName, "_")
		elemBase := pgToGoType(elemType, elemType, false)
		result := "[]" + elemBase
		if nullable {
			return "*" + result
		}
		return result
	}

	base := func() string {
		switch normalizeType(dataType, udtName) {
		case "int2", "smallint":
			return "int16"
		case "int4", "integer":
			return "int32"
		case "int8", "bigint", "serial", "bigserial":
			return "int64"
		case "float4", "real":
			return "float32"
		case "float8", "double precision", "numeric", "decimal":
			return "float64"
		case "bool", "boolean":
			return "bool"
		case "timestamptz", "timestamp with time zone", "timestamp", "date", "time":
			return "time.Time"
		case "jsonb", "json":
			return "json.RawMessage"
		case "bytea":
			return "[]byte"
		default:
			return "string"
		}
	}()
	if nullable && base != "json.RawMessage" && base != "[]byte" {
		return "*" + base
	}
	return base
}

func pgToTSType(dataType, udtName string) string {
	// Handle arrays first
	if dataType == "ARRAY" {
		elemType := strings.TrimPrefix(udtName, "_")
		elemBase := pgToTSType(elemType, elemType)
		return elemBase + "[]"
	}

	switch normalizeType(dataType, udtName) {
	case "int2", "int4", "int8", "smallint", "integer", "bigint",
		"float4", "float8", "real", "double precision", "numeric", "decimal",
		"serial", "bigserial":
		return "number"
	case "bool", "boolean":
		return "boolean"
	case "timestamptz", "timestamp with time zone", "timestamp", "date":
		return "Date"
	case "jsonb", "json":
		return "unknown"
	case "bytea":
		return "string"
	default:
		return "string"
	}
}

func pgToRustType(dataType, udtName string, nullable bool) string {
	// Handle arrays first
	if dataType == "ARRAY" {
		elemType := strings.TrimPrefix(udtName, "_")
		elemBase := pgToRustType(elemType, elemType, false)
		result := "Vec<" + elemBase + ">"
		if nullable {
			return "Option<" + result + ">"
		}
		return result
	}

	base := func() string {
		switch normalizeType(dataType, udtName) {
		case "int2", "smallint":
			return "i16"
		case "int4", "integer":
			return "i32"
		case "int8", "bigint", "serial", "bigserial":
			return "i64"
		case "float4", "real":
			return "f32"
		case "float8", "double precision", "numeric", "decimal":
			return "f64"
		case "bool", "boolean":
			return "bool"
		case "timestamptz", "timestamp with time zone":
			return "DateTime<Utc>"
		case "timestamp":
			return "NaiveDateTime"
		case "date":
			return "NaiveDate"
		case "jsonb", "json":
			return "Value"
		case "bytea":
			return "Vec<u8>"
		default:
			return "String"
		}
	}()
	if nullable {
		return "Option<" + base + ">"
	}
	return base
}

func pgToPythonType(dataType, udtName string, nullable bool) string {
	base := func() string {
		t := normalizeType(dataType, udtName)
		// Handle arrays first
		if dataType == "ARRAY" {
			elemType := strings.TrimPrefix(udtName, "_")
			elemBase := pgToPythonType(elemType, elemType, false)
			return "List[" + elemBase + "]"
		}
		switch t {
		case "int2", "int4", "int8", "smallint", "integer", "bigint", "serial", "bigserial":
			return "int"
		case "float4", "float8", "real", "double precision", "numeric", "decimal":
			return "float"
		case "bool", "boolean":
			return "bool"
		case "timestamptz", "timestamp with time zone", "timestamp":
			return "datetime"
		case "date":
			return "date"
		case "jsonb", "json":
			return "dict"
		case "bytea":
			return "bytes"
		default:
			return "str"
		}
	}()
	if nullable {
		return "Optional[" + base + "]"
	}
	return base
}

func pgToElixirType(dataType, udtName string) string {
	switch normalizeType(dataType, udtName) {
	case "int2", "int4", "int8", "smallint", "integer", "bigint", "serial", "bigserial":
		return ":integer"
	case "float4", "float8", "real", "double precision", "numeric", "decimal":
		return ":float"
	case "bool", "boolean":
		return ":boolean"
	case "timestamptz", "timestamp with time zone":
		return ":utc_datetime"
	case "timestamp":
		return ":naive_datetime"
	case "date":
		return ":date"
	case "jsonb", "json":
		return ":map"
	case "bytea":
		return ":binary"
	case "uuid":
		return ":binary_id"
	case "array":
		elemType := strings.TrimPrefix(udtName, "_")
		elemBase := pgToElixirType(elemType, elemType)
		return "{:array, " + elemBase + "}"
	default:
		return ":string"
	}
}

func pgToZigType(dataType, udtName string, nullable bool) string {
	base := func() string {
		switch normalizeType(dataType, udtName) {
		case "int2", "smallint":
			return "i16"
		case "int4", "integer":
			return "i32"
		case "int8", "bigint", "serial", "bigserial":
			return "i64"
		case "float4", "real":
			return "f32"
		case "float8", "double precision", "numeric", "decimal":
			return "f64"
		case "bool", "boolean":
			return "bool"
		case "timestamptz", "timestamp with time zone", "timestamp", "date":
			return "i64"
		case "jsonb", "json", "text", "character varying":
			return "[]const u8"
		case "bytea":
			return "[]u8"
		case "uuid":
			return "[]const u8"
		case "array":
			elemType := strings.TrimPrefix(udtName, "_")
			elemBase := pgToZigType(elemType, elemType, false)
			return "[]" + elemBase
		default:
			return "[]const u8"
		}
	}()
	if nullable {
		return "?" + base
	}
	return base
}

// toCamelCase converts snake_case to CamelCase.
func toCamelCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})
	var sb strings.Builder
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		runes := []rune(p)
		runes[0] = unicode.ToUpper(runes[0])
		sb.WriteString(string(runes))
	}
	return sb.String()
}

// GenerateCode generates typed code for a given table's columns.
func GenerateCode(lang, table string, cols []colInfo) (string, error) {
	switch lang {
	case "go":
		return genGo(table, cols), nil
	case "ts":
		return genTypeScript(table, cols), nil
	case "rust":
		return genRust(table, cols), nil
	case "python":
		return genPython(table, cols), nil
	case "elixir":
		return genElixir(table, cols), nil
	case "zig":
		return genZig(table, cols), nil
	default:
		return "", fmt.Errorf("unknown language: %s", lang)
	}
}

// Querier defines the interface for types that can execute queries.
type Querier interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

// FetchColsForTable queries column metadata from a database table.
// It returns the column information needed for code generation.
func FetchColsForTable(ctx context.Context, querier Querier, schema, table string) ([]colInfo, error) {
	rows, err := querier.Query(ctx, `
		SELECT c.column_name, c.data_type, c.udt_name,
		       (c.is_nullable = 'YES'), COALESCE(pk.is_pk, false)
		FROM information_schema.columns c
		LEFT JOIN (
			SELECT ku.column_name, true AS is_pk
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage ku
				ON tc.constraint_name = ku.constraint_name
				AND tc.table_schema = ku.table_schema
				AND tc.table_name = ku.table_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
			  AND tc.table_schema = $1 AND tc.table_name = $2
		) pk ON pk.column_name = c.column_name
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position
	`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []colInfo
	for rows.Next() {
		var c colInfo
		if err := rows.Scan(&c.name, &c.dataType, &c.udtName, &c.nullable, &c.isPK); err != nil {
			continue
		}
		cols = append(cols, c)
	}

	return cols, nil
}
