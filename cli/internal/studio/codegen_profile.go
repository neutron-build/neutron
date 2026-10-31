package studio

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

const LegacyProfile = "legacy"
const LosslessReadProfile = "lossless-read-v1"

// ValidateCodegenProfile never silently falls back from an unknown profile.
func ValidateCodegenProfile(profile, lang string) error {
	if profile == "" || profile == LegacyProfile {
		switch lang {
		case "go", "ts", "python", "rust", "elixir", "zig":
			return nil
		}
	} else if profile == LosslessReadProfile {
		switch lang {
		case "go", "ts", "python":
			return nil
		}
	} else {
		return fmt.Errorf("unknown codegen profile %q", profile)
	}
	return fmt.Errorf("profile %q: unsupported language %q", profile, lang)
}

// ValidateCodegenBatch checks the shared namespace of selected-profile output
// before callers write any files. All supported languages emit CamelCase types.
func ValidateCodegenBatch(profile, lang string, tables []string) error {
	if err := ValidateCodegenProfile(profile, lang); err != nil {
		return err
	}
	if profile == "" || profile == LegacyProfile {
		return nil
	}
	symbols, files := map[string]string{}, map[string]string{}
	ext := map[string]string{"go": ".go", "ts": ".ts", "python": ".py"}[lang]
	for _, table := range tables {
		symbol := toCamelCase(table)
		if !validReadName(table) || !validReadName(symbol) {
			return fmt.Errorf("%s %s: invalid or reserved table identifier %q", lang, table, table)
		}
		if previous, ok := symbols[symbol]; ok {
			return fmt.Errorf("%s tables %q and %q emit colliding symbol %q", lang, previous, table, symbol)
		}
		symbols[symbol] = table
		filename := table + ext
		folded := strings.ToLower(filename)
		if previous, ok := files[folded]; ok {
			return fmt.Errorf("%s tables %q and %q emit case-insensitive colliding filenames %q and %q", lang, previous, table, previous+ext, filename)
		}
		files[folded] = table
	}
	return nil
}

// FetchColsForProfile leaves the historical metadata path unchanged. Selected
// lossless generation requires actual catalog identity, including domains.
func FetchColsForProfile(ctx context.Context, q Querier, schema, table, profile string) ([]colInfo, error) {
	if profile == "" || profile == LegacyProfile {
		return FetchColsForTable(ctx, q, schema, table)
	}
	if profile != LosslessReadProfile {
		return nil, fmt.Errorf("unknown codegen profile %q", profile)
	}
	rows, err := q.Query(ctx, `SELECT a.attname, t.typname, tn.nspname, t.typtype::text,
 a.atttypid::bigint, NOT a.attnotnull
 FROM pg_catalog.pg_attribute a
 JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
 JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace
 WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind IN ('r','p','v','m','f')
 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("%s.%s catalog metadata: %w", schema, table, err)
	}
	defer rows.Close()
	var cols []colInfo
	for rows.Next() {
		var c colInfo
		if err := rows.Scan(&c.name, &c.udtName, &c.typeNamespace, &c.typeKind, &c.typeOID, &c.nullable); err != nil {
			return nil, fmt.Errorf("%s.%s catalog metadata scan: %w", schema, table, err)
		}
		c.dataType = c.udtName
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s.%s catalog metadata rows: %w", schema, table, err)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("%s.%s: no column identity metadata", schema, table)
	}
	return cols, nil
}

type readScalar struct{ name, ts, py, goType string }

var readScalars = map[int64]readScalar{
	21: {"int2", "number", "int", "int16"}, 23: {"int4", "number", "int", "int32"}, 20: {"int8", "string", "int", "int64"},
	1700: {"numeric", "string", "Decimal", "string"}, 16: {"bool", "boolean", "bool", "bool"}, 25: {"text", "string", "str", "string"},
	1043: {"varchar", "string", "str", "string"}, 1042: {"bpchar", "string", "str", "string"}, 2950: {"uuid", "string", "UUID", "string"}, 17: {"bytea", "Uint8Array", "bytes", "[]byte"},
}
var readIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

// Reject identifiers requiring escaping or renaming; this profile never silently
// changes physical names or permits generated field/import collisions.
var readReserved = strings.Fields("break case chan const continue default defer else fallthrough for func go goto if import interface map package range return select struct switch type var class def del elif except finally from global in is lambda nonlocal not or pass raise try while with yield async await and as assert False None True let export extends implements new private protected public static super this throw typeof void delete do enum instanceof null true false model_config BaseModel Decimal UUID Optional ConfigDict schema dict json validate model_dump model_validate")

func validReadName(name string) bool {
	if !readIdentifier.MatchString(name) {
		return false
	}
	for _, word := range readReserved {
		if name == word {
			return false
		}
	}
	return !strings.HasPrefix(name, "model_")
}

// GenerateCodeProfile returns a SELECT/read representation, not an insert or
// update contract. Legacy output is deliberately byte-for-byte unchanged.
func GenerateCodeProfile(profile, lang, table string, cols []colInfo) (string, error) {
	if err := ValidateCodegenProfile(profile, lang); err != nil {
		return "", err
	}
	if profile == "" || profile == LegacyProfile {
		return GenerateCode(lang, table, cols)
	}
	typeName := toCamelCase(table)
	if !validReadName(table) || !validReadName(typeName) {
		return "", fmt.Errorf("%s %s: invalid or reserved table identifier %q", lang, table, table)
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("%s %s: no selected columns", lang, table)
	}
	seen := map[string]bool{}
	types := make([]readScalar, len(cols))
	numeric := false
	uuid := false
	for i, c := range cols {
		context := fmt.Sprintf("%s %s column %q type %s.%s (OID %d, kind %q)", lang, table, c.name, c.typeNamespace, c.udtName, c.typeOID, c.typeKind)
		typ, ok := readScalars[c.typeOID]
		if !ok || c.typeNamespace != "pg_catalog" || c.typeKind != "b" || c.udtName != typ.name {
			return "", fmt.Errorf("%s: unsupported identity for %s", context, profile)
		}
		name := c.name
		if lang == "go" {
			name = toCamelCase(name)
		}
		if !validReadName(c.name) || !validReadName(name) || seen[name] {
			return "", fmt.Errorf("%s: invalid, reserved or colliding identifier", context)
		}
		seen[name] = true
		types[i] = typ
		numeric = numeric || c.typeOID == 1700
		uuid = uuid || c.typeOID == 2950
	}
	var b strings.Builder
	switch lang {
	case "ts":
		b.WriteString("export interface " + typeName + " {\n")
	case "go":
		b.WriteString("// " + typeName + " is a selected row from " + table + " (" + profile + ").\n")
		b.WriteString("type " + typeName + " struct {\n")
	case "python":
		b.WriteString("from __future__ import annotations\nfrom typing import Optional\n")
		if uuid {
			b.WriteString("from uuid import UUID\n")
		}
		if numeric {
			b.WriteString("from decimal import Decimal\nfrom pydantic import BaseModel, ConfigDict\n")
		} else {
			b.WriteString("from pydantic import BaseModel\n")
		}
		b.WriteString("\n\nclass " + typeName + "(BaseModel):\n")
		if numeric {
			b.WriteString("    model_config = ConfigDict(allow_inf_nan=True)\n\n")
		}
	}
	for i, c := range cols {
		t := types[i]
		switch lang {
		case "ts":
			typ := t.ts
			if c.nullable {
				typ += " | null"
			}
			fmt.Fprintf(&b, "  %s: %s\n", c.name, typ)
		case "python":
			typ := t.py
			if c.nullable {
				typ = "Optional[" + typ + "]"
			}
			fmt.Fprintf(&b, "    %s: %s\n", c.name, typ)
		case "go":
			typ := t.goType
			if c.nullable {
				typ = "*" + typ
			}
			fmt.Fprintf(&b, "\t%s %s `db:%q json:%q`\n", toCamelCase(c.name), typ, c.name, c.name)
		}
	}
	if lang != "python" {
		b.WriteString("}\n")
	}
	return b.String(), nil
}
