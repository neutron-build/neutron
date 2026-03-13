package neutroncli

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"unicode"
)

func cmdGen(schemaPath string) int {
	f, err := os.Open(schemaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	defer f.Close()

	tables, err := parseSQL(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing schema: %v\n", err)
		return 1
	}

	if len(tables) == 0 {
		fmt.Fprintln(os.Stderr, "No CREATE TABLE statements found")
		return 1
	}

	code := generateStructs(tables)
	fmt.Print(code)
	return 0
}

type sqlTable struct {
	Name    string
	Columns []sqlColumn
}

type sqlColumn struct {
	Name     string
	Type     string
	Nullable bool
	PK       bool
}

var createTableRe = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)`)
var columnRe = regexp.MustCompile(`^\s+(\w+)\s+(\w+(?:\s*\([^)]*\))?)(.*)$`)

func parseSQL(f *os.File) ([]sqlTable, error) {
	scanner := bufio.NewScanner(f)
	var tables []sqlTable
	var current *sqlTable

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		// Check for CREATE TABLE
		if m := createTableRe.FindStringSubmatch(line); m != nil {
			tables = append(tables, sqlTable{Name: m[1]})
			current = &tables[len(tables)-1]
			continue
		}

		// Check for end of table
		if current != nil && (strings.HasPrefix(line, ");") || line == ")") {
			current = nil
			continue
		}

		// Parse column
		if current != nil {
			if m := columnRe.FindStringSubmatch(scanner.Text()); m != nil {
				colName := m[1]
				colType := strings.TrimSpace(m[2])
				rest := strings.ToUpper(m[3])

				// Skip constraints that look like columns
				upper := strings.ToUpper(colName)
				if upper == "PRIMARY" || upper == "UNIQUE" || upper == "CHECK" ||
					upper == "CONSTRAINT" || upper == "FOREIGN" || upper == "INDEX" {
					continue
				}

				col := sqlColumn{
					Name:     colName,
					Type:     colType,
					Nullable: !strings.Contains(rest, "NOT NULL"),
					PK:       strings.Contains(rest, "PRIMARY KEY"),
				}
				current.Columns = append(current.Columns, col)
			}
		}
	}

	return tables, scanner.Err()
}

func generateStructs(tables []sqlTable) string {
	var b strings.Builder
	b.WriteString("package model\n\n")

	needsTime := false
	for _, t := range tables {
		for _, c := range t.Columns {
			goType := sqlToGoType(c.Type, c.Nullable)
			if strings.Contains(goType, "time.Time") {
				needsTime = true
			}
		}
	}

	if needsTime {
		b.WriteString("import \"time\"\n\n")
	}

	for i, t := range tables {
		structName := toGoName(t.Name)
		b.WriteString(fmt.Sprintf("type %s struct {\n", structName))

		for _, c := range t.Columns {
			goName := toGoName(c.Name)
			goType := sqlToGoType(c.Type, c.Nullable)
			b.WriteString(fmt.Sprintf("\t%s %s `json:\"%s\" db:\"%s\"`\n",
				goName, goType, c.Name, c.Name))
		}

		b.WriteString("}\n")
		if i < len(tables)-1 {
			b.WriteString("\n")
		}
	}

	return b.String()
}

func sqlToGoType(sqlType string, nullable bool) string {
	upper := strings.ToUpper(sqlType)

	// Strip parenthetical parts for matching
	if idx := strings.Index(upper, "("); idx >= 0 {
		upper = strings.TrimSpace(upper[:idx])
	}

	var goType string
	switch upper {
	case "INT", "INTEGER", "INT4":
		goType = "int32"
	case "BIGINT", "INT8", "BIGSERIAL", "SERIAL8":
		goType = "int64"
	case "SMALLINT", "INT2":
		goType = "int16"
	case "SERIAL":
		goType = "int32"
	case "BOOLEAN", "BOOL":
		goType = "bool"
	case "REAL", "FLOAT4":
		goType = "float32"
	case "DOUBLE", "FLOAT8", "NUMERIC", "DECIMAL":
		goType = "float64"
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER", "UUID", "CITEXT":
		goType = "string"
	case "BYTEA":
		goType = "[]byte"
	case "TIMESTAMP", "TIMESTAMPTZ", "DATE", "TIME", "TIMETZ":
		goType = "time.Time"
	case "JSONB", "JSON":
		goType = "json.RawMessage"
	case "VECTOR":
		goType = "[]float32"
	default:
		goType = "any"
	}

	if nullable && goType != "[]byte" && goType != "any" && !strings.HasPrefix(goType, "[]") {
		goType = "*" + goType
	}

	return goType
}

func toGoName(s string) string {
	parts := strings.Split(s, "_")
	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		// Common acronyms
		upper := strings.ToUpper(p)
		switch upper {
		case "ID", "URL", "HTTP", "API", "SQL", "IP", "UUID", "JSON", "XML",
			"HTML", "CSS", "TCP", "UDP", "DNS", "SSH", "TLS", "SSL", "FTP":
			b.WriteString(upper)
		default:
			runes := []rune(p)
			runes[0] = unicode.ToUpper(runes[0])
			b.WriteString(string(runes))
		}
	}
	return b.String()
}
