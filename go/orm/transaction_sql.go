package orm

import (
	"fmt"
	"strings"
)

// validateScopeSQL is conservative statement admission, not a PostgreSQL
// parser or security sandbox. Bound ORM SQL is always admitted. Single raw
// data statements may be used, but ambiguous backslashes in ordinary strings
// are refused rather than relying on caller session string settings.
func validateScopeSQL(sql string) error {
	first := ""
	terminated := false
	for i := 0; i < len(sql); {
		c := sql[i]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			i++
			continue
		}
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2
			depth := 1
			for i < len(sql) && depth > 0 {
				if i+1 < len(sql) && sql[i:i+2] == "/*" {
					depth++
					i += 2
				} else if i+1 < len(sql) && sql[i:i+2] == "*/" {
					depth--
					i += 2
				} else {
					i++
				}
			}
			if depth != 0 {
				return fmt.Errorf("orm: unterminated SQL comment")
			}
			continue
		}
		if terminated {
			return fmt.Errorf("orm: multiple statements forbidden in transaction scope")
		}
		if c == ';' {
			terminated = true
			i++
			continue
		}
		if c == '\'' || c == '"' {
			quoteByte := c
			escaped := c == '\'' && i > 0 && (sql[i-1] == 'E' || sql[i-1] == 'e') && (i < 2 || !wordByte(sql[i-2]))
			i++
			closed := false
			for i < len(sql) {
				if sql[i] == '\\' && quoteByte == '\'' {
					if !escaped {
						return fmt.Errorf("orm: ambiguous ordinary-string backslash forbidden")
					}
					i += 2
					continue
				}
				if sql[i] == quoteByte {
					if i+1 < len(sql) && sql[i+1] == quoteByte {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				i++
			}
			if !closed {
				return fmt.Errorf("orm: unterminated SQL quote")
			}
			continue
		}
		if c == '$' {
			end := i + 1
			for end < len(sql) && wordByte(sql[end]) {
				end++
			}
			if end < len(sql) && sql[end] == '$' {
				delimiter := sql[i : end+1]
				after := strings.Index(sql[end+1:], delimiter)
				if after < 0 {
					return fmt.Errorf("orm: unterminated dollar quote")
				}
				i = end + 1 + after + len(delimiter)
				continue
			}
		}
		if first == "" {
			start := i
			for i < len(sql) && wordByte(sql[i]) {
				i++
			}
			if i == start {
				return fmt.Errorf("orm: data statement keyword required")
			}
			first = strings.ToUpper(sql[start:i])
			continue
		}
		i++
	}
	switch first {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "WITH", "VALUES", "EXPLAIN":
		return nil
	}
	return fmt.Errorf("orm: transaction scope admits single data/query statements only")
}
func wordByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}
