package studio

// X01: the table search endpoint — vector similarity and full-text search
// over a table column, read-only and parameter-bound. This is the search
// journey behind Studio's table view: vectors are not row-editable values,
// but a table that CARRIES them must still be searchable.
//
// POST /api/table/v2/search
//   { connectionId, schema, table, kind: "vector"|"fts", column, query,
//     operator?: "l2"|"cosine"|"inner-product"|"l1", config?: tsconfig,
//     limit?: 1..100 }
//
// Vector queries render `ORDER BY col <op> $1::vector` with pgvector's own
// scoring/ordering semantics (the operator is a fixed vocabulary, never
// caller SQL). FTS queries render to_tsvector/websearch_to_tsquery + ts_rank
// ordering — core PostgreSQL, no extension. A backend without the required
// capability answers with the server's own error, surfaced sanitized.

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

const maxSearchLimit = 100

var searchVectorOperators = map[string]string{
	"l2":            "<->",
	"cosine":        "<=>",
	"inner-product": "<#>",
	"l1":            "<+>",
}

type tableSearchRequestV2 struct {
	ConnectionID string `json:"connectionId"`
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	Kind         string `json:"kind"`
	Column       string `json:"column"`
	Query        string `json:"query"`
	Operator     string `json:"operator,omitempty"`
	Config       string `json:"config,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}

func (s *Server) handleTableSearchV2(w http.ResponseWriter, r *http.Request) {
	var body tableSearchRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	if body.ConnectionID == "" || body.Schema == "" || body.Table == "" || body.Column == "" || body.Query == "" {
		writeDomainError(w, mutationDomainError{msg: "connectionId, schema, table, column, and query are required"})
		return
	}
	limit := body.Limit
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > maxSearchLimit {
		writeDomainError(w, mutationDomainError{msg: fmt.Sprintf("limit must be 1..%d", maxSearchLimit)})
		return
	}
	client, ok := s.clientFor(body.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	meta, err := fetchTableMeta(r.Context(), client, body.Schema, body.Table)
	if err != nil {
		log.Printf("studio: search introspection error: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "introspection failed"})
		return
	}
	if !meta.Exists {
		writeDomainError(w, mutationDomainError{msg: fmt.Sprintf("%s.%s was not found", body.Schema, body.Table)})
		return
	}
	col, exists := meta.Columns[body.Column]
	if !exists {
		writeDomainError(w, mutationDomainError{msg: fmt.Sprintf("column %q does not exist on %s.%s", body.Column, body.Schema, body.Table)})
		return
	}

	tableRef := fmt.Sprintf("%s.%s", quoteIdent(body.Schema), quoteIdent(body.Table))
	colRef := quoteIdent(body.Column)

	switch body.Kind {
	case "vector":
		if col.TypeName != "vector" {
			writeDomainError(w, mutationDomainError{msg: fmt.Sprintf("column %q is %s, not vector", body.Column, col.TypeName)})
			return
		}
		op := "<=>"
		if body.Operator != "" {
			symbol, ok := searchVectorOperators[body.Operator]
			if !ok {
				writeDomainError(w, mutationDomainError{msg: `operator must be one of "l2", "cosine", "inner-product", "l1"`})
				return
			}
			op = symbol
		}
		vec, err := parseSearchVector(body.Query)
		if err != nil {
			writeDomainError(w, mutationDomainError{msg: err.Error()})
			return
		}
		sqlText := fmt.Sprintf(`SELECT * FROM %s ORDER BY %s %s $1::vector LIMIT %d`, tableRef, colRef, op, limit)
		s.runSearchQuery(w, r, client, sqlText, vec)
	case "fts":
		if col.TypeName != "text" && col.TypeName != "varchar" && col.TypeName != "tsvector" {
			writeDomainError(w, mutationDomainError{msg: fmt.Sprintf("column %q is %s; full-text search applies to text, varchar, and tsvector columns", body.Column, col.TypeName)})
			return
		}
		config := body.Config
		if config == "" {
			config = "english"
		}
		if !isPlainConfigName(config) {
			writeDomainError(w, mutationDomainError{msg: `config must be a plain lowercase text-search configuration name (e.g. "english", "simple")`})
			return
		}
		var sqlText string
		var args []any
		if col.TypeName == "tsvector" {
			// A stored tsvector column matches directly.
			sqlText = fmt.Sprintf(
				`SELECT * FROM %s WHERE %s @@ websearch_to_tsquery($1::text::regconfig, $2) ORDER BY ts_rank(%s, websearch_to_tsquery($1::text::regconfig, $2)) DESC LIMIT %d`,
				tableRef, colRef, colRef, limit)
			args = []any{config, body.Query}
		} else {
			sqlText = fmt.Sprintf(
				`SELECT * FROM %s WHERE to_tsvector($1::text::regconfig, %s) @@ websearch_to_tsquery($1::text::regconfig, $2) ORDER BY ts_rank(to_tsvector($1::text::regconfig, %s), websearch_to_tsquery($1::text::regconfig, $2)) DESC LIMIT %d`,
				tableRef, colRef, colRef, limit)
			args = []any{config, body.Query}
		}
		s.runSearchQuery(w, r, client, sqlText, args...)
	default:
		writeDomainError(w, mutationDomainError{msg: `kind must be "vector" or "fts"`})
	}
}

// runSearchQuery executes the parameter-bound search and streams the tagged
// rows. Server errors (missing pgvector extension, wrong vector dimension,
// bad tsvector literal) surface sanitized — Studio reports them, never
// works around them.
func (s *Server) runSearchQuery(w http.ResponseWriter, r *http.Request, client interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, sqlText string, args ...any) {
	rows, err := client.Query(r.Context(), sqlText, args...)
	if err != nil {
		log.Printf("studio: search query error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"columns": []string{}, "rows": [][]any{}, "rowCount": 0, "error": sanitizeError(err)})
		return
	}
	defer rows.Close()
	result, err := collectTaggedRows(rows)
	if err != nil {
		log.Printf("studio: search read error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"columns": []string{}, "rows": [][]any{}, "rowCount": 0, "error": sanitizeError(err)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"columns": result.columns, "rows": result.data, "rowCount": len(result.data)})
}

// parseSearchVector accepts the JSON array form ("[0.1,0.2]" or "0.1,0.2")
// and returns the exact pgvector literal text. Finite numbers only; the
// dimension contract belongs to the column and is enforced by the server.
func parseSearchVector(query string) (string, error) {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.TrimPrefix(strings.TrimSuffix(trimmed, "]"), "[")
	if trimmed == "" {
		return "", fmt.Errorf("query vector is empty — provide numbers, e.g. [0.1, 0.2, 0.3]")
	}
	parts := strings.Split(trimmed, ",")
	nums := make([]string, 0, len(parts))
	for _, p := range parts {
		var f float64
		if _, err := fmt.Sscanf(strings.TrimSpace(p), "%g", &f); err != nil {
			return "", fmt.Errorf("query vector elements must be numbers, got %q", strings.TrimSpace(p))
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "", fmt.Errorf("query vector elements must be finite numbers")
		}
		nums = append(nums, strings.TrimSpace(p))
	}
	return "[" + strings.Join(nums, ",") + "]", nil
}

func isPlainConfigName(config string) bool {
	if len(config) == 0 || len(config) > 63 {
		return false
	}
	for i, c := range config {
		if c == '_' || (c >= 'a' && c <= 'z') || (i > 0 && c >= '0' && c <= '9') {
			continue
		}
		return false
	}
	return true
}
