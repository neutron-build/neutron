package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// Row-level editing endpoints for the SQL browser: PK-aware UPDATE/DELETE
// plus the FK map that powers "follow reference" navigation. All identifiers
// are quoted; all values are bound parameters.
//
// Interim containment (B04): the backend re-introspects the table's key
// metadata from the database on EVERY mutation request and permits a write
// only when the table has a proven single-column primary key and the request
// addresses rows by exactly that key. Composite-PK and no-key tables are
// read-only; forged pkColumn claims are rejected before any SQL runs; each
// mutation is one guarded atomic statement that can only ever affect exactly
// one row. Full multi-column key identities are the S01 protocol.

type updateRowRequest struct {
	ConnectionID string `json:"connectionId"`
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	PKColumn     string `json:"pkColumn"`
	PKValue      any    `json:"pkValue"`
	Column       string `json:"column"`
	Value        any    `json:"value"`
	IsNull       bool   `json:"isNull"`
}

func (s *Server) handleTableRowUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body updateRowRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if body.ConnectionID == "" || body.Schema == "" || body.Table == "" ||
		body.PKColumn == "" || body.Column == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema, table, pkColumn, and column are required")
		return
	}
	if !body.IsNull && body.Value == nil {
		writeError(w, http.StatusBadRequest, "value is required (or set isNull=true for SQL NULL)")
		return
	}

	client, ok := s.clientFor(body.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}

	meta, err := fetchTableMeta(r.Context(), client, body.Schema, body.Table)
	if err != nil {
		log.Printf("studio: key introspection error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"error": sanitizeError(err)})
		return
	}
	pk, reject := requireSingleColumnKey(meta, body.Schema, body.Table, body.PKColumn)
	if reject != "" {
		writeJSON(w, http.StatusOK, map[string]any{"error": reject})
		return
	}
	target, exists := meta.Columns[body.Column]
	if !exists {
		writeJSON(w, http.StatusOK, map[string]any{"error": fmt.Sprintf(
			"column %q does not exist on %s.%s; mutation rejected", body.Column, body.Schema, body.Table)})
		return
	}
	if body.Column == pk && target.autoAssigned() {
		writeJSON(w, http.StatusOK, map[string]any{"error": fmt.Sprintf(
			"primary key column %q is generated/identity/auto-assigned and read-only", body.Column)})
		return
	}

	sqlText, args := buildUpdateExactlyOne(body.Schema, body.Table, pk, body.Column, body.IsNull, body.Value, body.PKValue)
	n, err := mutateExactlyOne(r.Context(), client, sqlText, args...)
	if err != nil {
		log.Printf("studio: row update error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"error": sanitizeError(err)})
		return
	}
	if n != 1 {
		writeJSON(w, http.StatusOK, map[string]any{"error": s.explainMissedRow(r.Context(), client, body.Schema, body.Table, pk, body.PKValue, "update")})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rowsAffected": n})
}

type deleteRowRequest struct {
	ConnectionID string `json:"connectionId"`
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	PKColumn     string `json:"pkColumn"`
	PKValue      any    `json:"pkValue"`
}

func (s *Server) handleTableRowDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body deleteRowRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if body.ConnectionID == "" || body.Schema == "" || body.Table == "" || body.PKColumn == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema, table, and pkColumn are required")
		return
	}

	client, ok := s.clientFor(body.ConnectionID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}

	meta, err := fetchTableMeta(r.Context(), client, body.Schema, body.Table)
	if err != nil {
		log.Printf("studio: key introspection error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"error": sanitizeError(err)})
		return
	}
	pk, reject := requireSingleColumnKey(meta, body.Schema, body.Table, body.PKColumn)
	if reject != "" {
		writeJSON(w, http.StatusOK, map[string]any{"error": reject})
		return
	}

	sqlText, args := buildDeleteExactlyOne(body.Schema, body.Table, pk, body.PKValue)
	n, err := mutateExactlyOne(r.Context(), client, sqlText, args...)
	if err != nil {
		log.Printf("studio: row delete error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"error": sanitizeError(err)})
		return
	}
	if n != 1 {
		writeJSON(w, http.StatusOK, map[string]any{"error": s.explainMissedRow(r.Context(), client, body.Schema, body.Table, pk, body.PKValue, "delete")})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rowsAffected": n})
}

// tableColumnMeta is one introspected column of a table.
type tableColumnMeta struct {
	Name        string
	IsPK        bool
	Identity    string // attidentity: '' | 'a' (always) | 'd' (by default)
	Generated   string // attgenerated: '' | 's' (stored)
	DefaultExpr string
}

// autoAssigned reports whether the column's value is generated by the server
// (identity, stored generated, or a nextval/serial default).
func (m tableColumnMeta) autoAssigned() bool {
	return m.Identity != "" || m.Generated != "" ||
		strings.HasPrefix(strings.ToLower(m.DefaultExpr), "nextval(")
}

// tableMeta is the authoritative, freshly introspected shape of a table.
type tableMeta struct {
	Exists  bool
	PKCols  []string
	Columns map[string]tableColumnMeta
}

// tableMetaSQL reads the table's columns, primary-key membership and
// generated/identity/default flags from pg_catalog. Studio-owned introspection
// (kept here rather than in internal/db): mutations must re-verify key
// metadata at request time, independent of what the browser claims.
const tableMetaSQL = `
SELECT a.attname,
       EXISTS (
	       SELECT 1 FROM pg_catalog.pg_index i
	       WHERE i.indrelid = c.oid AND i.indisprimary AND a.attnum = ANY(i.indkey)
       ) AS is_pk,
       COALESCE(a.attidentity::text, ''),
       COALESCE(a.attgenerated::text, ''),
       COALESCE(pg_catalog.pg_get_expr(d.adbin, d.adrelid), '')
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r','p')
ORDER BY a.attnum
`

// fetchTableMeta re-introspects a table from the database. Zero columns means
// the relation does not exist (or is not an ordinary table).
func fetchTableMeta(ctx context.Context, client *db.Client, schemaName, tableName string) (*tableMeta, error) {
	rows, err := client.Query(ctx, tableMetaSQL, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	meta := &tableMeta{Columns: map[string]tableColumnMeta{}}
	for rows.Next() {
		var col tableColumnMeta
		if err := rows.Scan(&col.Name, &col.IsPK, &col.Identity, &col.Generated, &col.DefaultExpr); err != nil {
			return nil, err
		}
		meta.Columns[col.Name] = col
		if col.IsPK {
			meta.PKCols = append(meta.PKCols, col.Name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	meta.Exists = len(meta.Columns) > 0
	return meta, nil
}

// requireSingleColumnKey enforces the interim key contract against freshly
// introspected metadata. It returns the proven single-column PK name, or a
// rejection message explaining why the table is read-only or the request's
// key claim is forged.
func requireSingleColumnKey(meta *tableMeta, schemaName, tableName, claimedPK string) (string, string) {
	if !meta.Exists {
		return "", fmt.Sprintf("table %s.%s was not found or is not an ordinary table; mutations are rejected", schemaName, tableName)
	}
	if len(meta.PKCols) == 0 {
		return "", fmt.Sprintf("table %s.%s has no primary key; rows are read-only until full row identities land", schemaName, tableName)
	}
	if len(meta.PKCols) > 1 {
		return "", fmt.Sprintf("table %s.%s has a composite primary key (%s); row edits need the full key tuple and are read-only in this interim version",
			schemaName, tableName, strings.Join(meta.PKCols, ", "))
	}
	pk := meta.PKCols[0]
	if claimedPK != pk {
		return "", fmt.Sprintf("pkColumn %q is not the primary key of %s.%s (expected %q); mutation rejected",
			claimedPK, schemaName, tableName, pk)
	}
	return pk, ""
}

// buildUpdateExactlyOne builds a guarded UPDATE. The scalar count subquery in
// the WHERE clause is evaluated in the same statement snapshot as the write:
// the UPDATE only fires when exactly one row matches the key, so a stale or
// (on an engine that fails to enforce key uniqueness) multi-row request can
// never partially apply. Parameters are bound values.
func buildUpdateExactlyOne(schemaName, tableName, pkColumn, column string, isNull bool, value, pkValue any) (string, []any) {
	tableRef := fmt.Sprintf("%s.%s", quoteIdent(schemaName), quoteIdent(tableName))
	key := quoteIdent(pkColumn)
	if isNull {
		return fmt.Sprintf(
			"UPDATE %s SET %s = NULL WHERE %s = $1 AND (SELECT count(*) FROM %s WHERE %s = $1) = 1",
			tableRef, quoteIdent(column), key, tableRef, key,
		), []any{pkValue}
	}
	return fmt.Sprintf(
		"UPDATE %s SET %s = $1 WHERE %s = $2 AND (SELECT count(*) FROM %s WHERE %s = $2) = 1",
		tableRef, quoteIdent(column), key, tableRef, key,
	), []any{value, pkValue}
}

// buildDeleteExactlyOne builds the DELETE counterpart of the guarded UPDATE.
func buildDeleteExactlyOne(schemaName, tableName, pkColumn string, pkValue any) (string, []any) {
	tableRef := fmt.Sprintf("%s.%s", quoteIdent(schemaName), quoteIdent(tableName))
	key := quoteIdent(pkColumn)
	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s = $1 AND (SELECT count(*) FROM %s WHERE %s = $1) = 1",
		tableRef, key, tableRef, key,
	), []any{pkValue}
}

// mutateExactlyOne executes the guarded mutation as ONE atomic statement (an
// implicit transaction) and returns the number of rows it actually changed —
// 1 on success, 0 when the guard suppressed the write.
func mutateExactlyOne(ctx context.Context, client *db.Client, mutationSQL string, args ...any) (int64, error) {
	var n int64
	err := client.QueryRow(ctx, fmt.Sprintf("WITH mutated AS (%s RETURNING 1) SELECT count(*) FROM mutated", mutationSQL), args...).Scan(&n)
	return n, err
}

// explainMissedRow distinguishes a stale/missing key from a key that (only
// possible on engines that do not enforce PK uniqueness) matches several rows.
// The message deliberately does not distinguish "row exists but is not
// visible" (RLS) from "row does not exist".
func (s *Server) explainMissedRow(ctx context.Context, client *db.Client, schemaName, tableName, pkColumn string, pkValue any, verb string) string {
	var matches int64
	err := client.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s.%s WHERE %s = $1",
		quoteIdent(schemaName), quoteIdent(tableName), quoteIdent(pkColumn),
	), pkValue).Scan(&matches)
	if err != nil || matches == 0 {
		return fmt.Sprintf("%s matched no row for %s = %v: the row is stale, deleted, or not visible to this connection", verb, pkColumn, pkValue)
	}
	return fmt.Sprintf("%s refused: key %s = %v matches %d rows, expected exactly one", verb, pkColumn, pkValue, matches)
}

type fkDetail struct {
	Column    string `json:"column"`
	RefSchema string `json:"refSchema"`
	RefTable  string `json:"refTable"`
	RefColumn string `json:"refColumn"`
}

func (s *Server) handleTableFKs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	connID := q.Get("connectionId")
	schemaName := q.Get("schema")
	tableName := q.Get("table")
	if connID == "" || schemaName == "" || tableName == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema, and table are required")
		return
	}
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}

	fks, err := tableForeignKeys(r.Context(), client, schemaName, tableName)
	if err != nil {
		log.Printf("studio: fk query error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"fks": []fkDetail{}, "error": sanitizeError(err)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"fks": fks})
}

func tableForeignKeys(ctx context.Context, client *db.Client, schemaName, tableName string) ([]fkDetail, error) {
	const fkSQL = `
SELECT fa.attname AS concol,
       nf.nspname AS refschema,
       rt.relname AS reftable,
       ta.attname AS refcol
FROM pg_constraint rc
JOIN pg_class cl ON cl.oid = rc.conrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = $1
JOIN pg_class rt ON rt.oid = rc.confrelid
JOIN pg_namespace nf ON nf.oid = rt.relnamespace
CROSS JOIN LATERAL unnest(rc.conkey, rc.confkey) AS k(cnum, rnum)
JOIN pg_attribute fa ON fa.attrelid = cl.oid AND fa.attnum = k.cnum
JOIN pg_attribute ta ON ta.attrelid = rt.oid AND ta.attnum = k.rnum
WHERE rc.contype = 'f' AND cl.relname = $2
ORDER BY fa.attname
`
	rows, err := client.Query(ctx, fkSQL, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []fkDetail
	for rows.Next() {
		var fk fkDetail
		if err := rows.Scan(&fk.Column, &fk.RefSchema, &fk.RefTable, &fk.RefColumn); err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}
