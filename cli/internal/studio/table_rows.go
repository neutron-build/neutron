package studio

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
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
// one row. Full multi-column key identities are the S01 protocol
// (rows_v2.go); these interim endpoints stay session-guarded and precision-
// safe for direct clients during the transition, but the SPA no longer uses
// them.

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
	if !s.requireMutationAuth(w, r) {
		return
	}
	var body updateRowRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxMutationBody)
	// Numbers stay exact (json.Number): a float64 decode would round an int8
	// key beyond 2^53 onto a DIFFERENT row's key.
	if err := decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	pkValue, perr := decodeWireValue(body.PKValue)
	value, verr := decodeWireValue(body.Value)
	if perr != nil || verr != nil {
		writeError(w, http.StatusBadRequest, "pkValue and value must be scalars or tagged wire cells")
		return
	}
	body.PKValue, body.Value = pkValue, value
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
	if !s.requireMutationAuth(w, r) {
		return
	}
	var body deleteRowRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxMutationBody)
	if err := decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	pkValue, perr := decodeWireValue(body.PKValue)
	if perr != nil {
		writeError(w, http.StatusBadRequest, "pkValue must be a scalar or tagged wire cell")
		return
	}
	body.PKValue = pkValue
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
	TypeOID     uint32 // pg_type.oid — drives the wire tag
	TypeName    string // pg_type.typname
	TypType     string // pg_type.typtype: 'b' base, 'e' enum, 'd' domain, ...
	NotNull     bool
	KeyPos      int  // 1-based position in the primary key, 0 when not a key column
	CanUpdate   bool // has_column_privilege(..., 'UPDATE') for the current role
	CanInsert   bool // has_column_privilege(..., 'INSERT') for the current role
}

// autoAssigned reports whether the column's value is generated by the server
// (identity, stored generated, or a nextval/serial default).
func (m tableColumnMeta) autoAssigned() bool {
	return m.Identity != "" || m.Generated != "" ||
		strings.HasPrefix(strings.ToLower(m.DefaultExpr), "nextval(")
}

// tableMeta is the authoritative, freshly introspected shape of a table.
type tableMeta struct {
	Exists    bool
	RelOID    uint32   // pg_class.oid: binds identities to this exact relation
	PKCols    []string // primary-key columns in constraint (index key) order
	Columns   map[string]tableColumnMeta
	Order     []tableColumnMeta // attnum order (map iteration is random)
	CanDelete bool              // has_table_privilege(..., 'DELETE')
}

// tableMetaSQL reads the table's columns, primary-key membership and key
// position, type, generated/identity/default flags and the current role's
// privileges from pg_catalog. Studio-owned introspection
// (kept here rather than in internal/db): mutations must re-verify key
// metadata at request time, independent of what the browser claims.
const tableMetaSQL = `
SELECT a.attname,
       COALESCE((
	       SELECT u.ord
	       FROM pg_catalog.pg_index i
	       CROSS JOIN LATERAL pg_catalog.unnest(i.indkey) WITH ORDINALITY AS u(k, ord)
	       WHERE i.indrelid = c.oid AND i.indisprimary AND u.k = a.attnum
       ), 0) AS key_pos,
       COALESCE(a.attidentity::text, ''),
       COALESCE(a.attgenerated::text, ''),
       COALESCE(pg_catalog.pg_get_expr(d.adbin, d.adrelid), ''),
       a.atttypid,
       t.typname,
       t.typtype::text,
       a.attnotnull,
       c.oid,
       pg_catalog.has_column_privilege(c.oid, a.attnum, 'UPDATE'),
       pg_catalog.has_column_privilege(c.oid, a.attnum, 'INSERT'),
       pg_catalog.has_table_privilege(c.oid, 'DELETE')
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
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
	var keyed []tableColumnMeta
	for rows.Next() {
		var col tableColumnMeta
		var keyPos int32
		if err := rows.Scan(&col.Name, &keyPos, &col.Identity, &col.Generated, &col.DefaultExpr,
			&col.TypeOID, &col.TypeName, &col.TypType, &col.NotNull,
			&meta.RelOID, &col.CanUpdate, &col.CanInsert, &meta.CanDelete); err != nil {
			return nil, err
		}
		col.KeyPos = int(keyPos)
		col.IsPK = col.KeyPos > 0
		meta.Columns[col.Name] = col
		meta.Order = append(meta.Order, col)
		if col.IsPK {
			keyed = append(keyed, col)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(keyed, func(i, j int) bool { return keyed[i].KeyPos < keyed[j].KeyPos })
	for _, col := range keyed {
		meta.PKCols = append(meta.PKCols, col.Name)
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

// fkDetail describes one foreign key constraint with its COMPLETE key
// tuples: composite FKs carry every (column, refColumn) pair in constraint
// order, so FK navigation and editing address the whole tuple, never one
// component. Single-column FKs additionally expose the legacy per-column
// fields for existing consumers.
type fkDetail struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	RefSchema  string   `json:"refSchema"`
	RefTable   string   `json:"refTable"`
	RefColumns []string `json:"refColumns"`
	Composite  bool     `json:"composite"`
	// Legacy per-column spelling, set only when len(Columns) == 1.
	Column    string `json:"column,omitempty"`
	RefColumn string `json:"refColumn,omitempty"`
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
	// One row per constraint; the LATERAL unnest keeps conkey/confkey pairs
	// aligned (pair i of the local tuple maps to pair i of the target tuple)
	// and array_agg preserves the in-constraint order.
	const fkSQL = `
SELECT rc.conname,
       nf.nspname AS refschema,
       rt.relname AS reftable,
       array_agg(fa.attname ORDER BY k.ord) AS concols,
       array_agg(ta.attname ORDER BY k.ord) AS refcols
FROM pg_constraint rc
JOIN pg_class cl ON cl.oid = rc.conrelid
JOIN pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = $1
JOIN pg_class rt ON rt.oid = rc.confrelid
JOIN pg_namespace nf ON nf.oid = rt.relnamespace
CROSS JOIN LATERAL unnest(rc.conkey, rc.confkey) WITH ORDINALITY AS k(cnum, rnum, ord)
JOIN pg_attribute fa ON fa.attrelid = cl.oid AND fa.attnum = k.cnum
JOIN pg_attribute ta ON ta.attrelid = rt.oid AND ta.attnum = k.rnum
WHERE rc.contype = 'f' AND cl.relname = $2
GROUP BY rc.conname, nf.nspname, rt.relname, rc.oid
ORDER BY rc.conname
`
	rows, err := client.Query(ctx, fkSQL, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []fkDetail
	for rows.Next() {
		var fk fkDetail
		if err := rows.Scan(&fk.Name, &fk.RefSchema, &fk.RefTable, &fk.Columns, &fk.RefColumns); err != nil {
			return nil, err
		}
		fk.Composite = len(fk.Columns) > 1
		if !fk.Composite {
			fk.Column = fk.Columns[0]
			fk.RefColumn = fk.RefColumns[0]
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}
