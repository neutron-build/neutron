package studio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// sanitizeError strips potentially sensitive information (e.g. credentials in URLs)
// from error messages before sending them to the HTTP client.
func sanitizeError(err error) string {
	msg := err.Error()
	// Strip credentials from any postgres:// or other scheme URLs in the error
	// by redacting userinfo portions.
	for _, scheme := range []string{"postgres://", "postgresql://", "http://", "https://"} {
		idx := strings.Index(msg, scheme)
		if idx < 0 {
			continue
		}
		// Parse the URL portion out of the error string
		urlStart := idx
		urlEnd := len(msg)
		for i := urlStart; i < len(msg); i++ {
			if msg[i] == ' ' || msg[i] == '"' || msg[i] == '\'' {
				urlEnd = i
				break
			}
		}
		rawURL := msg[urlStart:urlEnd]
		if u, parseErr := url.Parse(rawURL); parseErr == nil && u.User != nil {
			redacted := u.Redacted()
			msg = msg[:urlStart] + redacted + msg[urlEnd:]
		}
	}
	return msg
}

// --- /api/connections ---

func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listConnections(w, r)
	case http.MethodPost:
		s.addConnection(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

type connResponse struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	URL       string `json:"url"` // masked
	IsNucleus bool   `json:"isNucleus"`
}

func toResponse(c SavedConnection) connResponse {
	return connResponse{
		ID:        c.ID,
		Name:      c.Name,
		URL:       MaskedURL(c.URL),
		IsNucleus: c.IsNucleus,
	}
}

func (s *Server) listConnections(w http.ResponseWriter, r *http.Request) {
	list := s.store.List()
	out := make([]connResponse, len(list))
	for i, c := range list {
		out[i] = toResponse(c)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) addConnection(w http.ResponseWriter, r *http.Request) {
	if !s.requireMutationAuth(w, r) {
		return
	}
	var body struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if body.Name == "" || body.URL == "" {
		writeError(w, http.StatusBadRequest, "name and url are required")
		return
	}
	conn, err := s.store.Add(body.Name, body.URL)
	if err != nil {
		log.Printf("studio: add connection error: %v", err)
		writeError(w, http.StatusInternalServerError, "Internal server error")
		return
	}
	writeJSON(w, http.StatusOK, toResponse(conn))
}

// --- /api/connections/test ---

func (s *Server) handleTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	client, err := db.Connect(ctx, body.URL)
	if err != nil {
		log.Printf("studio: test connection error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "isNucleus": false, "version": "", "error": sanitizeError(err)})
		return
	}
	defer client.Close()

	isNucleus, version, err := client.IsNucleus(ctx)
	if err != nil {
		log.Printf("studio: test connection nucleus check error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "isNucleus": false, "version": "", "error": sanitizeError(err)})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "isNucleus": isNucleus, "version": version})
}

// --- /api/connections/:id and /api/connections/:id/connect ---

func (s *Server) handleConnection(w http.ResponseWriter, r *http.Request) {
	// Path: /api/connections/{id} or /api/connections/{id}/connect
	path := strings.TrimPrefix(r.URL.Path, "/api/connections/")
	parts := strings.SplitN(path, "/", 2)
	id := parts[0]
	sub := ""
	if len(parts) == 2 {
		sub = parts[1]
	}

	switch {
	case sub == "connect" && r.Method == http.MethodPost:
		s.connectDB(w, r, id)
	case sub == "" && r.Method == http.MethodDelete:
		s.removeConnection(w, r, id)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) removeConnection(w http.ResponseWriter, r *http.Request, id string) {
	if !s.requireMutationAuth(w, r) {
		return
	}
	if err := s.store.Remove(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	// Close any active client
	s.mu.Lock()
	if c, ok := s.clients[id]; ok {
		c.Close()
		delete(s.clients, id)
	}
	delete(s.epochs, id)
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) connectDB(w http.ResponseWriter, r *http.Request, id string) {
	if !s.requireMutationAuth(w, r) {
		return
	}
	saved, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("connection %q not found", id))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	client, err := db.Connect(ctx, saved.URL)
	if err != nil {
		log.Printf("studio: connect error for %s: %v", id, err)
		writeError(w, http.StatusBadGateway, fmt.Sprintf("connect: %s", sanitizeError(err)))
		return
	}
	s.setClient(id, client)

	isNucleus, version, err := client.IsNucleus(ctx)
	if err != nil {
		log.Printf("studio: nucleus check error for %s: %v", id, err)
		writeError(w, http.StatusBadGateway, sanitizeError(err))
		return
	}
	s.store.SetNucleus(id, isNucleus)

	models := nucleusModels(isNucleus)
	var featureMap map[string]bool
	if isNucleus {
		if fm, fErr := client.NucleusFeatures(ctx); fErr == nil && len(fm) > 0 {
			featureMap = fm
			models = []string{"sql"}
			for _, m := range allNucleusModels {
				if fm[m] {
					models = append(models, m)
				}
			}
		}
	}

	features := map[string]any{
		"isNucleus":  isNucleus,
		"version":    version,
		"models":     models,
		"featureMap": featureMap,
	}

	sc, err := FetchSchema(ctx, client, isNucleus)
	if err != nil {
		log.Printf("studio: fetch schema error for %s: %v", id, err)
		writeError(w, http.StatusInternalServerError, "Failed to fetch schema")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"features": features,
		"schema":   sc,
	})
}

// allNucleusModels is the canonical list of the 14 data models (excluding "sql" which is always present).
var allNucleusModels = []string{"kv", "vector", "timeseries", "document", "graph", "fts", "geo", "blob", "pubsub", "streams", "columnar", "datalog", "cdc"}

func nucleusModels(isNucleus bool) []string {
	if !isNucleus {
		return []string{"sql"}
	}
	models := []string{"sql"}
	return append(models, allNucleusModels...)
}

// --- /api/query ---
// The SQL editor endpoints (/api/query, /api/query/cancel,
// /api/query/explain) live in sqlexec.go.

// taggedResult is a query result with cells converted to their wire forms.
type taggedResult struct {
	columns []string
	data    [][]any
	// truncated: more rows existed than the collector's cap retained.
	truncated bool
}

// collectTaggedRows drains a pgx result, converting every cell of a tagged
// type (int8/numeric/bytea/temporal) to its {t, v} wire form so precision
// survives JSON. Execution errors surface after iteration (pgx behavior).
func collectTaggedRows(rows pgx.Rows) (*taggedResult, error) {
	return collectTaggedRowsCapped(rows, 0)
}

// maxEditorResultRows bounds one SQL editor result held in memory (S06):
// the rows beyond it are not decoded or retained, and the response says
// the result was truncated. Whole-table reads are streamed exports.
const maxEditorResultRows = 10000

// collectTaggedRowsCapped is collectTaggedRows keeping at most max rows
// (max <= 0: unbounded). On reaching the cap it stops iterating and marks
// the result truncated; the caller's rows.Close() drains the remainder
// without decoding or retaining it.
func collectTaggedRowsCapped(rows pgx.Rows, max int) (*taggedResult, error) {
	fds := rows.FieldDescriptions()
	cols := make([]string, len(fds))
	for i, fd := range fds {
		cols[i] = string(fd.Name)
	}
	var data [][]any
	for rows.Next() {
		if max > 0 && len(data) >= max {
			return &taggedResult{columns: cols, data: data, truncated: true}, nil
		}
		// A row that cannot be decoded fails the read loudly; silently
		// skipping it would show a table with rows missing.
		vals, err := rows.Values()
		if err != nil {
			return &taggedResult{columns: cols}, err
		}
		row := make([]any, len(vals))
		for i, v := range vals {
			row[i] = encodeTaggedCell(fds[i].DataTypeOID, v)
		}
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		return &taggedResult{columns: cols}, err
	}
	return &taggedResult{columns: cols, data: data}, nil
}

// --- /api/schema ---

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	connID := r.URL.Query().Get("connectionId")
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	saved, _ := s.store.Get(connID)
	sc, err := FetchSchema(r.Context(), client, saved.IsNucleus)
	if err != nil {
		log.Printf("studio: schema fetch error: %v", err)
		writeError(w, http.StatusInternalServerError, "Failed to fetch schema")
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

// --- /api/features ---

func (s *Server) handleFeatures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	connID := r.URL.Query().Get("connectionId")
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	isNucleus, version, _ := client.IsNucleus(r.Context())

	// Nucleus has no feature-flag function; NucleusFeatures reports the fixed
	// set of models every Nucleus build ships.
	models := nucleusModels(isNucleus)
	var featureMap map[string]bool
	if isNucleus {
		if fm, err := client.NucleusFeatures(r.Context()); err == nil && len(fm) > 0 {
			featureMap = fm
			// Rebuild models list from the live feature query
			models = []string{"sql"} // SQL always present
			for _, m := range allNucleusModels {
				if fm[m] {
					models = append(models, m)
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"isNucleus":  isNucleus,
		"version":    version,
		"models":     models,
		"featureMap": featureMap,
	})
}

// --- /api/table ---

// allowedFilterOps maps the frontend's filter op names to SQL operators.
var allowedFilterOps = map[string]string{
	"eq":       "=",
	"ne":       "<>",
	"lt":       "<",
	"lte":      "<=",
	"gt":       ">",
	"gte":      ">=",
	"like":     "LIKE",
	"ilike":    "ILIKE",
	"is-null":  "IS NULL",
	"not-null": "IS NOT NULL",
}

// tableFilter is one component of a multi-filter read
// (GET /api/table?filters=[{"column":...,"op":...,"value":...}]).
// Filters AND together with any legacy single filter and the FK match
// tuple.
type tableFilter struct {
	Column string `json:"column"`
	Op     string `json:"op"`
	Value  string `json:"value"`
}

// tableSort is one component of a multi-sort read
// (GET /api/table?sorts=[{"column":...,"dir":...}]); sorts apply in array
// order — earlier keys take precedence.
type tableSort struct {
	Column string `json:"column"`
	Dir    string `json:"dir"`
}

// Read-shape bounds: a hand-built query string cannot grow unbounded work.
const (
	maxTableFilters = 8
	maxTableSorts   = 4
	// maxTableReadLimit bounds one table page (S06). The SPA's virtualized
	// grid pages at most this many rows; larger reads are streamed exports.
	maxTableReadLimit = 1000
)

// parseTableFilters decodes the filters parameter strictly: unknown fields,
// unknown ops, repeated or empty columns, or a column missing from the live
// catalog (when the catalog is known) are 400s, never a silently different
// result set. Values are bound parameters (text; PostgreSQL casts to the
// column type).
func parseTableFilters(raw string, meta *tableMeta, catalogKnown bool, argOffset int) ([]string, []any, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var filters []tableFilter
	if err := dec.Decode(&filters); err != nil {
		return nil, nil, fmt.Errorf("filters must be a JSON array of {column, op, value}: %v", err)
	}
	if len(filters) == 0 || len(filters) > maxTableFilters {
		return nil, nil, fmt.Errorf("filters must name between 1 and %d conditions", maxTableFilters)
	}
	seen := map[string]bool{}
	var conds []string
	var args []any
	for _, f := range filters {
		if f.Column == "" || seen[f.Column] {
			return nil, nil, fmt.Errorf("filter column %q is empty or repeated", f.Column)
		}
		seen[f.Column] = true
		if catalogKnown {
			if _, ok := meta.Columns[f.Column]; !ok {
				return nil, nil, fmt.Errorf("filter column %q does not exist", f.Column)
			}
		}
		op, ok := allowedFilterOps[f.Op]
		if !ok {
			return nil, nil, fmt.Errorf("filterOp must be one of eq, ne, lt, lte, gt, gte, like, ilike, is-null, not-null")
		}
		if op == "IS NULL" || op == "IS NOT NULL" {
			conds = append(conds, fmt.Sprintf("%s %s", quoteIdent(f.Column), op))
			continue
		}
		args = append(args, f.Value)
		conds = append(conds, fmt.Sprintf("%s %s $%d", quoteIdent(f.Column), op, argOffset+len(args)))
	}
	return conds, args, nil
}

// parseTableSorts decodes the sorts parameter with the same strictness as
// parseTableFilters; dir is asc/desc case-insensitively. Columns are
// qualified by the table name, matching the legacy single-sort shape.
func parseTableSorts(raw, tableName string, meta *tableMeta, catalogKnown bool) ([]string, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	var sorts []tableSort
	if err := dec.Decode(&sorts); err != nil {
		return nil, fmt.Errorf("sorts must be a JSON array of {column, dir}: %v", err)
	}
	if len(sorts) == 0 || len(sorts) > maxTableSorts {
		return nil, fmt.Errorf("sorts must name between 1 and %d keys", maxTableSorts)
	}
	parts := make([]string, 0, len(sorts))
	for _, s := range sorts {
		if s.Column == "" {
			return nil, fmt.Errorf("sort column is empty")
		}
		if catalogKnown {
			if _, ok := meta.Columns[s.Column]; !ok {
				return nil, fmt.Errorf("sort column %q does not exist", s.Column)
			}
		}
		dir := "ASC"
		if strings.EqualFold(s.Dir, "desc") {
			dir = "DESC"
		} else if !strings.EqualFold(s.Dir, "asc") {
			return nil, fmt.Errorf("sort dir must be asc or desc, got %q", s.Dir)
		}
		parts = append(parts, fmt.Sprintf("%s.%s %s", quoteIdent(tableName), quoteIdent(s.Column), dir))
	}
	return parts, nil
}

func (s *Server) handleTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	connID := q.Get("connectionId")
	schemaName := q.Get("schema")
	tableName := q.Get("table")
	limit := parseInt(q.Get("limit"), 200)
	offset := parseInt(q.Get("offset"), 0)
	filterColumn := q.Get("filterColumn")
	filterOp := q.Get("filterOp")
	filterValue := q.Get("filterValue")
	sortColumn := q.Get("sortColumn")
	sortDir := q.Get("sortDir")

	if connID == "" || schemaName == "" || tableName == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema, and table are required")
		return
	}
	if limit > maxTableReadLimit {
		// Bounded pagination (S06): a page is at most maxTableReadLimit rows.
		// Refused rather than clamped, so a caller never mistakes a shorter
		// page for the end of the data. Whole-table reads are exports.
		writeError(w, http.StatusBadRequest, fmt.Sprintf(
			"limit must be at most %d rows per page; page with offset or use the streamed export for whole tables", maxTableReadLimit))
		return
	}
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}

	if limit <= 0 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	// Authoritative column list (attnum order) for the select and for the
	// reported identity metadata. When introspection is unavailable (an
	// engine without these catalogs) or the relation is not an ordinary
	// table, the read still works — as an explicitly read-only result.
	meta, metaErr := fetchTableMeta(r.Context(), client, schemaName, tableName)
	identityOK := metaErr == nil && meta.Exists
	if metaErr != nil {
		log.Printf("studio: table introspection error: %v", metaErr)
	}

	var conds []string
	var args []any
	if filterColumn != "" {
		op, ok := allowedFilterOps[filterOp]
		if !ok {
			writeError(w, http.StatusBadRequest, "filterOp must be one of eq, ne, lt, lte, gt, gte, like, ilike, is-null, not-null")
			return
		}
		if op == "IS NULL" || op == "IS NOT NULL" {
			conds = append(conds, fmt.Sprintf("%s.%s %s", quoteIdent(tableName), quoteIdent(filterColumn), op))
		} else {
			args = append(args, filterValue)
			conds = append(conds, fmt.Sprintf("%s.%s %s $%d", quoteIdent(tableName), quoteIdent(filterColumn), op, len(args)))
		}
	}
	// filters: multiple ANDed conditions (the multi-filter UI). Validated as
	// strictly as the match tuple: a column the live catalog does not know
	// is a 400, not a query error envelope.
	if raw := q.Get("filters"); raw != "" {
		fConds, fArgs, err := parseTableFilters(raw, meta, identityOK, len(args))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		conds = append(conds, fConds...)
		args = append(args, fArgs...)
	}
	// match: a full-tuple equality filter (composite FK follow). Every
	// component is a bound parameter; the tuple is ANDed as a whole.
	if raw := q.Get("match"); raw != "" {
		matchConds, matchArgs, err := parseMatch(raw, meta, identityOK, len(args))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		conds = append(conds, matchConds...)
		args = append(args, matchArgs...)
	}
	where := ""
	if len(conds) > 0 {
		where = " WHERE " + strings.Join(conds, " AND ")
	}

	var orderParts []string
	if sortColumn != "" {
		if identityOK {
			if _, ok := meta.Columns[sortColumn]; !ok {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("sortColumn %q does not exist", sortColumn))
				return
			}
		}
		dir := "ASC"
		if strings.EqualFold(sortDir, "desc") {
			dir = "DESC"
		}
		orderParts = append(orderParts, fmt.Sprintf("%s.%s %s", quoteIdent(tableName), quoteIdent(sortColumn), dir))
	}
	// sorts: multiple ordered keys (the multi-sort UI), applied after the
	// legacy single key.
	if raw := q.Get("sorts"); raw != "" {
		sParts, err := parseTableSorts(raw, tableName, meta, identityOK)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		orderParts = append(orderParts, sParts...)
	}
	// Deterministic paging (S06): the primary key is the unique tail of
	// every table read, so OFFSET pages neither repeat nor skip rows of an
	// unchanged table (ties under the user's sort keys resolve by key).
	if identityOK {
		sortedCols := map[string]bool{}
		if sortColumn != "" {
			sortedCols[sortColumn] = true
		}
		if raw := q.Get("sorts"); raw != "" {
			var keys []tableSort
			_ = json.Unmarshal([]byte(raw), &keys) // validated above
			for _, k := range keys {
				sortedCols[k.Column] = true
			}
		}
		orderParts = append(orderParts, keyTiebreakers(meta, tableName, sortedCols)...)
	}
	order := ""
	if len(orderParts) > 0 {
		order = " ORDER BY " + strings.Join(orderParts, ", ")
	}

	// Quoted identifiers throughout; filter values are bound parameters.
	tableRef := fmt.Sprintf("%s.%s", quoteIdent(schemaName), quoteIdent(tableName))
	selectCols := "*"
	versionSelect := ""
	if identityOK {
		selectList := make([]string, 0, len(meta.Order))
		for _, col := range meta.Order {
			selectList = append(selectList, quoteIdent(col.Name))
		}
		selectCols = strings.Join(selectList, ", ")
		versionSelect = ", xmin::text"
	}
	buildSQL := func() string {
		return fmt.Sprintf(`SELECT %s%s FROM %s%s%s LIMIT %d OFFSET %d`,
			selectCols, versionSelect, tableRef, where, order, limit, offset)
	}

	start := time.Now()
	rows, err := client.Query(r.Context(), buildSQL(), args...)
	if err != nil && versionSelect != "" && isUndefinedColumnErr(err) {
		// An engine that does not expose xmin (Nucleus) must still be
		// browsable: retry once without the version column and report the
		// table as unversioned — read-only, never unguarded editing.
		versionSelect = ""
		rows, err = client.Query(r.Context(), buildSQL(), args...)
	}
	if err != nil {
		log.Printf("studio: table query error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{
			"columns":  []string{},
			"rows":     [][]any{},
			"rowCount": 0,
			"duration": time.Since(start).Milliseconds(),
			"error":    sanitizeError(err),
		})
		return
	}
	defer rows.Close()

	result, err := collectTaggedRows(rows)
	if err != nil {
		log.Printf("studio: table query error: %v", err)
		writeJSON(w, http.StatusOK, map[string]any{
			"columns":  []string{},
			"rows":     [][]any{},
			"rowCount": 0,
			"duration": time.Since(start).Milliseconds(),
			"error":    sanitizeError(err),
		})
		return
	}

	// Separated row counts: filterCount applies the SAME conditions as the
	// read (filters + match), totalCount applies none. rowCount above stays
	// the FETCHED count. A count failure degrades to omitted fields (the
	// read itself stays useful), never a failed read.
	var filterCount, totalCount int64
	countsOK := false
	if err := client.QueryRow(r.Context(), fmt.Sprintf(
		"SELECT (SELECT count(*) FROM %s%s), (SELECT count(*) FROM %s)", tableRef, where, tableRef),
		args...).Scan(&filterCount, &totalCount); err == nil {
		countsOK = true
	}

	response := map[string]any{
		"columns":    result.columns,
		"rows":       result.data,
		"rowCount":   len(result.data),
		"duration":   time.Since(start).Milliseconds(),
		"keyColumns": []string{},
		"versioned":  false,
		"readOnly":   true,
	}
	if countsOK {
		response["filterCount"] = filterCount
		response["totalCount"] = totalCount
	}
	if !identityOK {
		response["readOnlyReason"] = fmt.Sprintf(
			"%s.%s: row identity metadata is unavailable on this connection — rows are read-only", schemaName, tableName)
		writeJSON(w, http.StatusOK, response)
		return
	}

	// Split the trailing version column off the row arrays; `columns` and
	// every row keep exactly the table's real width.
	versioned := versionSelect != ""
	if versioned {
		result.columns = result.columns[:len(result.columns)-1]
		versions := make([]string, len(result.data))
		reduced := make([][]any, len(result.data))
		for i, row := range result.data {
			if len(row) > 0 {
				if v, ok := row[len(row)-1].(string); ok {
					versions[i] = v
				}
				reduced[i] = row[:len(row)-1]
			}
		}
		response["columns"] = result.columns
		response["rows"] = reduced
		response["versions"] = versions
	}
	state := tableReadOnlyState(meta, versioned)
	response["keyColumns"] = meta.PKCols
	response["versioned"] = versioned
	response["binding"] = bindingFor(s.connectionEpoch(connID), meta.RelOID)
	response["readOnly"] = state.readOnly
	if state.readOnly {
		response["readOnlyReason"] = fmt.Sprintf("%s.%s %s", schemaName, tableName, state.reason)
	}
	writeJSON(w, http.StatusOK, response)
}

// matchCell is one component of a full-tuple equality filter.
type matchCell struct {
	Column string `json:"column"`
	Value  any    `json:"value"`
}

// parseMatch decodes the match parameter: a JSON array of {column, value}
// equality components, ANDed as one tuple. Columns must exist (when the
// catalog is known) and appear once; values are bound parameters decoded
// exactly (tagged cells to exact driver values, JSON numbers as their
// literal text so PostgreSQL parses the digits into the column type). NULL
// never matches under equality and is refused.
func parseMatch(raw string, meta *tableMeta, catalogKnown bool, argOffset int) ([]string, []any, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	var cells []matchCell
	if err := dec.Decode(&cells); err != nil {
		return nil, nil, fmt.Errorf("match must be a JSON array of {column, value}: %v", err)
	}
	if len(cells) == 0 || len(cells) > 32 {
		return nil, nil, fmt.Errorf("match must name between 1 and 32 columns")
	}
	seen := map[string]bool{}
	var conds []string
	var args []any
	for _, c := range cells {
		if c.Column == "" || seen[c.Column] {
			return nil, nil, fmt.Errorf("match column %q is empty or repeated", c.Column)
		}
		seen[c.Column] = true
		if catalogKnown {
			if _, ok := meta.Columns[c.Column]; !ok {
				return nil, nil, fmt.Errorf("match column %q does not exist", c.Column)
			}
		}
		var v any
		switch val := c.Value.(type) {
		case nil:
			return nil, nil, fmt.Errorf("match column %q: NULL never matches by equality", c.Column)
		case json.Number:
			v = val.String()
		case string, bool:
			v = val
		case map[string]any:
			cell, ok := taggedCellOf(val)
			if !ok {
				return nil, nil, fmt.Errorf("match column %q: value must be a scalar or tagged wire cell", c.Column)
			}
			d, err := decodeTagged(cell)
			if err != nil {
				return nil, nil, fmt.Errorf("match column %q: %v", c.Column, err)
			}
			v = d
		default:
			return nil, nil, fmt.Errorf("match column %q: value must be a scalar or tagged wire cell", c.Column)
		}
		args = append(args, v)
		conds = append(conds, fmt.Sprintf("%s = $%d", quoteIdent(c.Column), argOffset+len(args)))
	}
	return conds, args, nil
}

// isUndefinedColumnErr reports whether a query error means the referenced
// column does not exist on this engine (PostgreSQL SQLSTATE 42703, or a
// text mentioning the column for engines without SQLSTATE).
func isUndefinedColumnErr(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42703"
	}
	return strings.Contains(err.Error(), "xmin")
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

// quoteIdent safely quotes a PostgreSQL identifier.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
