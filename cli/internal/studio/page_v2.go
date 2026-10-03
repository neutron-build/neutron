package studio

// This deliberately narrow PostgreSQL-direct page profile reads one ordinary,
// permanent, non-inherited table with one pg_catalog.int8 primary key, ASC.
// Each request has a REPEATABLE READ READ ONLY snapshot; continuation is LIVE
// KEYSET, not a snapshot shared across requests. Inserts behind the boundary
// and edits of keys can be missed; later pages observe later committed values.
// The 8 MiB budget bounds transferred/retained row bytes and the JSON response,
// not PostgreSQL's memory while evaluating a datum's text length. There is no
// COUNT, OFFSET, Nucleus fallback, arbitrary SQL, or held cross-page transaction.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const pageBodyBytes = 32 << 10
const pageTokenBytes = 8 << 10
const pageRowBytes = 8 << 20
const pageTTL = 15 * time.Minute

var pageSecret struct {
	once sync.Once
	key  [32]byte
	err  error
}

func pageSigningKey() ([]byte, error) {
	pageSecret.once.Do(func() { _, pageSecret.err = rand.Read(pageSecret.key[:]) })
	return pageSecret.key[:], pageSecret.err
}

type pageRequest struct {
	ConnectionID string `json:"connectionId"`
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	Profile      string `json:"profile"`
	Limit        int    `json:"limit"`
	Cursor       string `json:"cursor"`
}

type pageCursor struct {
	Version      int    `json:"version"`
	Expires      int64  `json:"expires"`
	Launch       string `json:"launch"`
	ConnectionID string `json:"connectionId"`
	Epoch        string `json:"epoch"`
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	Definition   string `json:"definition"`
	Role         string `json:"role"`
	Limit        int    `json:"limit"`
	After        string `json:"after"`
}

// Exact field spelling and duplicate refusal avoid encoding/json's permissive
// aliases and last-value-wins behavior at this new protocol boundary.
func parsePageRequest(body []byte) (pageRequest, error) {
	var p pageRequest
	d := json.NewDecoder(bytes.NewReader(body))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return p, errors.New("page request must be an object")
	}
	allowed := map[string]bool{"connectionId": true, "schema": true, "table": true, "profile": true, "limit": true, "cursor": true}
	seen := map[string]bool{}
	for d.More() {
		t, err = d.Token()
		if err != nil {
			return p, err
		}
		k, ok := t.(string)
		if !ok || !allowed[k] || seen[k] {
			return p, errors.New("unknown or repeated page field; filters, sorts and match are unsupported")
		}
		seen[k] = true
		var v json.RawMessage
		if err = d.Decode(&v); err != nil {
			return p, err
		}
	}
	if _, err = d.Token(); err != nil {
		return p, err
	}
	if _, err = d.Token(); err != io.EOF {
		return p, errors.New("trailing page data")
	}
	if err = json.Unmarshal(body, &p); err != nil {
		return p, err
	}
	if p.Profile != "postgres-direct" || p.ConnectionID == "" || p.Schema == "" || p.Table == "" || p.Limit < 1 || p.Limit > 1000 || len(p.Cursor) > pageTokenBytes {
		return p, errors.New("require postgres-direct, connectionId/schema/table, limit 1..1000 and cursor <=8 KiB")
	}
	for _, name := range []string{p.ConnectionID, p.Schema, p.Table} {
		if len(name) > 1024 || strings.ContainsRune(name, 0) {
			return p, errors.New("invalid page identifier")
		}
	}
	return p, nil
}

func signPageCursor(c pageCursor, key []byte) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	m := hmac.New(sha256.New, key)
	m.Write(b)
	token := base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
	if len(token) > pageTokenBytes {
		return "", errors.New("cursor exceeds limit")
	}
	return token, nil
}
func verifyPageCursor(token string, key []byte, now time.Time) (pageCursor, error) {
	var c pageCursor
	if len(token) > pageTokenBytes {
		return c, errors.New("invalid cursor")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return c, errors.New("invalid cursor")
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return c, errors.New("invalid cursor")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return c, errors.New("invalid cursor")
	}
	m := hmac.New(sha256.New, key)
	m.Write(b)
	if !hmac.Equal(sig, m.Sum(nil)) {
		return c, errors.New("invalid cursor")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return c, errors.New("invalid cursor")
	}
	if c.Version != 1 || c.Expires <= now.Unix() || c.Expires > now.Add(pageTTL).Unix() {
		return c, errors.New("expired or invalid cursor")
	}
	n, err := strconv.ParseInt(c.After, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != c.After {
		return c, errors.New("invalid cursor key")
	}
	return c, nil
}
func pageCursorMatches(c pageCursor, p pageRequest, epoch, launch, definition, role string) bool {
	return c.ConnectionID == p.ConnectionID && c.Epoch == epoch && c.Launch == launch && c.Schema == p.Schema && c.Table == p.Table && c.Definition == definition && c.Role == role && c.Limit == p.Limit
}

// Relation identity includes dropped attribute slots, namespace/type OIDs,
// typmods/collations/defaults, indexes/constraints and RLS policy definitions.
// Access Share pins the selected physical relation against ALTER/DROP for the
// request; the next request must independently match the exact definition.
const pageDefinitionSQL = `SELECT current_user::text, pg_catalog.json_build_array(
 (SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()),
 c.oid,c.relnamespace,c.relkind,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity,
 (SELECT pg_catalog.json_build_array(oid,rolsuper,rolbypassrls) FROM pg_catalog.pg_roles WHERE rolname=current_user),
 (SELECT pg_catalog.json_agg(pg_catalog.json_build_array(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attcollation,a.attnotnull,a.attisdropped,a.attidentity,a.attgenerated,t.typnamespace,t.typtype,t.typbasetype,pg_catalog.pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum)
 FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_type t ON t.oid=a.atttypid LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=c.oid AND a.attnum>0),
 (SELECT pg_catalog.json_agg(pg_catalog.json_build_array(i.indexrelid,i.indisprimary,i.indisvalid,i.indisunique,pg_catalog.pg_get_indexdef(i.indexrelid)) ORDER BY i.indexrelid) FROM pg_catalog.pg_index i WHERE i.indrelid=c.oid),
 (SELECT pg_catalog.json_agg(pg_catalog.json_build_array(k.oid,pg_catalog.pg_get_constraintdef(k.oid),k.convalidated) ORDER BY k.oid) FROM pg_catalog.pg_constraint k WHERE k.conrelid=c.oid),
 (SELECT pg_catalog.json_agg(pg_catalog.json_build_array(p.oid,p.polname,p.polcmd,p.polpermissive,p.polroles,pg_catalog.pg_get_expr(p.polqual,p.polrelid),pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid)) ORDER BY p.oid) FROM pg_catalog.pg_policy p WHERE p.polrelid=c.oid),
 pg_catalog.current_setting('row_security'),session_user::text
 )::text
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind='r' AND c.relpersistence='p'
 AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits h WHERE h.inhrelid=c.oid OR h.inhparent=c.oid)`

func pageMeta(ctx context.Context, tx pgx.Tx, p pageRequest) (*tableMeta, error) {
	rows, err := tx.Query(ctx, tableMetaSQL, p.Schema, p.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	meta := &tableMeta{Columns: map[string]tableColumnMeta{}}
	for rows.Next() {
		var col tableColumnMeta
		var pos int32
		if err = rows.Scan(&col.Name, &pos, &col.Identity, &col.Generated, &col.DefaultExpr, &col.TypeOID, &col.TypeName, &col.TypType, &col.NotNull, &meta.RelOID, &col.CanUpdate, &col.CanInsert, &meta.CanDelete, &col.Attnum, &meta.ForeignDescendant, &meta.RuleEvents); err != nil {
			return nil, err
		}
		col.KeyPos = int(pos)
		col.IsPK = pos > 0
		meta.Order = append(meta.Order, col)
		meta.Columns[col.Name] = col
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(meta.Order, func(i, j int) bool { return meta.Order[i].Attnum < meta.Order[j].Attnum })
	for _, c := range meta.Order {
		if c.IsPK {
			meta.PKCols = append(meta.PKCols, c.Name)
		}
	}
	meta.Exists = len(meta.Order) > 0
	if len(meta.PKCols) != 1 {
		return nil, errors.New("requires a single bigint primary key")
	}
	pk := meta.Columns[meta.PKCols[0]]
	// Native built-in OID, not a typename lookalike or a bigint domain.
	if pk.TypeOID != 20 || pk.TypType != "b" || !pk.NotNull {
		return nil, errors.New("requires a native bigint primary key")
	}
	return meta, nil
}

func (s *Server) handleTablePageV2(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, 405, "method not allowed")
		return
	}
	if !s.requireMutationAuth(w, r) {
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	// Actual HTTP sockets enforce the same budget while a client sends its
	// body; test recorders do not implement deadlines.
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(5 * time.Second))
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	defer func() {
		// Deadlines belong to this request, not a later keep-alive request.
		_ = controller.SetReadDeadline(time.Time{})
		_ = controller.SetWriteDeadline(time.Time{})
	}()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, pageBodyBytes))
	if err != nil {
		writeError(w, 400, "page body exceeds 32 KiB or is unreadable")
		return
	}
	p, err := parsePageRequest(body)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	key, err := pageSigningKey()
	if err != nil {
		writeError(w, 503, "cursor signing unavailable")
		return
	}
	launchBytes := sha256.Sum256([]byte(s.sessionToken))
	launch := hex.EncodeToString(launchBytes[:])
	s.mu.RLock()
	client, ok := s.clients[p.ConnectionID]
	epoch := s.epochs[p.ConnectionID]
	s.mu.RUnlock()
	if !ok {
		writeError(w, 400, "not connected")
		return
	}
	if epoch == "" {
		epoch = "0"
	}
	var cursor pageCursor
	if p.Cursor != "" {
		cursor, err = verifyPageCursor(p.Cursor, key, time.Now())
		if err != nil {
			writeError(w, 409, "invalid or expired cursor")
			return
		}
		if !pageCursorMatches(cursor, p, epoch, launch, cursor.Definition, cursor.Role) {
			writeError(w, 409, "cursor connection or plan changed")
			return
		}
	}
	conn, err := client.Acquire(ctx)
	if err != nil {
		writeError(w, 502, "page backend unavailable")
		return
	}
	defer conn.Release()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		writeError(w, 502, "page snapshot unavailable")
		return
	}
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = tx.Rollback(cleanup)
	}()
	fail := func() { writeError(w, 502, "page query failed or exceeded its deadline") }
	// Normalize operator/type/function resolution independently of saved
	// connection startup options; every relation identifier remains qualified.
	if _, err = tx.Exec(ctx, "SET LOCAL search_path = pg_catalog"); err != nil {
		fail()
		return
	}
	ref := quoteIdent(p.Schema) + "." + quoteIdent(p.Table)
	if _, err = tx.Exec(ctx, "LOCK TABLE ONLY "+ref+" IN ACCESS SHARE MODE"); err != nil {
		fail()
		return
	}
	// Lock before the first snapshot-taking query: catalog introspection must
	// describe the same physical relation subsequently read.
	// An explicitly selected direct profile still refuses Nucleus; detection
	// and every statement use the same dedicated request transaction.
	var pgVersion string
	if err = tx.QueryRow(ctx, `SELECT pg_catalog.version()`).Scan(&pgVersion); err != nil {
		fail()
		return
	}
	if !strings.HasPrefix(pgVersion, "PostgreSQL ") || strings.Contains(pgVersion, "Nucleus") {
		writeError(w, 400, "page profile requires PostgreSQL-direct")
		return
	}
	var role, definition string
	if err = tx.QueryRow(ctx, pageDefinitionSQL, p.Schema, p.Table).Scan(&role, &definition); err != nil {
		writeError(w, 400, "page profile requires an ordinary permanent non-inherited PostgreSQL table")
		return
	}
	definitionHash := sha256.Sum256([]byte(definition))
	definition = hex.EncodeToString(definitionHash[:])
	if p.Cursor != "" && !pageCursorMatches(cursor, p, epoch, launch, definition, role) {
		writeError(w, 409, "cursor role or table definition changed")
		return
	}
	meta, err := pageMeta(ctx, tx, p)
	if err != nil {
		writeError(w, 400, "page profile requires a single native bigint primary key")
		return
	}
	pk := quoteIdent(meta.PKCols[0])
	where := ""
	args := []any{}
	if p.Cursor != "" {
		after, _ := strconv.ParseInt(cursor.After, 10, 64)
		where = " WHERE " + pk + ">$1"
		args = append(args, after)
	}
	// Candidate IDs are bounded BEFORE any length scan. Text-length preflight
	// uses this same snapshot and prevents a concurrent value growth race.
	idRows, err := tx.Query(ctx, "SELECT "+pk+" FROM "+ref+where+" ORDER BY "+pk+" ASC LIMIT "+strconv.Itoa(p.Limit+1), args...)
	if err != nil {
		fail()
		return
	}
	ids := []int64{}
	for idRows.Next() {
		var id int64
		if err = idRows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	idRows.Close()
	if err != nil || idRows.Err() != nil {
		fail()
		return
	}
	hasNext := len(ids) > p.Limit
	if hasNext {
		ids = ids[:p.Limit]
	}
	columns := []string{}
	sizeParts := []string{}
	for _, col := range meta.Order {
		q := quoteIdent(col.Name)
		columns = append(columns, q)
		sizeParts = append(sizeParts, "COALESCE(pg_catalog.octet_length("+q+"::text)::bigint,0)")
	}
	var estimated int64
	if err = tx.QueryRow(ctx, "SELECT COALESCE(sum("+strings.Join(sizeParts, "+")+"),0)::bigint FROM "+ref+" WHERE "+pk+"=ANY($1::pg_catalog.int8[])", ids).Scan(&estimated); err != nil {
		fail()
		return
	}
	if estimated > pageRowBytes {
		writeError(w, 413, "page exceeds 8 MiB; use a smaller limit or streaming export")
		return
	}
	rows, err := tx.Query(ctx, "SELECT "+strings.Join(columns, ",")+", xmin::text FROM "+ref+" WHERE "+pk+"=ANY($1::pg_catalog.int8[]) ORDER BY "+pk+" ASC", ids)
	if err != nil {
		fail()
		return
	}
	data := [][]any{}
	versions := []string{}
	rawBytes := 0
	fds := rows.FieldDescriptions()
	for rows.Next() {
		for _, raw := range rows.RawValues() {
			rawBytes += len(raw)
		}
		if rawBytes > pageRowBytes {
			rows.Close()
			writeError(w, 413, "page exceeds 8 MiB; use a smaller limit or streaming export")
			return
		}
		vals, e := rows.Values()
		if e != nil {
			err = e
			break
		}
		row := make([]any, len(vals)-1)
		for i := range row {
			row[i] = encodeTaggedCell(fds[i].DataTypeOID, vals[i])
		}
		v, valid := vals[len(vals)-1].(string)
		if !valid {
			err = errors.New("invalid row version")
			break
		}
		data = append(data, row)
		versions = append(versions, v)
	}
	rows.Close()
	if err != nil || rows.Err() != nil {
		fail()
		return
	}
	if len(data) != len(ids) {
		fail()
		return
	}
	next := ""
	if hasNext {
		next, err = signPageCursor(pageCursor{Version: 1, Expires: time.Now().Add(pageTTL).Unix(), Launch: launch, ConnectionID: p.ConnectionID, Epoch: epoch, Schema: p.Schema, Table: p.Table, Definition: definition, Role: role, Limit: p.Limit, After: strconv.FormatInt(ids[len(ids)-1], 10)}, key)
		if err != nil {
			fail()
			return
		}
	}
	names := []string{}
	for _, c := range meta.Order {
		names = append(names, c.Name)
	}
	response := map[string]any{"columns": names, "rows": data, "versions": versions, "keyColumns": meta.PKCols, "binding": bindingFor(epoch, meta.RelOID), "versioned": true, "readOnly": false, "rowCount": len(data), "hasNext": hasNext, "nextCursor": next, "consistency": "live-keyset/request-repeatable-read"}
	b, err := json.Marshal(response)
	if err != nil {
		fail()
		return
	}
	if len(b) > pageRowBytes {
		writeError(w, 413, "encoded page exceeds 8 MiB; use a smaller limit or streaming export")
		return
	}
	if err = tx.Commit(ctx); err != nil {
		fail()
		return
	}
	s.mu.RLock()
	current, currentOK := s.clients[p.ConnectionID]
	currentEpoch := s.epochs[p.ConnectionID]
	if currentEpoch == "" {
		currentEpoch = "0"
	}
	stale := !currentOK || current != client || currentEpoch != epoch
	if stale {
		s.mu.RUnlock()
		writeError(w, 409, "connection changed during page read")
		return
	}
	// The response retains the captured epoch even if reconnect follows this
	// check; guarded mutations and continuation refuse that stale binding.
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(b)
}
