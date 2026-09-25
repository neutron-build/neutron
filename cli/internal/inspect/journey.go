package inspect

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/neutron-build/neutron/cli/internal/db"
)

// The journey connects one table's stages:
//
//	schema -> migrations -> queries -> SQL/plan -> rows -> models -> change events
//
// Each stage reports whether it is available on the connected engine, why
// not when it is not, and which model's limits govern it. Everything here
// is read-only: SQL runs inside a rolled-back READ ONLY transaction, and on
// engines that do not apply READ ONLY only fixed, guard-checked reads run.

// Stage names, in journey order.
const (
	StageSchema       = "schema"
	StageMigrations   = "migrations"
	StageQueries      = "queries"
	StagePlan         = "plan"
	StageRows         = "rows"
	StageModels       = "models"
	StageChangeEvents = "change-events"
)

// Stage status.
const (
	StageAvailable   = "available"
	StageEmpty       = "empty"
	StageUnavailable = "unavailable"
)

// Stage is one hop of the journey.
type Stage struct {
	Stage  string `json:"stage"`
	Model  string `json:"model"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	Data   any    `json:"data,omitempty"`
}

// Journey is the inspection document for one table.
type Journey struct {
	Engine Engine  `json:"engine"`
	Schema string  `json:"schema"`
	Table  string  `json:"table"`
	Limits Report  `json:"limits"`
	Stages []Stage `json:"stages"`
}

// Stage returns the named stage.
func (j *Journey) Stage(name string) *Stage {
	for i := range j.Stages {
		if j.Stages[i].Stage == name {
			return &j.Stages[i]
		}
	}
	return nil
}

// JourneyOptions selects the table and bounds the reads.
type JourneyOptions struct {
	Schema string
	Table  string
	// MigrationsDir is the application's migrations directory; empty when
	// none is configured (the history table is still read).
	MigrationsDir string
	// SampleRows bounds the row sample (default 5, max 50).
	SampleRows int
	// EventLimit bounds change events and bound nodes (default 20, max 200).
	EventLimit int
	// Queries is the caller's own statement record for this connection, if
	// it keeps one (Studio does; MCP does not). Nil marks the stage
	// unavailable with QueriesReason.
	Queries       []QueryRecord
	QueriesReason string
}

// QueryRecord is one executed statement known to the caller.
type QueryRecord struct {
	At         time.Time `json:"at"`
	Surface    string    `json:"surface"`
	SQL        string    `json:"sql"`
	DurationMs float64   `json:"durationMs"`
	State      string    `json:"state"`
	RowCount   int       `json:"rowCount,omitempty"`
}

var plainIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// BuildJourney assembles the journey for one table. Engine-level failures
// become stage reasons; only an unusable connection is an error.
func BuildJourney(ctx context.Context, client *db.Client, opts JourneyOptions) (*Journey, error) {
	if opts.Schema == "" || opts.Table == "" {
		return nil, errors.New("schema and table are required")
	}
	if opts.SampleRows <= 0 {
		opts.SampleRows = 5
	}
	if opts.SampleRows > 50 {
		opts.SampleRows = 50
	}
	if opts.EventLimit <= 0 {
		opts.EventLimit = 20
	}
	if opts.EventLimit > 200 {
		opts.EventLimit = 200
	}
	engine, err := DetectEngine(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("identify engine: %w", err)
	}
	var live *LiveSettings
	if engine.Product == "postgres" {
		live = ReadLiveSettings(ctx, client)
	}
	j := &Journey{Engine: engine, Schema: opts.Schema, Table: opts.Table, Limits: BuildReport(engine, live)}

	schemaStage, keyColumns := journeySchema(ctx, client, engine, opts)
	j.Stages = append(j.Stages, schemaStage)
	j.Stages = append(j.Stages, journeyMigrations(ctx, client, engine, opts))
	j.Stages = append(j.Stages, journeyQueries(opts))
	j.Stages = append(j.Stages, journeyPlan(ctx, client, engine, opts))
	rowsStage, ids := journeyRows(ctx, client, engine, opts, keyColumns)
	j.Stages = append(j.Stages, rowsStage)
	j.Stages = append(j.Stages, journeyModels(ctx, client, engine, opts, ids))
	j.Stages = append(j.Stages, journeyChangeEvents(ctx, client, engine, opts))
	return j, nil
}

func unavailable(stage, model, reason string) Stage {
	return Stage{Stage: stage, Model: model, Status: StageUnavailable, Reason: reason}
}

// TableRef names another relation (FK navigation).
type TableRef struct {
	Constraint string   `json:"constraint"`
	Schema     string   `json:"schema"`
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	RefColumns []string `json:"refColumns"`
}

// SchemaData is the schema stage payload.
type SchemaData struct {
	Source         string       `json:"source"`
	DocumentSHA256 string       `json:"documentSHA256,omitempty"`
	Columns        []ColumnInfo `json:"columns"`
	KeyColumns     []string     `json:"keyColumns"`
	References     []TableRef   `json:"references"`
	ReferencedBy   []TableRef   `json:"referencedBy"`
}

// ColumnInfo is one column of the inspected table.
type ColumnInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	NotNull    bool   `json:"notNull"`
	PrimaryKey bool   `json:"primaryKey"`
}

func journeySchema(ctx context.Context, client *db.Client, engine Engine, opts JourneyOptions) (Stage, []string) {
	if engine.Product != "postgres" {
		return unavailable(StageSchema, "sql",
			"structure comes from the schema contract v2 introspection, verified on PostgreSQL only; Nucleus catalog introspection is not usable (capability report blocker 1, N4)"), nil
	}
	doc, err := client.IntrospectV2(ctx)
	if err != nil {
		return unavailable(StageSchema, "sql", "introspection failed: "+RedactText(err.Error())), nil
	}
	model, err := db.ModelFromRoot(doc.Root)
	if err != nil {
		return unavailable(StageSchema, "sql", "introspection decode failed: "+err.Error()), nil
	}
	id := db.V2Identity{Schema: opts.Schema, Name: opts.Table}
	t := model.Table(id)
	if t == nil {
		return unavailable(StageSchema, "sql", fmt.Sprintf("no table %s in the live catalog (dropped, renamed, a view, or not visible to this role)", id)), nil
	}
	data := SchemaData{Source: "introspection-v2", DocumentSHA256: doc.SHA256Hex, KeyColumns: []string{}, References: []TableRef{}, ReferencedBy: []TableRef{}}
	pk := map[string]bool{}
	if c := t.PrimaryKey(); c != nil {
		data.KeyColumns = append(data.KeyColumns, c.Columns...)
		for _, col := range c.Columns {
			pk[col] = true
		}
	}
	for _, c := range t.Columns {
		typ, err := db.RenderV2TypeDDL(c.Type)
		if err != nil {
			typ = c.Type.Name
		}
		data.Columns = append(data.Columns, ColumnInfo{Name: c.Name, Type: typ, NotNull: c.NotNull || pk[c.Name], PrimaryKey: pk[c.Name]})
	}
	for _, c := range t.Constraints {
		if c.Type == "foreign-key" && c.References != nil {
			data.References = append(data.References, TableRef{Constraint: c.Name, Schema: c.References.Table.Schema, Name: c.References.Table.Name, Columns: c.Columns, RefColumns: c.References.Columns})
		}
	}
	for _, other := range model.Tables {
		if other.Identity == id {
			continue
		}
		for _, c := range other.Constraints {
			if c.Type == "foreign-key" && c.References != nil && c.References.Table == id {
				data.ReferencedBy = append(data.ReferencedBy, TableRef{Constraint: c.Name, Schema: other.Identity.Schema, Name: other.Identity.Name, Columns: c.Columns, RefColumns: c.References.Columns})
			}
		}
	}
	return Stage{Stage: StageSchema, Model: "sql", Status: StageAvailable, Data: data}, data.KeyColumns
}

// MigrationEntry is one migration known to the files or the history.
type MigrationEntry struct {
	Version   string     `json:"version"`
	Name      string     `json:"name"`
	Applied   bool       `json:"applied"`
	AppliedAt *time.Time `json:"appliedAt,omitempty"`
	// Checksum: verified | mismatch | unverified | pending | file-missing
	Checksum string `json:"checksum"`
	// Mentions: the file's SQL names the table (a text match on the
	// identifier, not a parse).
	Mentions bool `json:"mentions"`
	// Lines are the statement lines naming the table (at most 5).
	Lines []string `json:"lines,omitempty"`
}

// MigrationsData is the migrations stage payload.
type MigrationsData struct {
	Directory  string           `json:"directory,omitempty"`
	FilesNote  string           `json:"filesNote,omitempty"`
	History    string           `json:"history"`
	Entries    []MigrationEntry `json:"entries"`
	Mentioning int              `json:"mentioning"`
}

func journeyMigrations(ctx context.Context, client *db.Client, engine Engine, opts JourneyOptions) Stage {
	return MigrationsStage(ctx, client, engine, opts.MigrationsDir, opts.Schema, opts.Table)
}

// MigrationsStage reads the applied history (read-only) and the migration
// files in dir, with each migration's checksum status. When table is set,
// entries whose SQL names it are marked.
func MigrationsStage(ctx context.Context, client *db.Client, engine Engine, dir, schema, table string) Stage {
	if engine.Product != "postgres" {
		return unavailable(StageMigrations, "sql",
			"the CLI migration workflow targets PostgreSQL; on Nucleus DDL is not transactional and there is no advisory lock (capability report N2, N7), so migrations stay experimental there")
	}
	data := MigrationsData{Entries: []MigrationEntry{}}
	shape, err := client.InspectMigrationHistory(ctx)
	if err != nil {
		return unavailable(StageMigrations, "sql", "reading _neutron_migrations failed: "+RedactText(err.Error()))
	}
	data.History = shape.String()
	applied, err := client.AppliedMigrations(ctx)
	if err != nil {
		return unavailable(StageMigrations, "sql", "reading applied migrations failed: "+RedactText(err.Error()))
	}
	var files []db.MigrationFile
	switch {
	case dir == "":
		data.FilesNote = "no migrations directory configured; applied history only"
	default:
		data.Directory = dir
		if st, err := os.Stat(dir); err != nil || !st.IsDir() {
			data.FilesNote = "migrations directory " + dir + " not found; applied history only"
		} else if files, err = db.ReadMigrationFiles(dir); err != nil {
			data.FilesNote = "reading migration files failed: " + err.Error()
			files = nil
		}
	}
	byVersion := map[string]db.MigrationRecord{}
	for _, r := range applied {
		byVersion[r.Version] = r
	}
	seen := map[string]bool{}
	var mention *regexp.Regexp
	if table != "" {
		mention = tableMentionPattern(schema, table)
	}
	for _, f := range files {
		e := MigrationEntry{Version: f.Version, Name: f.Name, Checksum: "pending"}
		if r, ok := byVersion[f.Version]; ok {
			seen[f.Version] = true
			e.Applied = true
			at := r.AppliedAt
			e.AppliedAt = &at
			switch {
			case r.Checksum == nil:
				e.Checksum = "unverified"
			case *r.Checksum == db.MigrationChecksum(f.SQL):
				e.Checksum = "verified"
			default:
				e.Checksum = "mismatch"
			}
		}
		if mention != nil {
			e.Lines = mentionLines(f.SQL, mention)
			e.Mentions = len(e.Lines) > 0
		}
		data.Entries = append(data.Entries, e)
	}
	for _, r := range applied {
		if seen[r.Version] {
			continue
		}
		at := r.AppliedAt
		data.Entries = append(data.Entries, MigrationEntry{Version: r.Version, Name: r.Name, Applied: true, AppliedAt: &at, Checksum: "file-missing"})
	}
	sort.SliceStable(data.Entries, func(a, b int) bool {
		return db.CompareVersions(data.Entries[a].Version, data.Entries[b].Version) < 0
	})
	for _, e := range data.Entries {
		if e.Mentions {
			data.Mentioning++
		}
	}
	status := StageAvailable
	if len(data.Entries) == 0 {
		status = StageEmpty
	}
	return Stage{Stage: StageMigrations, Model: "sql", Status: status, Data: data}
}

// tableMentionPattern matches the table name as a whole identifier, quoted
// or not, optionally schema-qualified.
func tableMentionPattern(schema, table string) *regexp.Regexp {
	name := regexp.QuoteMeta(table)
	return regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_$])"?` + name + `"?([^A-Za-z0-9_$]|$)`)
}

func mentionLines(sql string, re *regexp.Regexp) []string {
	var out []string
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		if re.MatchString(trimmed) {
			if len(trimmed) > 200 {
				trimmed = trimmed[:200] + " …"
			}
			out = append(out, trimmed)
			if len(out) == 5 {
				break
			}
		}
	}
	return out
}

// QueriesData is the queries stage payload.
type QueriesData struct {
	Entries []QueryRecord `json:"entries"`
	Scope   string        `json:"scope"`
}

func journeyQueries(opts JourneyOptions) Stage {
	if opts.Queries == nil {
		reason := opts.QueriesReason
		if reason == "" {
			reason = "this surface keeps no statement record"
		}
		return unavailable(StageQueries, "sql", reason)
	}
	mention := tableMentionPattern(opts.Schema, opts.Table)
	data := QueriesData{Entries: []QueryRecord{}, Scope: opts.QueriesReason}
	for _, q := range opts.Queries {
		if mention.MatchString(q.SQL) {
			data.Entries = append(data.Entries, q)
			if len(data.Entries) == opts.EventLimit {
				break
			}
		}
	}
	status := StageAvailable
	if len(data.Entries) == 0 {
		status = StageEmpty
	}
	return Stage{Stage: StageQueries, Model: "sql", Status: status, Data: data}
}

// QualifiedName quotes schema.table for SQL text.
func QualifiedName(schema, table string) string {
	return quoteIdent(schema) + "." + quoteIdent(table)
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// PlanData is the SQL/plan stage payload.
type PlanData struct {
	Statement string `json:"statement"`
	// Plan is PostgreSQL's EXPLAIN (FORMAT JSON) output, unmodified.
	Plan     json.RawMessage `json:"plan"`
	Executed bool            `json:"executed"`
	Note     string          `json:"note"`
}

func sampleStatement(opts JourneyOptions) string {
	return fmt.Sprintf("SELECT * FROM %s LIMIT %d", QualifiedName(opts.Schema, opts.Table), opts.SampleRows)
}

func journeyPlan(ctx context.Context, client *db.Client, engine Engine, opts JourneyOptions) Stage {
	if engine.Product != "postgres" {
		return unavailable(StagePlan, "sql", "EXPLAIN output and its read-only guarantee are not verified on this engine (capability report txn.read_only_rejects_writes)")
	}
	stmt := sampleStatement(opts)
	raw, err := ExplainJSON(ctx, client, stmt)
	if err != nil {
		return unavailable(StagePlan, "sql", "EXPLAIN failed: "+RedactText(err.Error()))
	}
	return Stage{Stage: StagePlan, Model: "sql", Status: StageAvailable, Data: PlanData{
		Statement: stmt, Plan: raw, Executed: false,
		Note: "EXPLAIN without ANALYZE: planned, never executed, inside a rolled-back READ ONLY transaction",
	}}
}

// ExplainJSON returns EXPLAIN (FORMAT JSON) for a read statement, planned
// but not executed, inside a rolled-back READ ONLY transaction.
func ExplainJSON(ctx context.Context, client *db.Client, stmt string) (json.RawMessage, error) {
	if err := CheckReadOnlySQL(stmt, "postgres"); err != nil {
		return nil, err
	}
	var out json.RawMessage
	// PostgreSQL only (callers check): the connection is discarded after.
	err := ReadOnly(ctx, client, Engine{Product: "postgres"}, func(tx pgx.Tx) error {
		var text string
		if err := tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+stmt, pgx.QueryExecModeExec).Scan(&text); err != nil {
			return err
		}
		out = json.RawMessage(text)
		return nil
	})
	return out, err
}

// ReadOnly runs fn inside a READ ONLY transaction and always rolls back.
// On every engine but Nucleus the connection is then discarded (advisory
// locks released, session closed), so nothing a statement left on the
// session outlives the read. Nucleus has no advisory locks or session reset
// to rely on and its guard is lexical, so its connections are pooled.
func ReadOnly(ctx context.Context, client *db.Client, engine Engine, fn func(tx pgx.Tx) error) error {
	return client.ReadOnlyTx(ctx, engine.Product != "nucleus", fn)
}

// RowsData is the rows stage payload.
type RowsData struct {
	Statement  string   `json:"statement"`
	Columns    []string `json:"columns"`
	Rows       [][]any  `json:"rows"`
	KeyColumns []string `json:"keyColumns"`
	// IDColumn is the single integer column used to find row-bound model
	// objects (graph nodes stamp sqlref_row with it), when one exists.
	IDColumn string `json:"idColumn,omitempty"`
	Note     string `json:"note"`
}

func journeyRows(ctx context.Context, client *db.Client, engine Engine, opts JourneyOptions, keyColumns []string) (Stage, []int64) {
	stmt := sampleStatement(opts)
	data := RowsData{Statement: stmt, Columns: []string{}, Rows: [][]any{}, KeyColumns: keyColumns}
	if data.KeyColumns == nil {
		data.KeyColumns = []string{}
	}
	if engine.Product == "postgres" {
		data.Note = "sample read in a rolled-back READ ONLY transaction; browse the table for keyed, editable rows"
	} else {
		data.Note = "sample read; this engine does not apply READ ONLY, so only this fixed SELECT runs"
	}
	var ids []int64
	err := ReadOnly(ctx, client, engine, func(tx pgx.Tx) error {
		// Unnamed statement (no plan cache): the table's shape may have
		// changed since this connection last read it.
		rows, err := tx.Query(ctx, stmt, pgx.QueryExecModeExec)
		if err != nil {
			return err
		}
		defer rows.Close()
		for _, fd := range rows.FieldDescriptions() {
			data.Columns = append(data.Columns, fd.Name)
		}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			row := make([]any, len(vals))
			for i, v := range vals {
				row[i] = CellValue(v)
			}
			data.Rows = append(data.Rows, row)
		}
		return rows.Err()
	})
	if err != nil {
		return unavailable(StageRows, "sql", "row read failed: "+RedactText(err.Error())), nil
	}
	// Row-bound model objects key on one integer id: the single-column
	// primary key when known, else a column literally named "id".
	idCol := ""
	if len(data.KeyColumns) == 1 {
		idCol = data.KeyColumns[0]
	} else if len(data.KeyColumns) == 0 {
		for _, c := range data.Columns {
			if c == "id" {
				idCol = c
			}
		}
	}
	if idCol != "" {
		idx := -1
		for i, c := range data.Columns {
			if c == idCol {
				idx = i
			}
		}
		for _, r := range data.Rows {
			if idx >= 0 {
				if n, ok := integerCell(r[idx]); ok {
					ids = append(ids, n)
				}
			}
		}
		if len(ids) == len(data.Rows) {
			data.IDColumn = idCol
		} else {
			ids = nil
		}
	}
	status := StageAvailable
	if len(data.Rows) == 0 {
		status = StageEmpty
	}
	return Stage{Stage: StageRows, Model: "sql", Status: status, Data: data}, ids
}

func integerCell(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	case float64:
		if n == math.Trunc(n) && math.Abs(n) <= 1<<53 {
			return int64(n), true
		}
	}
	return 0, false
}

// CellValue renders a driver value as JSON-safe data without losing
// precision: integers beyond 2^53, numerics and timestamps become strings;
// bytes become \x-prefixed hex.
func CellValue(v any) any {
	if v == nil {
		return nil
	}
	if val, ok := v.(driver.Valuer); ok {
		if dv, err := val.Value(); err == nil {
			v = dv
		}
	}
	switch x := v.(type) {
	case nil:
		return nil
	case string, bool:
		return x
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		if x > 1<<53 || x < -(1<<53) {
			return strconv.FormatInt(x, 10)
		}
		return x
	case float32:
		return float64(x)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return strconv.FormatFloat(x, 'g', -1, 64)
		}
		return x
	case time.Time:
		return x.Format(time.RFC3339Nano)
	case []byte:
		return `\x` + hex.EncodeToString(x)
	case [16]byte:
		return fmt.Sprintf("%x-%x-%x-%x-%x", x[0:4], x[4:6], x[6:8], x[8:10], x[10:16])
	case map[string]any, []any:
		return x
	default:
		return fmt.Sprint(x)
	}
}

// BoundNode is a graph node stamped with a row of the inspected table.
type BoundNode struct {
	NodeID    string `json:"nodeId"`
	RowID     any    `json:"rowId"`
	RowSchema string `json:"rowSchema,omitempty"`
	// InSample: the stamped row is one of the rows stage's sampled rows.
	InSample bool `json:"inSample"`
}

// ModelsData is the models stage payload.
type ModelsData struct {
	Query      string      `json:"query"`
	GraphNodes []BoundNode `json:"graphNodes"`
	Truncated  bool        `json:"truncated"`
	Documents  string      `json:"documents"`
}

func journeyModels(ctx context.Context, client *db.Client, engine Engine, opts JourneyOptions, ids []int64) Stage {
	if engine.Product != "nucleus" {
		return unavailable(StageModels, "graph", "row-bound graph nodes and documents are Nucleus models; the connected engine is not Nucleus")
	}
	if !plainIdent.MatchString(opts.Table) {
		return unavailable(StageModels, "graph", "GRAPH_QUERY takes Cypher text only (no parameters), so only plain identifiers ([A-Za-z_][A-Za-z0-9_]*) are looked up")
	}
	cypher := fmt.Sprintf("MATCH (n) WHERE n.sqlref_table = '%s' RETURN n, n.sqlref_row, n.sqlref_schema", opts.Table)
	data := ModelsData{
		Query:      cypher,
		GraphNodes: []BoundNode{},
		Documents:  "collections bound to a table exist only in client code (boundTo creates nothing in the engine), so they cannot be discovered from the database",
	}
	var raw *string
	err := ReadOnly(ctx, client, engine, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT GRAPH_QUERY($1)", cypher).Scan(&raw)
	})
	if err != nil {
		return unavailable(StageModels, "graph", "graph lookup failed: "+RedactText(err.Error()))
	}
	sample := map[int64]bool{}
	for _, id := range ids {
		sample[id] = true
	}
	if raw != nil {
		var parsed struct {
			Rows [][]json.RawMessage `json:"rows"`
		}
		if err := json.Unmarshal([]byte(*raw), &parsed); err != nil {
			return unavailable(StageModels, "graph", "unexpected GRAPH_QUERY answer: "+truncate(*raw, 120))
		}
		for _, r := range parsed.Rows {
			if len(r) < 2 {
				continue
			}
			// RETURN n answers the node id (X02: rows are [[id, ...], ...]);
			// an object form carrying "id" is accepted too.
			node := BoundNode{NodeID: nodeIDOf(r[0])}
			var rowID any
			_ = json.Unmarshal(r[1], &rowID)
			node.RowID = rowID
			if len(r) > 2 {
				var s *string
				if json.Unmarshal(r[2], &s) == nil && s != nil {
					node.RowSchema = *s
				}
			}
			// A node stamped with another schema belongs to that schema's
			// table of the same name (X02 binding rule).
			if node.RowSchema != "" && node.RowSchema != opts.Schema {
				continue
			}
			if id, ok := integerCell(rowID); ok {
				node.InSample = sample[id]
			}
			if len(data.GraphNodes) == opts.EventLimit {
				data.Truncated = true
				break
			}
			data.GraphNodes = append(data.GraphNodes, node)
		}
	}
	status := StageAvailable
	if len(data.GraphNodes) == 0 {
		status = StageEmpty
	}
	return Stage{Stage: StageModels, Model: "graph", Status: status, Data: data}
}

// ChangeEvent is one CDC log entry (metadata only).
type ChangeEvent struct {
	Seq    int64  `json:"seq"`
	Table  string `json:"table"`
	Change string `json:"change"`
	TS     int64  `json:"ts"`
}

// ChangeEventsData is the change-events stage payload.
type ChangeEventsData struct {
	Retained int64         `json:"retained"`
	Events   []ChangeEvent `json:"events"`
	Note     string        `json:"note"`
}

// cdcWindow bounds how far back the change-events stage scans.
const cdcWindow = 10000

func journeyChangeEvents(ctx context.Context, client *db.Client, engine Engine, opts JourneyOptions) Stage {
	if engine.Product != "nucleus" {
		return unavailable(StageChangeEvents, "cdc", "Studio has no change-event source on this engine (PostgreSQL logical decoding is not surfaced)")
	}
	data := ChangeEventsData{Events: []ChangeEvent{}, Note: "latest events for this table within the last " + strconv.Itoa(cdcWindow) + " retained events; metadata only"}
	var raw *string
	err := ReadOnly(ctx, client, engine, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT CDC_COUNT()").Scan(&data.Retained); err != nil {
			return err
		}
		after := data.Retained - cdcWindow
		if after < 0 {
			after = 0
		}
		return tx.QueryRow(ctx, "SELECT CDC_TABLE_READ($1, $2, $3)", opts.Table, after, int64(100000)).Scan(&raw)
	})
	if err != nil {
		return unavailable(StageChangeEvents, "cdc", "change-event read failed: "+RedactText(err.Error()))
	}
	if raw != nil && strings.TrimSpace(*raw) != "" {
		var events []ChangeEvent
		if err := json.Unmarshal([]byte(*raw), &events); err != nil {
			return unavailable(StageChangeEvents, "cdc", "unexpected CDC_TABLE_READ answer: "+truncate(*raw, 120))
		}
		if len(events) > opts.EventLimit {
			events = events[len(events)-opts.EventLimit:]
		}
		// Newest first.
		for i := len(events) - 1; i >= 0; i-- {
			data.Events = append(data.Events, events[i])
		}
	}
	status := StageAvailable
	if len(data.Events) == 0 {
		status = StageEmpty
	}
	return Stage{Stage: StageChangeEvents, Model: "cdc", Status: status, Data: data}
}

func nodeIDOf(raw json.RawMessage) string {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return strings.Trim(string(raw), `"`)
	}
	switch x := v.(type) {
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case string:
		return x
	case map[string]any:
		if id, ok := x["id"]; ok {
			return fmt.Sprint(id)
		}
	}
	return strings.Trim(string(raw), `"`)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + " …"
}
