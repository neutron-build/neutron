// Package inspect is the shared read-side of Studio's cross-model
// inspection workflow and the `neutron mcp` inspection tools (X06): engine
// identity, per-model transaction/durability limits, the read-only SQL
// guard, value redaction and the schema -> migration -> query -> plan -> row
// -> model -> change-event journey.
//
// Every limit stated here traces to measured evidence or to the engine's own
// semantics document, and says which. The registry is checked against the
// capability report (conformance/live/orm/capabilities.nucleus.json) and the
// conformance legs by limits_report_test.go: a limit that claims more than
// its evidence establishes fails the build.
package inspect

import (
	"context"
	"strings"

	"github.com/neutron-build/neutron/cli/internal/db"
)

// Status is the tri-state availability vocabulary shared with the SDKs'
// capability gates: anything but "supported" is not advertised.
type Status string

const (
	Supported   Status = "supported"
	Unsupported Status = "unsupported"
	Unknown     Status = "unknown"
)

// Transaction behaviour of one model's writes relative to the SQL
// transaction they are issued in. The vocabulary is deliberately small; the
// note carries the specifics.
const (
	// TxAtomic: all-or-nothing and isolated from other sessions until
	// COMMIT. Only claimed for SQL on PostgreSQL.
	TxAtomic = "atomic"
	// TxPartial: some statement classes are transactional, others are not
	// (the note names which).
	TxPartial = "partial"
	// TxRollbackNotIsolated: ROLLBACK removes the writes, but other sessions
	// can read them before COMMIT.
	TxRollbackNotIsolated = "rollback-not-isolated"
	// TxRefused: the engine refuses these writes inside an explicit
	// transaction.
	TxRefused = "refused-in-transaction"
	// TxNone: writes or deliveries take effect at statement time and
	// survive ROLLBACK.
	TxNone = "not-transactional"
	// TxUnknown: not measured on the connected build.
	TxUnknown = "unknown"
	// TxNotApplicable: read-only or pure functions; nothing to commit.
	TxNotApplicable = "not-applicable"
)

// Durability of acknowledged writes.
const (
	// DurRestart: committed writes survived a kill of the engine process
	// and a restart in a measured run. The note names the run and whether
	// the kill was a recorded SIGKILL; where the signal was not recorded, a
	// clean shutdown is not ruled out. Power loss was never measured.
	DurRestart = "survives-restart"
	// DurDocumented: the engine documents commit durability; this program
	// did not measure it.
	DurDocumented = "engine-documented"
	// DurNone: nothing is persisted.
	DurNone          = "not-durable"
	DurUnknown       = "unknown"
	DurNotApplicable = "not-applicable"
)

// Evidence sources. Paths are repository-relative so the consistency test
// can open them.
const (
	SrcCapabilityReport = "conformance/live/orm/capabilities.nucleus.json"
	SrcX02Leg           = "conformance/live/orm/x02-nucleus-leg.mjs"
	SrcX03Leg           = "conformance/live/orm/x03-nucleus-leg.mjs"
	SrcX05Leg           = "conformance/live/orm/x05-nucleus-leg.mjs"
	SrcX01Leg           = "conformance/live/orm/x01-nucleus-leg.mjs"
	SrcX04Battery       = "typescript/packages/neutron-nucleus/src/models.live.test.ts"
	SrcSpecialtyGate    = "typescript/packages/neutron-nucleus/src/capabilities.ts"
	SrcModelSemantics   = "nucleus/docs/MODEL_SEMANTICS.md"
	SrcPostgresDocs     = "postgresql-documentation"
	SrcLiveSettings     = "live-connection-settings"
)

// Evidence is one fact behind a limit. For the capability report, Ref is a
// probe id and Observed its recorded status; for a conformance leg, Ref is
// the verdict/check name found in that leg's source and Observed is "pass"
// (the leg exits non-zero when any verdict fails); for documents, Ref is the
// section and Observed "documented". Supports names the ModelLimits fields
// this fact bears on; limits_report_test.go checks each against what the
// cited probe or verdict actually measured.
type Evidence struct {
	Source   string   `json:"source"`
	Ref      string   `json:"ref"`
	Observed string   `json:"observed"`
	Supports []string `json:"supports"`
}

// Fields an Evidence item can support.
const (
	FieldAvailability  = "availability"
	FieldTransaction   = "transaction"
	FieldDurability    = "durability"
	FieldAtomicWithSQL = "atomicWithSql"
	FieldWarnings      = "warnings"
)

const (
	fAvail = FieldAvailability
	fTx    = FieldTransaction
	fDur   = FieldDurability
	fAtom  = FieldAtomicWithSQL
	fWarn  = FieldWarnings
)

// Measured reports whether the evidence comes from a run against the named
// engine build (capability report or leg), not from prose.
func (e Evidence) Measured() bool {
	switch e.Source {
	case SrcCapabilityReport, SrcX01Leg, SrcX02Leg, SrcX03Leg, SrcX05Leg, SrcX04Battery, SrcSpecialtyGate, SrcLiveSettings:
		return true
	}
	return false
}

// ModelLimits is what one data model on the connected engine actually
// guarantees. UI surfaces render it where the user acts; nothing may imply
// more than it states.
type ModelLimits struct {
	Model string `json:"model"`
	Label string `json:"label"`
	// Availability of the model's surface on this engine.
	Availability       Status `json:"availability"`
	AvailabilityReason string `json:"availabilityReason"`
	Transaction        string `json:"transaction"`
	TransactionNote    string `json:"transactionNote"`
	Durability         string `json:"durability"`
	DurabilityNote     string `json:"durabilityNote"`
	// AtomicWithSQL: whether this model's writes commit atomically with SQL
	// rows written in the same transaction (isolated, all-or-nothing).
	AtomicWithSQL Status `json:"atomicWithSql"`
	// Warnings are hazards the user must see before acting.
	Warnings []string   `json:"warnings"`
	Evidence []Evidence `json:"evidence"`
}

// Engine identifies the connected server (FRAMEWORK_CONTRACT §1).
type Engine struct {
	Product string `json:"product"` // postgres | nucleus | unknown
	Version string `json:"version"`
	Raw     string `json:"raw"`
}

// MeasuredBuild names the engine build the Nucleus limits were measured
// against. The consistency test pins these to the capability report.
type MeasuredBuild struct {
	Report         string `json:"report"`
	NucleusVersion string `json:"nucleusVersion"`
	NucleusTree    string `json:"nucleusTree"`
	Recorded       string `json:"recorded"`
}

// Measured is the build the capability report and the X01-X05 legs ran on.
var Measured = MeasuredBuild{
	Report:         SrcCapabilityReport,
	NucleusVersion: "1.0.2",
	NucleusTree:    "3313729ae51300b67b77b2181ee87ac5287dfdec",
	Recorded:       "2026-09-24",
}

// LiveSettings are PostgreSQL settings read from the connection itself: the
// durability and isolation defaults actually in force, not assumed.
type LiveSettings struct {
	Fsync             string `json:"fsync,omitempty"`
	SynchronousCommit string `json:"synchronousCommit,omitempty"`
	DefaultIsolation  string `json:"defaultTransactionIsolation,omitempty"`
	DefaultReadOnly   string `json:"defaultTransactionReadOnly,omitempty"`
	Error             string `json:"error,omitempty"`
}

// Report is the per-connection limits document.
type Report struct {
	Engine Engine `json:"engine"`
	// Measured is the build the Nucleus rows were measured on.
	Measured MeasuredBuild `json:"measured"`
	// Current: the connected engine is the measured build (always true for
	// PostgreSQL rows, which rest on engine semantics and live settings).
	Current bool `json:"current"`
	// CurrentNote explains a non-current report, and for a current Nucleus
	// report how "current" was decided.
	CurrentNote string        `json:"currentNote,omitempty"`
	Live        *LiveSettings `json:"live,omitempty"`
	Models      []ModelLimits `json:"models"`
}

// Model returns the limits for one model, if present.
func (r *Report) Model(name string) (ModelLimits, bool) {
	for _, m := range r.Models {
		if m.Model == name {
			return m, true
		}
	}
	return ModelLimits{}, false
}

// DetectEngine identifies the connected engine from SELECT VERSION().
func DetectEngine(ctx context.Context, client *db.Client) (Engine, error) {
	isNucleus, version, err := client.IsNucleus(ctx)
	if err != nil {
		return Engine{Product: "unknown"}, err
	}
	if isNucleus {
		raw := ""
		_ = client.QueryRow(ctx, "SELECT VERSION()").Scan(&raw)
		return Engine{Product: "nucleus", Version: version, Raw: raw}, nil
	}
	return EngineFromVersion(version), nil
}

// EngineFromVersion classifies a VERSION() string that is not Nucleus's.
func EngineFromVersion(raw string) Engine {
	if strings.Contains(raw, "Nucleus") {
		v := raw
		if i := strings.Index(raw, "Nucleus "); i >= 0 {
			v = raw[i+len("Nucleus "):]
			if j := strings.IndexAny(v, " —)"); j >= 0 {
				v = v[:j]
			}
		}
		return Engine{Product: "nucleus", Version: v, Raw: raw}
	}
	if strings.HasPrefix(raw, "PostgreSQL ") {
		v := strings.TrimPrefix(raw, "PostgreSQL ")
		if j := strings.IndexAny(v, " ,"); j >= 0 {
			v = v[:j]
		}
		return Engine{Product: "postgres", Version: v, Raw: raw}
	}
	return Engine{Product: "unknown", Raw: raw}
}

// ReadLiveSettings reads PostgreSQL's durability/isolation settings from the
// connection. Errors are reported in the result, never guessed around.
func ReadLiveSettings(ctx context.Context, client *db.Client) *LiveSettings {
	ls := &LiveSettings{}
	read := func(name string, dst *string) {
		if ls.Error != "" {
			return
		}
		if err := client.QueryRow(ctx, "SELECT current_setting($1)", name).Scan(dst); err != nil {
			ls.Error = "could not read " + name + ": " + err.Error()
		}
	}
	read("fsync", &ls.Fsync)
	read("synchronous_commit", &ls.SynchronousCommit)
	read("default_transaction_isolation", &ls.DefaultIsolation)
	read("default_transaction_read_only", &ls.DefaultReadOnly)
	return ls
}

// BuildReport assembles the limits for an engine. live may be nil (not
// read); it only affects PostgreSQL rows.
func BuildReport(engine Engine, live *LiveSettings) Report {
	r := Report{Engine: engine, Measured: Measured, Live: live}
	switch engine.Product {
	case "postgres":
		r.Current = true
		r.Models = postgresLimits(live)
	case "nucleus":
		r.Current = engine.Version == Measured.NucleusVersion
		models := nucleusLimits()
		if r.Current {
			r.CurrentNote = "the connected engine reports Nucleus " + Measured.NucleusVersion +
				", the measured build; builds are matched by version string only (the nucleus/ tree cannot be read over the connection), so a different tree reporting the same version is treated as measured"
		} else {
			r.CurrentNote = "limits were measured on Nucleus " + Measured.NucleusVersion +
				" (nucleus/ tree " + Measured.NucleusTree[:12] + "); the connected build reports " +
				quoteOrUnknown(engine.Version) + " and has not been measured, so every status below is unknown"
			for i := range models {
				models[i] = demoteUnmeasured(models[i], engine.Version)
			}
		}
		r.Models = models
	default:
		r.CurrentNote = "the connected server is neither PostgreSQL nor Nucleus; nothing about it is measured"
		r.Models = unknownLimits()
	}
	return r
}

func quoteOrUnknown(v string) string {
	if v == "" {
		return "no version"
	}
	return v
}

// demoteUnmeasured keeps the measured facts as context but makes every
// status unknown: a different build may behave differently in either
// direction.
func demoteUnmeasured(m ModelLimits, version string) ModelLimits {
	prefix := "measured on Nucleus " + Measured.NucleusVersion + ", not on " + quoteOrUnknown(version) + ": "
	m.AvailabilityReason = prefix + m.AvailabilityReason
	if m.Availability != Unknown {
		m.Availability = Unknown
	}
	if m.Transaction != TxNotApplicable {
		m.TransactionNote = prefix + m.TransactionNote
		m.Transaction = TxUnknown
	}
	if m.Durability != DurNotApplicable {
		m.DurabilityNote = prefix + m.DurabilityNote
		m.Durability = DurUnknown
	}
	if m.AtomicWithSQL == Supported {
		m.AtomicWithSQL = Unknown
	}
	return m
}

// AllModels is the fixed model order used by every report.
var AllModels = []string{
	"sql", "kv", "vector", "timeseries", "document", "graph",
	"fts", "geo", "blob", "streams", "columnar", "datalog", "cdc", "pubsub",
}

var modelLabels = map[string]string{
	"sql": "SQL", "kv": "Key-Value", "vector": "Vector", "timeseries": "TimeSeries",
	"document": "Document", "graph": "Graph", "fts": "Full-Text", "geo": "Geo",
	"blob": "Blob", "streams": "Streams", "columnar": "Columnar", "datalog": "Datalog",
	"cdc": "CDC", "pubsub": "PubSub",
}

func unknownLimits() []ModelLimits {
	out := make([]ModelLimits, 0, len(AllModels))
	for _, m := range AllModels {
		out = append(out, ModelLimits{
			Model: m, Label: modelLabels[m],
			Availability: Unknown, AvailabilityReason: "unrecognized engine",
			Transaction: TxUnknown, TransactionNote: "not measured",
			Durability: DurUnknown, DurabilityNote: "not measured",
			AtomicWithSQL: Unknown,
			Warnings:      []string{}, Evidence: []Evidence{},
		})
	}
	return out
}

func postgresLimits(live *LiveSettings) []ModelLimits {
	sql := ModelLimits{
		Model: "sql", Label: "SQL",
		Availability:       Supported,
		AvailabilityReason: "PostgreSQL; the conformance probes pass here as the control (run.mjs --control)",
		Transaction:        TxAtomic,
		TransactionNote:    "PostgreSQL transactions: DML and DDL commit or roll back together; Studio row commits and schema applies each run as one transaction",
		Durability:         DurDocumented,
		DurabilityNote:     "PostgreSQL WAL; a commit is durable when fsync and synchronous_commit are on",
		AtomicWithSQL:      Supported,
		Warnings:           []string{},
		Evidence: []Evidence{
			{Source: SrcPostgresDocs, Ref: "Transactions / WAL reliability", Observed: "documented", Supports: []string{fAvail, fTx, fDur, fAtom}},
		},
	}
	if live != nil {
		sql.Evidence = append(sql.Evidence, Evidence{Source: SrcLiveSettings, Ref: "fsync, synchronous_commit, default_transaction_isolation", Observed: "read", Supports: []string{fDur, fTx, fWarn}})
		switch {
		case live.Error != "":
			sql.Durability = DurUnknown
			sql.DurabilityNote = "the connection's durability settings could not be read: " + live.Error
		case live.Fsync == "off":
			sql.Durability = DurNone
			sql.DurabilityNote = "fsync is off on this server: a crash can corrupt or lose committed data"
			sql.Warnings = append(sql.Warnings, "fsync=off: committed data is not crash-safe on this server")
		case live.SynchronousCommit == "off":
			sql.DurabilityNote = "synchronous_commit is off: the most recent commits can be lost on a crash (the database stays consistent)"
			sql.Warnings = append(sql.Warnings, "synchronous_commit=off: recently acknowledged commits can be lost on a crash")
		default:
			sql.DurabilityNote = "fsync=" + live.Fsync + ", synchronous_commit=" + live.SynchronousCommit + " on this connection: commits are WAL-flushed before acknowledgement"
		}
		if live.DefaultIsolation != "" {
			sql.TransactionNote += "; default isolation on this connection: " + live.DefaultIsolation
		}
	}
	out := []ModelLimits{sql}
	for _, m := range AllModels[1:] {
		reason := "Nucleus model; the connected engine is PostgreSQL"
		switch m {
		case "vector":
			reason = "Nucleus vector model is not present; pgvector columns, when the extension is installed, are ordinary SQL columns with SQL's limits"
		case "fts":
			reason = "Nucleus FTS model is not present; PostgreSQL tsvector columns are ordinary SQL columns with SQL's limits"
		case "cdc":
			reason = "Nucleus CDC is not present; Studio does not surface PostgreSQL logical decoding"
		case "pubsub":
			reason = "Nucleus pub/sub is not present; PostgreSQL LISTEN/NOTIFY is reachable through @neutron-build/sql's listen-notify module, not Studio"
		}
		out = append(out, ModelLimits{
			Model: m, Label: modelLabels[m],
			Availability: Unsupported, AvailabilityReason: reason,
			Transaction: TxNotApplicable, TransactionNote: "model not available on this engine",
			Durability: DurNotApplicable, DurabilityNote: "model not available on this engine",
			AtomicWithSQL: Unsupported,
			Warnings:      []string{}, Evidence: []Evidence{},
		})
	}
	return out
}

// report is shorthand for capability-report evidence.
func report(probe, observed string, supports ...string) Evidence {
	return Evidence{Source: SrcCapabilityReport, Ref: probe, Observed: observed, Supports: supports}
}

func leg(src, ref string, supports ...string) Evidence {
	return Evidence{Source: src, Ref: ref, Observed: "pass", Supports: supports}
}

func gate(capability, observed string, supports ...string) Evidence {
	return Evidence{Source: SrcSpecialtyGate, Ref: capability, Observed: observed, Supports: supports}
}

func docs(section string, supports ...string) Evidence {
	return Evidence{Source: SrcModelSemantics, Ref: section, Observed: "documented", Supports: supports}
}

// nucleusLimits is the registry for the measured Nucleus build. Every
// supported/transactional/durable claim cites measured evidence; prose from
// the engine's semantics document only ever supports a WEAKER statement or a
// warning (limits_report_test.go enforces both).
func nucleusLimits() []ModelLimits {
	return []ModelLimits{
		{
			Model: "sql", Label: "SQL",
			Availability:       Supported,
			AvailabilityReason: "relational SQL over pgwire; the ORM capability report lists what the ORM can rely on (71/140 probes supported through pg)",
			Transaction:        TxPartial,
			TransactionNote:    "DML commits and rolls back (savepoints included) and other sessions do not see uncommitted rows; DDL is NOT transactional (it survives ROLLBACK and is visible before COMMIT); isolation levels and READ ONLY are not applied (every level behaves as read committed)",
			Durability:         DurRestart,
			DurabilityNote:     "committed rows survived a kill of the engine process and a restart in the X04 restart battery; the signal was not recorded (SIGKILL is not claimed) and power loss was not measured",
			AtomicWithSQL:      Supported,
			Warnings: []string{
				"DDL runs outside the transaction: a failed migration leaves earlier statements applied",
				"BEGIN READ ONLY does not stop writes on this engine",
				"no advisory locks, statement_timeout or query cancellation",
			},
			Evidence: []Evidence{
				report("txn.savepoint_rollback", "supported", fAvail, fTx),
				report("txn.read_committed_sees_commits", "supported", fTx),
				report("ddl.error_aborts_transaction", "supported", fTx),
				report("ddl.create_table_rollback", "unsupported", fTx, fWarn),
				report("ddl.failed_migration_all_or_nothing", "unsupported", fTx, fWarn),
				report("ddl.uncommitted_ddl_invisible", "unsupported", fTx),
				report("txn.isolation_levels_applied", "unsupported", fTx),
				report("txn.read_only_rejects_writes", "unsupported", fWarn),
				report("lock.advisory_session", "unsupported", fWarn),
				report("lock.statement_timeout", "unsupported", fWarn),
				leg(SrcX02Leg, "rollbackRevertsAll", fTx),
				leg(SrcX02Leg, "dirtyReads", fTx),
				leg(SrcX04Battery, "restart post verified", fDur),
			},
		},
		{
			Model: "kv", Label: "Key-Value",
			Availability:       Supported,
			AvailabilityReason: "string/typed values, TTLs, namespaces and collections pass the X04 live battery",
			Transaction:        TxUnknown,
			TransactionNote:    "not measured on this build; the engine documents that ROLLBACK undoes scalar KV writes without isolating them from other sessions, and that collection (list/hash/set) writes are refused inside a transaction",
			Durability:         DurRestart,
			DurabilityNote:     "a KV value and a TTL key's remaining lifetime survived a kill of the engine process and a restart in the X04 restart battery; the signal was not recorded (SIGKILL is not claimed) and power loss was not measured",
			AtomicWithSQL:      Unsupported,
			Warnings: []string{
				"one global keyspace: namespaces are key prefixes, not permissions",
				"other sessions can read uncommitted KV writes (engine semantics document)",
			},
			Evidence: []Evidence{
				leg(SrcX04Battery, "kv: namespace isolation live", fAvail, fWarn),
				leg(SrcX04Battery, "restart post verified", fDur),
				docs("KV (scalar keys)", fTx, fAtom, fWarn),
			},
		},
		{
			Model: "vector", Label: "Vector",
			Availability:       Unknown,
			AvailabilityReason: "pgvector-style SQL columns are unsupported on this build (the ORM's vector capability resolves unsupported, 0A000); the Nucleus VECTOR_* function surface Studio browses is not measured",
			Transaction:        TxUnknown,
			TransactionNote:    "not measured on this build",
			Durability:         DurUnknown,
			DurabilityNote:     "not measured on this build",
			AtomicWithSQL:      Unknown,
			Warnings:           []string{"vector results are not verified against an independent oracle on this engine"},
			Evidence: []Evidence{
				leg(SrcX01Leg, "vector-type", fAvail),
			},
		},
		{
			Model: "timeseries", Label: "TimeSeries",
			Availability:       Supported,
			AvailabilityReason: "TS_* insert, count and range reads pass the X03 leg (range scoping checked against hand oracles)",
			Transaction:        TxUnknown,
			TransactionNote:    "not measured on this build",
			Durability:         DurRestart,
			DurabilityNote:     "points survived SIGKILL and restart in the X03 leg; power loss was not measured",
			AtomicWithSQL:      Unknown,
			Warnings: []string{
				"retention policies are global and retroactive: they delete existing and backfilled points of every series",
			},
			Evidence: []Evidence{
				leg(SrcX03Leg, "afterKill9", fDur),
				leg(SrcX03Leg, "rangeCountScopingExact", fAvail),
			},
		},
		{
			Model: "document", Label: "Document",
			Availability:       Supported,
			AvailabilityReason: "collection-scoped DOC_* functions resolve supported (value-asserting probes, X02)",
			Transaction:        TxRollbackNotIsolated,
			TransactionNote:    "ROLLBACK removes the transaction's documents and COMMIT publishes them, but other sessions read them before COMMIT; Studio and the client run each document call as its own statement",
			Durability:         DurRestart,
			DurabilityNote:     "committed documents survived SIGKILL and restart; an open transaction's documents were gone (X02 leg); power loss was not measured",
			AtomicWithSQL:      Unsupported,
			Warnings: []string{
				"no isolation: other sessions see uncommitted documents",
				"collections are namespaces, not access control",
			},
			Evidence: []Evidence{
				gate("document-collections", "supported", fAvail),
				gate("specialty-session-isolation", "unsupported", fTx, fWarn),
				gate("atomic-sql-specialty-writes", "unsupported", fAtom),
				leg(SrcX02Leg, "rollbackRevertsAll", fTx),
				leg(SrcX02Leg, "dirtyReads", fTx, fAtom, fWarn),
				leg(SrcX02Leg, "committedDocument", fDur),
			},
		},
		{
			Model: "graph", Label: "Graph",
			Availability:       Supported,
			AvailabilityReason: "GRAPH_NODE/GRAPH_NEIGHBORS and property matching resolve supported (X02); traversal is bounded client-side",
			Transaction:        TxRollbackNotIsolated,
			TransactionNote:    "ROLLBACK removes the transaction's nodes and COMMIT publishes them, but other sessions read them before COMMIT",
			Durability:         DurRestart,
			DurabilityNote:     "committed nodes survived SIGKILL and restart (X02 leg); power loss was not measured",
			AtomicWithSQL:      Unsupported,
			Warnings: []string{
				"one global graph: no tenant, namespace or permission boundary",
				"no isolation: other sessions see uncommitted nodes",
				"GRAPH_QUERY takes Cypher text only (no parameters)",
			},
			Evidence: []Evidence{
				gate("graph-adjacency", "supported", fAvail),
				gate("graph-property-match", "supported", fAvail),
				gate("graph-tenant-isolation", "unsupported", fWarn),
				leg(SrcX02Leg, "rollbackRevertsAll", fTx),
				leg(SrcX02Leg, "dirtyReads", fTx, fAtom, fWarn),
				leg(SrcX02Leg, "committedGraph", fDur),
			},
		},
		{
			Model: "fts", Label: "Full-Text",
			Availability:       Unknown,
			AvailabilityReason: "PostgreSQL-style full-text functions are faked on this build (to_tsvector returns its argument, ts_rank is constant) and resolve unsupported; the FTS_* document index Studio browses is not measured",
			Transaction:        TxUnknown,
			TransactionNote:    "not measured on this build; the engine documents that FTS_* index writes happen at statement time and are only best-effort undone by ROLLBACK",
			Durability:         DurUnknown,
			DurabilityNote:     "not measured on this build",
			AtomicWithSQL:      Unsupported,
			Warnings:           []string{"FTS_* index writes are not tied to the SQL transaction (engine semantics document)"},
			Evidence: []Evidence{
				leg(SrcX01Leg, "fts-functions", fAvail),
				docs("Full-text search", fTx, fAtom, fWarn),
			},
		},
		{
			Model: "geo", Label: "Geo",
			Availability:       Supported,
			AvailabilityReason: "GEO_* functions match hand oracles; layers are ordinary SQL tables (X04 live battery)",
			Transaction:        TxNotApplicable,
			TransactionNote:    "pure functions hold no state; layer rows are SQL rows with SQL's limits",
			Durability:         DurNotApplicable,
			DurabilityNote:     "no geo store exists; layer rows are SQL rows",
			AtomicWithSQL:      Supported,
			Warnings:           []string{},
			Evidence: []Evidence{
				leg(SrcX04Battery, "geo: predicates match hand oracles", fAvail),
				leg(SrcX04Battery, "geo: layer journey", fAtom),
			},
		},
		{
			Model: "blob", Label: "Blob",
			Availability:       Supported,
			AvailabilityReason: "byte-exact round trips, ranges and streaming pass the X04 live battery",
			Transaction:        TxUnknown,
			TransactionNote:    "not measured on this build",
			Durability:         DurRestart,
			DurabilityNote:     "a blob's bytes and content type survived a kill of the engine process and a restart in the X04 restart battery; the signal was not recorded (SIGKILL is not claimed) and power loss was not measured",
			AtomicWithSQL:      Unsupported,
			Warnings:           []string{"blob writes are separate statements from any SQL row that references them"},
			Evidence: []Evidence{
				leg(SrcX04Battery, "blob: byte-exact round-trip", fAvail),
				leg(SrcX04Battery, "restart post verified", fDur),
			},
		},
		{
			Model: "streams", Label: "Streams",
			Availability:       Supported,
			AvailabilityReason: "STREAM_* append, range, consumer groups and acks pass the X05 leg",
			Transaction:        TxRollbackNotIsolated,
			TransactionNote:    "ROLLBACK removes a pending append (X05 leg); isolation from other sessions is not measured",
			Durability:         DurRestart,
			DurabilityNote:     "entries, groups, cursors and acks survived SIGKILL and restart (X05 leg); power loss was not measured",
			AtomicWithSQL:      Unknown,
			Warnings: []string{
				"resume with the last entry's full id: a bare millisecond cursor silently skips entries in that millisecond",
				"consumer-group delivery is at-most-once",
			},
			Evidence: []Evidence{
				leg(SrcX05Leg, "streams.roundtrip_ordering", fAvail),
				leg(SrcX05Leg, "streams.rollback_removes_pending_append", fTx),
				leg(SrcX05Leg, "streams.restart_entries_group_cursor_ack", fDur),
				leg(SrcX05Leg, "streams.resume_cursor_full_id_vs_bare_ms", fWarn),
				leg(SrcX05Leg, "streams.group_at_most_once_ack", fWarn),
			},
		},
		{
			Model: "columnar", Label: "Columnar",
			Availability:       Supported,
			AvailabilityReason: "COLUMNAR_* store and engine='columnar' tables pass the X03 leg",
			Transaction:        TxRefused,
			TransactionNote:    "COLUMNAR_* store inserts are refused inside an explicit transaction and are append-only; engine='columnar' SQL tables honour ROLLBACK like SQL tables",
			Durability:         DurRestart,
			DurabilityNote:     "store rows survived SIGKILL and restart (X03 leg); power loss was not measured",
			AtomicWithSQL:      Unsupported,
			Warnings: []string{
				"COLUMNAR_* inserts cannot join a transaction",
				"numbers passed as quoted text are stored as text and aggregate wrongly",
			},
			Evidence: []Evidence{
				leg(SrcX03Leg, "castAggregatesExact", fAvail, fWarn),
				leg(SrcX03Leg, "inTxInsert", fTx, fWarn),
				leg(SrcX03Leg, "rollbackOnEngineTable", fTx),
				leg(SrcX03Leg, "afterKill9", fDur),
			},
		},
		{
			Model: "datalog", Label: "Datalog",
			Availability:       Supported,
			AvailabilityReason: "DATALOG_* facts, rules and recursive queries pass the X05 leg",
			Transaction:        TxUnknown,
			TransactionNote:    "not measured on this build; the engine documents that a rolled-back DATALOG_ASSERT can return after a restart (its log is not compensated on rollback)",
			Durability:         DurRestart,
			DurabilityNote:     "facts and rules survived SIGKILL and restart (X05 leg); power loss was not measured",
			AtomicWithSQL:      Unsupported,
			Warnings:           []string{"a rolled-back assertion can reappear after a restart (engine semantics document)"},
			Evidence: []Evidence{
				leg(SrcX05Leg, "datalog.facts_and_rules_survive_restart", fDur),
				leg(SrcX05Leg, "datalog.roundtrip_recursion_oracle", fAvail),
				docs("Datalog", fTx, fAtom, fWarn),
			},
		},
		{
			Model: "cdc", Label: "CDC",
			Availability:       Supported,
			AvailabilityReason: "CDC_READ / CDC_TABLE_READ pass the X05 leg (metadata-only events)",
			Transaction:        TxNone,
			TransactionNote:    "events are emitted at statement time, before COMMIT; a rolled-back transaction's events stay in the log with no compensating record",
			Durability:         DurRestart,
			DurabilityNote:     "the log replayed and its sequence continued after SIGKILL and restart (X05 leg); per-event fsync is not measured",
			AtomicWithSQL:      Unsupported,
			Warnings: []string{
				"only INSERT statements emit events on this build: UPDATE and DELETE never reach the log",
				"events are metadata only (seq, table, change, ts) and include rolled-back work",
			},
			Evidence: []Evidence{
				leg(SrcX05Leg, "cdc.delivery_shape", fAvail, fWarn),
				leg(SrcX05Leg, "cdc.pre_commit_emission_and_gaps", fTx, fAtom, fWarn),
				leg(SrcX05Leg, "cdc.restart_replay_seq_continues", fDur),
			},
		},
		{
			Model: "pubsub", Label: "PubSub",
			Availability:       Supported,
			AvailabilityReason: "LISTEN/NOTIFY cross-connection delivery and the PUBSUB_* publish surface pass the X05 leg, with divergences",
			Transaction:        TxNone,
			TransactionNote:    "notifications sent inside a rolled-back transaction are still delivered",
			Durability:         DurNone,
			DurabilityNote:     "in-memory fan-out: nothing is persisted, and a message published with no subscriber is dropped (engine semantics document)",
			AtomicWithSQL:      Unsupported,
			Warnings: []string{
				"an idle listener receives nothing until it runs a statement",
				"messages are lost on restart",
			},
			Evidence: []Evidence{
				leg(SrcX05Leg, "listen.rollback_tx_pre_commit_divergence", fTx, fAtom),
				leg(SrcX05Leg, "listen.cross_connection_delivery_flush_divergence", fAvail, fWarn),
				docs("Pub/Sub", fDur, fWarn),
			},
		},
	}
}
