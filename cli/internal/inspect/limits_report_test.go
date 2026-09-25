package inspect

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// repoRoot is the repository root relative to this package directory.
const repoRoot = "../../.."

type capabilityReport struct {
	Engine struct {
		Product string `json:"product"`
		Version string `json:"version"`
	} `json:"engine"`
	Source struct {
		NucleusTree string `json:"nucleusTree"`
	} `json:"source"`
	GeneratedAt  string `json:"generatedAt"`
	Capabilities []struct {
		ID     string            `json:"id"`
		Status map[string]string `json:"status"`
	} `json:"capabilities"`
}

func loadCapabilityReport(t *testing.T) capabilityReport {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot, SrcCapabilityReport))
	if err != nil {
		t.Fatalf("read capability report: %v", err)
	}
	var r capabilityReport
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("parse capability report: %v", err)
	}
	return r
}

// The measured build the limits cite IS the build the capability report
// was recorded on.
func TestMeasuredBuildMatchesCapabilityReport(t *testing.T) {
	r := loadCapabilityReport(t)
	if r.Engine.Product != "nucleus" || r.Engine.Version != Measured.NucleusVersion {
		t.Fatalf("report engine %s %s, limits cite Nucleus %s", r.Engine.Product, r.Engine.Version, Measured.NucleusVersion)
	}
	if r.Source.NucleusTree != Measured.NucleusTree {
		t.Fatalf("report nucleus tree %s, limits cite %s", r.Source.NucleusTree, Measured.NucleusTree)
	}
	if r.GeneratedAt != Measured.Recorded {
		t.Fatalf("report recorded %s, limits cite %s", r.GeneratedAt, Measured.Recorded)
	}
}

// Every cited probe exists and carries exactly the observed status on BOTH
// drivers; every cited leg verdict exists in that leg's source.
func TestEvidenceTracesToSources(t *testing.T) {
	r := loadCapabilityReport(t)
	probes := map[string]map[string]string{}
	for _, c := range r.Capabilities {
		probes[c.ID] = c.Status
	}
	sources := map[string]string{}
	read := func(src string) string {
		if s, ok := sources[src]; ok {
			return s
		}
		raw, err := os.ReadFile(filepath.Join(repoRoot, src))
		if err != nil {
			t.Fatalf("evidence source %s unreadable: %v", src, err)
		}
		sources[src] = string(raw)
		return sources[src]
	}
	gate := read(SrcSpecialtyGate)
	absentStart := strings.Index(gate, "const MEASURED_ABSENT")
	probesStart := strings.Index(gate, "const PROBES")
	if absentStart < 0 || probesStart < 0 || probesStart > absentStart {
		t.Fatalf("capabilities.ts layout changed; update this test")
	}
	probeBlock, absentBlock := gate[probesStart:absentStart], gate[absentStart:]

	for _, report := range []Report{
		BuildReport(Engine{Product: "nucleus", Version: Measured.NucleusVersion}, nil),
		BuildReport(Engine{Product: "postgres", Version: "17.0"}, &LiveSettings{Fsync: "on", SynchronousCommit: "on"}),
	} {
		for _, m := range report.Models {
			for _, ev := range m.Evidence {
				switch ev.Source {
				case SrcCapabilityReport:
					st, ok := probes[ev.Ref]
					if !ok {
						t.Errorf("%s/%s cites probe %s, absent from the report", report.Engine.Product, m.Model, ev.Ref)
						continue
					}
					for _, driver := range []string{"pg", "postgres"} {
						if st[driver] != ev.Observed {
							t.Errorf("%s/%s cites %s as %s; report says %s via %s", report.Engine.Product, m.Model, ev.Ref, ev.Observed, st[driver], driver)
						}
					}
				case SrcSpecialtyGate:
					block := probeBlock
					if ev.Observed == string(Unsupported) {
						block = absentBlock
					}
					if !strings.Contains(block, "'"+ev.Ref+"'") {
						t.Errorf("%s/%s cites gate capability %s as %s; capabilities.ts does not resolve it that way", report.Engine.Product, m.Model, ev.Ref, ev.Observed)
					}
				case SrcX01Leg, SrcX02Leg, SrcX03Leg, SrcX05Leg, SrcX04Battery, SrcModelSemantics:
					if !strings.Contains(read(ev.Source), ev.Ref) {
						t.Errorf("%s/%s cites %q in %s; not found", report.Engine.Product, m.Model, ev.Ref, ev.Source)
					}
				case SrcPostgresDocs, SrcLiveSettings:
					if report.Engine.Product != "postgres" {
						t.Errorf("%s/%s cites PostgreSQL-only evidence", report.Engine.Product, m.Model)
					}
				default:
					t.Errorf("%s/%s cites unknown source %s", report.Engine.Product, m.Model, ev.Source)
				}
			}
		}
	}
}

// No limit implies more than its evidence establishes.
func TestLimitsNeverOverclaim(t *testing.T) {
	nucleus := BuildReport(Engine{Product: "nucleus", Version: Measured.NucleusVersion}, nil)
	pg := BuildReport(Engine{Product: "postgres", Version: "17.0"}, nil)
	for _, rep := range []Report{nucleus, pg} {
		if len(rep.Models) != len(AllModels) {
			t.Fatalf("%s: %d models, want %d", rep.Engine.Product, len(rep.Models), len(AllModels))
		}
		for i, m := range rep.Models {
			if m.Model != AllModels[i] {
				t.Errorf("%s: model order %s at %d, want %s", rep.Engine.Product, m.Model, i, AllModels[i])
			}
			// Atomicity is claimed only for PostgreSQL SQL.
			if m.Transaction == TxAtomic && !(rep.Engine.Product == "postgres" && m.Model == "sql") {
				t.Errorf("%s/%s claims atomic transactions", rep.Engine.Product, m.Model)
			}
			if rep.Engine.Product != "nucleus" {
				continue
			}
			if m.Durability == DurDocumented {
				t.Errorf("nucleus/%s: durability must be measured or unknown, not documented", m.Model)
			}
			if m.AtomicWithSQL == Supported && m.Model != "sql" && m.Model != "geo" {
				t.Errorf("nucleus/%s claims atomicity with SQL", m.Model)
			}
		}
	}
	// SQL on Nucleus must carry the non-transactional DDL and ignored
	// READ ONLY facts: the report records both unsupported.
	sql, _ := nucleus.Model("sql")
	if sql.Transaction != TxPartial || !strings.Contains(sql.TransactionNote, "DDL is NOT transactional") {
		t.Errorf("nucleus/sql transaction = %s %q", sql.Transaction, sql.TransactionNote)
	}
}

func TestUnmeasuredBuildDemotesEverything(t *testing.T) {
	rep := BuildReport(Engine{Product: "nucleus", Version: "9.9.9"}, nil)
	if rep.Current || !strings.Contains(rep.CurrentNote, "9.9.9") {
		t.Fatalf("current=%v note=%q", rep.Current, rep.CurrentNote)
	}
	for _, m := range rep.Models {
		if m.Availability != Unknown {
			t.Errorf("%s availability %s on an unmeasured build", m.Model, m.Availability)
		}
		if m.Transaction != TxUnknown && m.Transaction != TxNotApplicable {
			t.Errorf("%s transaction %s on an unmeasured build", m.Model, m.Transaction)
		}
		if m.Durability != DurUnknown && m.Durability != DurNotApplicable {
			t.Errorf("%s durability %s on an unmeasured build", m.Model, m.Durability)
		}
		if m.AtomicWithSQL == Supported {
			t.Errorf("%s atomicWithSql supported on an unmeasured build", m.Model)
		}
	}
	unk := BuildReport(Engine{Product: "unknown", Raw: "CockroachDB"}, nil)
	for _, m := range unk.Models {
		if m.Availability != Unknown {
			t.Errorf("unknown engine: %s availability %s", m.Model, m.Availability)
		}
	}
}

func TestPostgresLiveSettingsDriveDurability(t *testing.T) {
	cases := []struct {
		live *LiveSettings
		want string
		warn bool
	}{
		{&LiveSettings{Fsync: "on", SynchronousCommit: "on"}, DurDocumented, false},
		{&LiveSettings{Fsync: "on", SynchronousCommit: "off"}, DurDocumented, true},
		{&LiveSettings{Fsync: "off", SynchronousCommit: "on"}, DurNone, true},
		{&LiveSettings{Error: "permission denied"}, DurUnknown, false},
	}
	for _, c := range cases {
		rep := BuildReport(Engine{Product: "postgres"}, c.live)
		sql, _ := rep.Model("sql")
		if sql.Durability != c.want {
			t.Errorf("%+v: durability %s, want %s", c.live, sql.Durability, c.want)
		}
		if (len(sql.Warnings) > 0) != c.warn {
			t.Errorf("%+v: warnings %v", c.live, sql.Warnings)
		}
	}
	rep := BuildReport(Engine{Product: "postgres"}, nil)
	for _, m := range rep.Models[1:] {
		if m.Availability != Unsupported {
			t.Errorf("postgres/%s availability %s", m.Model, m.Availability)
		}
	}
}

func TestEngineFromVersion(t *testing.T) {
	cases := map[string]Engine{
		"PostgreSQL 17.11 on aarch64-apple-darwin":                  {Product: "postgres", Version: "17.11"},
		"PostgreSQL 16.0 (Nucleus 1.0.2 — The Definitive Database)": {Product: "nucleus", Version: "1.0.2"},
		"CockroachDB CCL v23":                                       {Product: "unknown"},
	}
	for raw, want := range cases {
		got := EngineFromVersion(raw)
		if got.Product != want.Product || got.Version != want.Version {
			t.Errorf("%q -> %+v, want %+v", raw, got, want)
		}
	}
}

// measuredFacts is what each measured probe or verdict the registry may cite
// actually observed, read from the probe definitions and leg sources, not
// from limits.go: per model it covered, the fields it bears on and the
// positive claim values it establishes for each (an empty list: it bears on
// the field only as a limitation). sigkill marks restart runs whose kill was
// a recorded SIGKILL. Adding a row here is a reviewable statement about a
// measurement; limits.go cannot upgrade a claim without one.
type fact struct {
	models  map[string]map[string][]string
	sigkill bool
}

func one(model string, fields map[string][]string) map[string]map[string][]string {
	return map[string]map[string][]string{model: fields}
}

var (
	none         = []string{}
	avail        = []string{string(Supported)}
	atomicWith   = []string{string(Supported)}
	survives     = []string{DurRestart}
	partialTx    = []string{TxPartial}
	rollbackOnly = []string{TxRollbackNotIsolated}
)

var measuredFacts = map[string]fact{
	// Capability report (SQL over pgwire).
	SrcCapabilityReport + "|txn.savepoint_rollback":              {models: one("sql", map[string][]string{fAvail: avail, fTx: partialTx})},
	SrcCapabilityReport + "|txn.read_committed_sees_commits":     {models: one("sql", map[string][]string{fTx: partialTx})},
	SrcCapabilityReport + "|ddl.error_aborts_transaction":        {models: one("sql", map[string][]string{fTx: partialTx})},
	SrcCapabilityReport + "|ddl.create_table_rollback":           {models: one("sql", map[string][]string{fTx: none, fWarn: none})},
	SrcCapabilityReport + "|ddl.failed_migration_all_or_nothing": {models: one("sql", map[string][]string{fTx: none, fWarn: none})},
	SrcCapabilityReport + "|ddl.uncommitted_ddl_invisible":       {models: one("sql", map[string][]string{fTx: none})},
	SrcCapabilityReport + "|txn.isolation_levels_applied":        {models: one("sql", map[string][]string{fTx: none})},
	SrcCapabilityReport + "|txn.read_only_rejects_writes":        {models: one("sql", map[string][]string{fWarn: none})},
	SrcCapabilityReport + "|lock.advisory_session":               {models: one("sql", map[string][]string{fWarn: none})},
	SrcCapabilityReport + "|lock.statement_timeout":              {models: one("sql", map[string][]string{fWarn: none})},
	// X02: one transaction wrote a SQL row, a graph node and a document.
	SrcX02Leg + "|rollbackRevertsAll": {models: map[string]map[string][]string{
		"sql": {fTx: partialTx}, "document": {fTx: rollbackOnly}, "graph": {fTx: rollbackOnly},
	}},
	// dirtyReads: the SQL row was invisible to a second session, the node
	// and document visible (isolation for SQL DML only).
	SrcX02Leg + "|dirtyReads": {models: map[string]map[string][]string{
		"sql": {fTx: partialTx}, "document": {fTx: none, fAtom: none, fWarn: none}, "graph": {fTx: none, fAtom: none, fWarn: none},
	}},
	SrcX02Leg + "|committedDocument": {models: one("document", map[string][]string{fDur: survives}), sigkill: true},
	SrcX02Leg + "|committedGraph":    {models: one("graph", map[string][]string{fDur: survives}), sigkill: true},
	// X04: the restart phase checks a KV value + TTL key, a blob and a geo
	// layer table's SQL rows after "kill pid + same start command" (no
	// signal recorded).
	SrcX04Battery + "|restart post verified": {models: map[string]map[string][]string{
		"sql": {fDur: survives}, "kv": {fDur: survives}, "blob": {fDur: survives},
	}},
	SrcX04Battery + "|kv: namespace isolation live":       {models: one("kv", map[string][]string{fAvail: avail, fWarn: none})},
	SrcX04Battery + "|blob: byte-exact round-trip":        {models: one("blob", map[string][]string{fAvail: avail})},
	SrcX04Battery + "|geo: predicates match hand oracles": {models: one("geo", map[string][]string{fAvail: avail})},
	// The layer journey creates the layer with CREATE TABLE and writes it
	// through SQL: layer rows are the SQL transaction's own rows.
	SrcX04Battery + "|geo: layer journey": {models: one("geo", map[string][]string{fAtom: atomicWith})},
	// X01: the pg-style surfaces resolved unsupported.
	SrcX01Leg + "|vector-type":   {models: one("vector", map[string][]string{fAvail: none})},
	SrcX01Leg + "|fts-functions": {models: one("fts", map[string][]string{fAvail: none})},
	// X03.
	SrcX03Leg + "|rangeCountScopingExact": {models: one("timeseries", map[string][]string{fAvail: avail})},
	SrcX03Leg + "|afterKill9": {models: map[string]map[string][]string{
		"timeseries": {fDur: survives}, "columnar": {fDur: survives},
	}, sigkill: true},
	SrcX03Leg + "|castAggregatesExact":   {models: one("columnar", map[string][]string{fAvail: avail, fWarn: none})},
	SrcX03Leg + "|inTxInsert":            {models: one("columnar", map[string][]string{fTx: none, fWarn: none})},
	SrcX03Leg + "|rollbackOnEngineTable": {models: one("columnar", map[string][]string{fTx: none})},
	// X05 (restarts are SIGKILL: the leg's killEngine).
	SrcX05Leg + "|streams.roundtrip_ordering":                        {models: one("streams", map[string][]string{fAvail: avail})},
	SrcX05Leg + "|streams.rollback_removes_pending_append":           {models: one("streams", map[string][]string{fTx: rollbackOnly})},
	SrcX05Leg + "|streams.restart_entries_group_cursor_ack":          {models: one("streams", map[string][]string{fDur: survives}), sigkill: true},
	SrcX05Leg + "|streams.resume_cursor_full_id_vs_bare_ms":          {models: one("streams", map[string][]string{fWarn: none})},
	SrcX05Leg + "|streams.group_at_most_once_ack":                    {models: one("streams", map[string][]string{fWarn: none})},
	SrcX05Leg + "|datalog.roundtrip_recursion_oracle":                {models: one("datalog", map[string][]string{fAvail: avail})},
	SrcX05Leg + "|datalog.facts_and_rules_survive_restart":           {models: one("datalog", map[string][]string{fDur: survives}), sigkill: true},
	SrcX05Leg + "|cdc.delivery_shape":                                {models: one("cdc", map[string][]string{fAvail: avail, fWarn: none})},
	SrcX05Leg + "|cdc.pre_commit_emission_and_gaps":                  {models: one("cdc", map[string][]string{fTx: none, fAtom: none, fWarn: none})},
	SrcX05Leg + "|cdc.restart_replay_seq_continues":                  {models: one("cdc", map[string][]string{fDur: survives}), sigkill: true},
	SrcX05Leg + "|listen.rollback_tx_pre_commit_divergence":          {models: one("pubsub", map[string][]string{fTx: none, fAtom: none})},
	SrcX05Leg + "|listen.cross_connection_delivery_flush_divergence": {models: one("pubsub", map[string][]string{fAvail: avail, fWarn: none})},
	// Specialty capability gate (capabilities.ts, value-asserting probes).
	SrcSpecialtyGate + "|document-collections":        {models: one("document", map[string][]string{fAvail: avail})},
	SrcSpecialtyGate + "|specialty-session-isolation": {models: one("document", map[string][]string{fTx: none, fWarn: none})},
	SrcSpecialtyGate + "|atomic-sql-specialty-writes": {models: one("document", map[string][]string{fAtom: none})},
	SrcSpecialtyGate + "|graph-adjacency":             {models: one("graph", map[string][]string{fAvail: avail})},
	SrcSpecialtyGate + "|graph-property-match":        {models: one("graph", map[string][]string{fAvail: avail})},
	SrcSpecialtyGate + "|graph-tenant-isolation":      {models: one("graph", map[string][]string{fWarn: none})},
}

// positiveClaims lists a model's claims that state a guarantee, by field.
func positiveClaims(m ModelLimits) map[string]string {
	out := map[string]string{}
	if m.Availability == Supported {
		out[fAvail] = string(Supported)
	}
	switch m.Transaction {
	case TxAtomic, TxPartial, TxRollbackNotIsolated:
		out[fTx] = m.Transaction
	}
	switch m.Durability {
	case DurRestart, DurDocumented:
		out[fDur] = m.Durability
	}
	if m.AtomicWithSQL == Supported {
		out[fAtom] = string(Supported)
	}
	return out
}

// Review 1 MEDIUM-2: every positive Nucleus claim rests on a measurement of
// THAT property for THAT model, and every measured citation bears only on
// fields its measurement covered. A claim upgraded while citing unrelated
// evidence (say, a restart check for a transaction claim) fails here.
func TestEvidenceSupportsEachClaim(t *testing.T) {
	rep := BuildReport(Engine{Product: "nucleus", Version: Measured.NucleusVersion}, nil)
	for _, m := range rep.Models {
		for _, ev := range m.Evidence {
			if len(ev.Supports) == 0 {
				t.Errorf("nucleus/%s: %s %q supports no field", m.Model, ev.Source, ev.Ref)
			}
			if !ev.Measured() {
				continue
			}
			f, ok := measuredFacts[ev.Source+"|"+ev.Ref]
			if !ok {
				t.Errorf("nucleus/%s cites %s %q, which has no measured-fact entry", m.Model, ev.Source, ev.Ref)
				continue
			}
			fields, ok := f.models[m.Model]
			if !ok {
				t.Errorf("nucleus/%s cites %s %q, which did not measure this model", m.Model, ev.Source, ev.Ref)
				continue
			}
			for _, field := range ev.Supports {
				if _, ok := fields[field]; !ok {
					t.Errorf("nucleus/%s: %s %q cited for %s, which it did not measure", m.Model, ev.Source, ev.Ref, field)
				}
			}
		}
		for field, value := range positiveClaims(m) {
			if m.Model == "sql" && field == fAtom {
				continue // SQL is atomic with itself; checked below
			}
			backed := false
			for _, ev := range m.Evidence {
				if !ev.Measured() || !containsString(ev.Supports, field) {
					continue
				}
				if vals := measuredFacts[ev.Source+"|"+ev.Ref].models[m.Model][field]; containsString(vals, value) {
					backed = true
				}
			}
			if !backed {
				t.Errorf("nucleus/%s claims %s = %s with no measured evidence of that property", m.Model, field, value)
			}
		}
		if m.Model == "sql" && m.AtomicWithSQL == Supported {
			if _, ok := positiveClaims(m)[fTx]; !ok {
				t.Errorf("nucleus/sql: atomicWithSql supported but its transactions are %s", m.Transaction)
			}
		}
		// Review 1 MEDIUM-1: SIGKILL is stated only where a cited restart
		// run recorded it, and an unrecorded signal is said so.
		if m.Durability == DurRestart {
			sigkill := false
			for _, ev := range m.Evidence {
				if containsString(ev.Supports, fDur) && measuredFacts[ev.Source+"|"+ev.Ref].sigkill {
					sigkill = true
				}
			}
			claimsKill := strings.Contains(m.DurabilityNote, "SIGKILL") && !strings.Contains(m.DurabilityNote, "SIGKILL is not claimed")
			if claimsKill && !sigkill {
				t.Errorf("nucleus/%s durability note claims SIGKILL; its restart evidence recorded no signal: %q", m.Model, m.DurabilityNote)
			}
			if !sigkill && !strings.Contains(m.DurabilityNote, "signal was not recorded") {
				t.Errorf("nucleus/%s durability note must say the kill signal was not recorded: %q", m.Model, m.DurabilityNote)
			}
		}
	}
	// Every measured-fact row is a real citation target (no stale rows that
	// could later be cited without review).
	cited := map[string]bool{}
	for _, m := range rep.Models {
		for _, ev := range m.Evidence {
			cited[ev.Source+"|"+ev.Ref] = true
		}
	}
	for k := range measuredFacts {
		if !cited[k] {
			t.Errorf("measured fact %s is not cited by the registry; remove it or cite it", k)
		}
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
