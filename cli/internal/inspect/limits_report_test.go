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
			measured := 0
			for _, ev := range m.Evidence {
				if ev.Measured() {
					measured++
				}
			}
			// Any positive Nucleus claim needs measured evidence; prose can
			// only ever support a weaker statement.
			positive := m.Availability == Supported ||
				m.Transaction == TxRollbackNotIsolated || m.Transaction == TxPartial ||
				m.Durability == DurRestart || m.AtomicWithSQL == Supported
			if positive && measured == 0 {
				t.Errorf("nucleus/%s makes a positive claim with no measured evidence", m.Model)
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
