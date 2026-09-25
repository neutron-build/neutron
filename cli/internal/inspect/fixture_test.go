package inspect

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// studioFixture is the exact limits JSON the server sends, checked into
// Studio so its UI tests render real server shapes. Regenerate with
// NEUTRON_UPDATE_FIXTURES=1 go test ./internal/inspect -run StudioLimitsFixture.
const studioFixture = "studio/src/lib/limits.fixture.json"

func TestStudioLimitsFixtureInSync(t *testing.T) {
	want := map[string]Report{
		"postgres":          BuildReport(Engine{Product: "postgres", Version: "17.11", Raw: "PostgreSQL 17.11"}, &LiveSettings{Fsync: "on", SynchronousCommit: "on", DefaultIsolation: "read committed", DefaultReadOnly: "off"}),
		"nucleus":           BuildReport(Engine{Product: "nucleus", Version: Measured.NucleusVersion, Raw: "PostgreSQL 16.0 (Nucleus " + Measured.NucleusVersion + " — The Definitive Database)"}, nil),
		"nucleusUnmeasured": BuildReport(Engine{Product: "nucleus", Version: "9.9.9", Raw: "PostgreSQL 16.0 (Nucleus 9.9.9 — The Definitive Database)"}, nil),
	}
	b, err := json.MarshalIndent(want, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	path := filepath.Join(repoRoot, studioFixture)
	if os.Getenv("NEUTRON_UPDATE_FIXTURES") == "1" {
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v (regenerate with NEUTRON_UPDATE_FIXTURES=1)", studioFixture, err)
	}
	if !bytes.Equal(got, b) {
		t.Fatalf("%s is stale: the registry changed; regenerate with NEUTRON_UPDATE_FIXTURES=1 and review the diff", studioFixture)
	}
}
