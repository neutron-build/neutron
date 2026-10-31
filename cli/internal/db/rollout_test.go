package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func rolloutFixture() RolloutArtifact {
	old := RolloutApplication{"old", strings.Repeat("a", 64)}
	fresh := RolloutApplication{"new", strings.Repeat("b", 64)}
	phases := make([]RolloutPhase, 6)
	for i, k := range rolloutKinds {
		phases[i] = RolloutPhase{Kind: k, Applications: []RolloutApplication{old, fresh}}
	}
	phases[0].Migrations = []RolloutMigration{{"001", strings.Repeat("c", 64)}}
	phases[2].Backfill = &RolloutBackfill{strings.Repeat("d", 64), 1000, 5000}
	phases[3].ValidationSHA256 = strings.Repeat("e", 64)
	phases[5].Applications = []RolloutApplication{fresh}
	phases[5].RetiredVersions = []string{"old"}
	phases[5].Destructive = true
	phases[5].Migrations = []RolloutMigration{{"002", strings.Repeat("f", 64)}}
	return RolloutArtifact{1, "unicode-\u00e9", strings.Repeat("1", 64), strings.Repeat("2", 64), phases}
}
func rolloutObserved(a RolloutArtifact, n int) RolloutObservation {
	raw, _ := CanonicalRolloutArtifact(a)
	sum := sha256.Sum256(raw)
	hash := hex.EncodeToString(sum[:])
	o := RolloutObservation{BaseSchemaSHA256: a.BaseSchemaSHA256, MigrationHashes: map[string]string{}, ActiveApplications: a.Phases[n%6].Applications}
	for _, p := range a.Phases {
		for _, m := range p.Migrations {
			o.MigrationHashes[m.ID] = m.UpSHA256
		}
	}
	for i := 0; i < n; i++ {
		o.Completed = append(o.Completed, RolloutCompletedPhase{a.Phases[i].Kind, hash, strings.Repeat("9", 64)})
	}
	o.DestructiveConfirmationSHA256 = hash
	return o
}
func TestRolloutNextPurePhaseSequence(t *testing.T) {
	a := rolloutFixture()
	for i, k := range rolloutKinds {
		next, err := PlanRolloutNext(a, rolloutObserved(a, i))
		if err != nil || next.Complete || next.Phase == nil || next.Phase.Kind != k {
			t.Fatalf("phase %d: %+v %v", i, next, err)
		}
	}
	next, err := PlanRolloutNext(a, rolloutObserved(a, 6))
	if err != nil || !next.Complete || next.Phase != nil {
		t.Fatalf("complete: %+v %v", next, err)
	}
	// Returned phase metadata is detached from caller-owned artifact data.
	next, _ = PlanRolloutNext(a, rolloutObserved(a, 0))
	next.Phase.Applications[0].Version = "tampered"
	if a.Phases[0].Applications[0].Version != "old" {
		t.Fatal("planner mutated input")
	}
}
func TestRolloutCanonicalBytesAndExactMigrationIDs(t *testing.T) {
	a := rolloutFixture()
	raw, err := CanonicalRolloutArtifact(a)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != "125eb3196d13f75b903bc98b1482c10b34dc0c334565f02597cac13e65c6e96f" {
		t.Fatal("version 1 canonical golden digest changed")
	}
	if bytes.HasSuffix(raw, []byte("\n")) || !bytes.Contains(raw, []byte(`"id":"001"`)) || !bytes.Contains(raw, []byte(`"workflowId":"unicode-é"`)) {
		t.Fatal("canonical string/ID/newline contract")
	}
	a.Phases[0].Applications[0], a.Phases[0].Applications[1] = a.Phases[0].Applications[1], a.Phases[0].Applications[0]
	equivalent, _ := CanonicalRolloutArtifact(a)
	if !bytes.Equal(raw, equivalent) {
		t.Fatal("application set order changes artifact identity")
	}
	o := rolloutObserved(a, 0)
	o.MigrationHashes["1"] = o.MigrationHashes["001"]
	delete(o.MigrationHashes, "001")
	if _, err := PlanRolloutNext(a, o); err == nil {
		t.Fatal("001 migration alias accepted as 1")
	}
}
func TestRolloutRejectsStaleUntrustedPreconditions(t *testing.T) {
	tests := map[string]func(*RolloutObservation){
		"base": func(o *RolloutObservation) { o.BaseSchemaSHA256 = strings.Repeat("0", 64) },
		"hash": func(o *RolloutObservation) { o.MigrationHashes["001"] = strings.Repeat("0", 64) },
		"app": func(o *RolloutObservation) {
			o.ActiveApplications = []RolloutApplication{{"new", strings.Repeat("0", 64)}}
		},
		"duplicate app": func(o *RolloutObservation) {
			o.ActiveApplications = append(o.ActiveApplications, o.ActiveApplications[0])
		},
		"out of order":       func(o *RolloutObservation) { o.Completed[0].Kind = "validate" },
		"stale artifact":     func(o *RolloutObservation) { o.Completed[0].ArtifactSHA256 = strings.Repeat("0", 64) },
		"missing evidence":   func(o *RolloutObservation) { o.Completed[0].EvidenceSHA256 = "" },
		"retired active":     func(o *RolloutObservation) { o.ActiveApplications = rolloutFixture().Phases[0].Applications },
		"stale confirmation": func(o *RolloutObservation) { o.DestructiveConfirmationSHA256 = strings.Repeat("0", 64) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			a := rolloutFixture()
			o := rolloutObserved(a, 5)
			mutate(&o)
			if _, err := PlanRolloutNext(a, o); err == nil {
				t.Fatal("invalid precondition accepted")
			}
		})
	}
	a := rolloutFixture()
	o := rolloutObserved(a, 5)
	a.WorkflowID = "changed"
	if _, err := PlanRolloutNext(a, o); err == nil {
		t.Fatal("confirmation/evidence replay across changed artifact accepted")
	}
}
func TestRolloutRejectsInvalidArtifacts(t *testing.T) {
	tests := map[string]func(*RolloutArtifact){
		"version":               func(a *RolloutArtifact) { a.RolloutVersion = 2 },
		"phase order":           func(a *RolloutArtifact) { a.Phases[0].Kind = "contract" },
		"duplicate migration":   func(a *RolloutArtifact) { a.Phases[5].Migrations = a.Phases[0].Migrations },
		"conflicting migration": func(a *RolloutArtifact) { a.Phases[5].Migrations[0].ID = "001" },
		"conflicting app":       func(a *RolloutArtifact) { a.Phases[1].Applications[0].ArtifactSHA256 = strings.Repeat("0", 64) },
		"duplicate app": func(a *RolloutArtifact) {
			a.Phases[0].Applications = append(a.Phases[0].Applications, a.Phases[0].Applications[0])
		},
		"invalid digest":     func(a *RolloutArtifact) { a.TargetSchemaSHA256 = strings.Repeat("A", 64) },
		"unbounded batch":    func(a *RolloutArtifact) { a.Phases[2].Backfill.MaxBatchRows = 1 << 62 },
		"unbounded time":     func(a *RolloutArtifact) { a.Phases[2].Backfill.MaxBatchMilliseconds = 1 << 62 },
		"missing validation": func(a *RolloutArtifact) { a.Phases[3].ValidationSHA256 = "" },
		"unknown retired":    func(a *RolloutArtifact) { a.Phases[5].RetiredVersions = []string{"unknown"} },
		"retired allowed":    func(a *RolloutArtifact) { a.Phases[5].RetiredVersions = []string{"new"} },
		"not destructive":    func(a *RolloutArtifact) { a.Phases[5].Destructive = false },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			a := rolloutFixture()
			mutate(&a)
			if _, err := CanonicalRolloutArtifact(a); err == nil {
				t.Fatal("invalid artifact accepted")
			}
		})
	}
	raw, _ := json.Marshal(rolloutFixture())
	for _, bad := range [][]byte{append(raw, raw...), []byte(strings.Replace(string(raw), `"rolloutVersion":1`, `"rolloutVersion":1,"rolloutVersion":1`, 1)), []byte(strings.Replace(string(raw), `"rolloutVersion":1`, `"rolloutVersion":1,"sql":"DROP TABLE x"`, 1)), []byte(strings.Replace(string(raw), `"maxBatchRows":1000`, `"maxBatchRows":9007199254740993`, 1)), []byte(strings.Replace(string(raw), `"maxBatchRows":1000`, `"maxBatchRows":1.1`, 1))} {
		if _, err := ParseRolloutArtifact(bad); err == nil {
			t.Fatal("ambiguous/unrepresentable input accepted")
		}
	}
	if _, err := ParseRolloutArtifact(raw); err != nil {
		t.Fatal(err)
	}
}
