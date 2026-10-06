package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/spf13/cobra"
)

func operatorTestFile(t *testing.T, name string, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func operatorTestSpec() db.BackfillSpec {
	return db.BackfillSpec{Version: 1, JobID: "001", Transformation: "copy-column-v1", Source: db.V2Identity{Schema: "public", Name: "source"}, Checkpoint: db.V2Identity{Schema: "public", Name: "progress"}, Key: "id", From: "src", To: "dst", WriterPolicySHA256: strings.Repeat("a", 64), BatchRows: 2, TimeoutMilliseconds: 1000}
}
func operatorTestCommand(group *cobra.Command, args ...string) (string, error) {
	root := &cobra.Command{Use: "test", SilenceErrors: true, SilenceUsage: true}
	root.AddCommand(group)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.ExecuteContext(context.Background())
	return out.String(), err
}
func TestBackfillOperatorRefusesBeforeConnecting(t *testing.T) {
	spec := operatorTestFile(t, "job.json", operatorTestSpec())
	cases := [][]string{
		{"inspect", "--spec", spec},
		{"inspect", "--spec", spec, "--profile", "transaction-pool"},
		{"chunk", "--spec", spec, "--profile", "postgres-direct"},
		{"chunk", "--spec", spec, "--profile", "postgres-direct", "--approve-job", "*"},
		{"inspect", "--spec", spec, "--profile", "postgres-direct", "--timeout", "0"},
		{"inspect", "--spec", spec, "--profile", "postgres-direct", "--timeout", "61s"},
		{"inspect", "--spec", spec, "--profile", "postgres-direct", "--timeout", "1ms"},
	}
	for _, arguments := range cases {
		args := append([]string{"backfill"}, arguments...)
		output, err := operatorTestCommand(newBackfillOperatorCommand(), args...)
		if err == nil || !strings.Contains(output, `"input_invalid"`) {
			t.Fatal("unsafe input reached connection stage", output, err)
		}
	}
	// Admission alone is pure, with exact identities and no configured endpoint.
	cmd, _, findErr := newBackfillOperatorCommand().Find([]string{"chunk"})
	if findErr != nil {
		t.Fatal(findErr)
	}
	if err := cmd.Flags().Parse([]string{"--spec", spec, "--profile", "postgres-direct", "--approve-job", strings.Repeat("b", 64)}); err != nil {
		t.Fatal(err)
	}
	parsed, approval, timeout, err := backfillOperatorInput(cmd, "chunk")
	if err != nil || parsed.JobID != "001" || approval != strings.Repeat("b", 64) || timeout != 30*time.Second {
		t.Fatal("admission mismatch", err)
	}
}
func TestOperatorStrictJSONAndSecretSafeNativeFailure(t *testing.T) {
	for _, raw := range []string{`{"observationVersion":1,"observationVersion":1}`, `{"observationVersion":1,"ObservationVersion":2}`, `{"activeApplications":[{"Version":"x"}]}`, `{"unknown":true}`, `{"observationVersion":1} {}`, strings.Repeat("[", 130) + strings.Repeat("]", 130)} {
		var observed rolloutOperatorObservation
		if operatorStrictJSON([]byte(raw), &observed) == nil {
			t.Fatal("malformed artifact admitted")
		}
	}
	var output bytes.Buffer
	command := &cobra.Command{}
	command.SetOut(&output)
	secret := "password=must-never-print"
	err := backfillOperatorDatabaseError(command, &db.BackfillError{Indeterminate: true, Cause: &pgconn.PgError{Code: "08006", Message: secret, Detail: secret}})
	if err == nil || !strings.Contains(output.String(), `"indeterminate"`) || strings.Contains(output.String(), secret) || strings.Contains(err.Error(), secret) {
		t.Fatal("native error escaped generic JSON boundary")
	}
	output.Reset()
	err = backfillOperatorDatabaseError(command, errors.New(secret))
	if err == nil || strings.Contains(output.String(), secret) {
		t.Fatal("generic native diagnostics escaped")
	}
}
func TestRolloutOperatorPurePlan(t *testing.T) {
	hash := strings.Repeat("a", 64)
	old := db.RolloutApplication{Version: "old", ArtifactSHA256: hash}
	next := db.RolloutApplication{Version: "new", ArtifactSHA256: strings.Repeat("b", 64)}
	artifact := db.RolloutArtifact{RolloutVersion: 1, WorkflowID: "operator-plan", BaseSchemaSHA256: hash, TargetSchemaSHA256: hash, Phases: []db.RolloutPhase{
		{Kind: "expand", Applications: []db.RolloutApplication{old}, Migrations: []db.RolloutMigration{{ID: "001", UpSHA256: hash}}},
		{Kind: "compatible-deploy", Applications: []db.RolloutApplication{old, next}},
		{Kind: "backfill", Applications: []db.RolloutApplication{next}, Backfill: &db.RolloutBackfill{ImplementationSHA256: hash, MaxBatchRows: 2, MaxBatchMilliseconds: 1000}},
		{Kind: "validate", Applications: []db.RolloutApplication{next}, ValidationSHA256: hash},
		{Kind: "cutover", Applications: []db.RolloutApplication{next}},
		{Kind: "contract", Applications: []db.RolloutApplication{next}, Migrations: []db.RolloutMigration{{ID: "002", UpSHA256: hash}}, RetiredVersions: []string{"old"}, Destructive: true},
	}}
	artifactPath := operatorTestFile(t, "rollout.json", artifact)
	observed := rolloutOperatorObservation{ObservationVersion: 1, BaseSchemaSHA256: hash, MigrationHashes: map[string]string{"001": hash, "002": hash}, ActiveApplications: []db.RolloutApplication{old}}
	observationPath := operatorTestFile(t, "observed.json", observed)
	output, err := operatorTestCommand(newRolloutOperatorCommand(), "rollout", "plan", "--artifact", artifactPath, "--observation", observationPath)
	if err != nil || !strings.Contains(output, `"status":"planned"`) || !strings.Contains(output, `"effects":false`) || !strings.Contains(output, `"kind":"expand"`) || !strings.Contains(output, "operator assertions") {
		t.Fatal("pure plan output differs", output, err)
	}
	observed.MigrationHashes["001"] = strings.Repeat("c", 64)
	observationPath = operatorTestFile(t, "stale.json", observed)
	output, err = operatorTestCommand(newRolloutOperatorCommand(), "rollout", "plan", "--artifact", artifactPath, "--observation", observationPath)
	if err == nil || !strings.Contains(output, `"preconditions_differ"`) {
		t.Fatal("stale migration hash admitted", output, err)
	}
}
