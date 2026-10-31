package db

// This is a disposable, direct PostgreSQL rehearsal, not deployment discovery.
// Three actual compiled subprocesses attest their executable hash and perform
// independent reads/writes. Active-version sets remain controlled fixture inputs.
// Target-only writes make old readers stale; rollback is proved only BEFORE
// that cutover, and destructive contraction requires exact artifact confirmation.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type rolloutAppResult struct {
	Version        string `json:"version"`
	ArtifactSHA256 string `json:"artifactSha256"`
	Value          string `json:"value"`
}
type rolloutFixtureApp struct {
	identity RolloutApplication
	path     string
}

func rolloutRehearsalHash(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func rolloutBuildFixtureApp(t *testing.T, version, read, write string) rolloutFixtureApp {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "main.go")
	binary := filepath.Join(dir, "app")
	code := fmt.Sprintf(`package main
import("context";"crypto/sha256";"encoding/hex";"encoding/json"
 "os";"time";"github.com/jackc/pgx/v5")
func fail(){os.Stderr.WriteString("fixture application operation failed\n");os.Exit(1)}
func main(){ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();if len(os.Args)!=3{fail()};c,e:=pgx.Connect(ctx,os.Getenv("NEUTRON_ROLLOUT_APP_DATABASE_URL"));if e!=nil{fail()};defer c.Close(ctx);if os.Args[1]=="write"{_,e=c.Exec(ctx,%q,os.Args[2]);if e!=nil{fail()}}else if os.Args[1]!="read"{fail()};var value string;if e=c.QueryRow(ctx,%q).Scan(&value);e!=nil{fail()};path,e:=os.Executable();if e!=nil{fail()};bytes,e:=os.ReadFile(path);if e!=nil{fail()};sum:=sha256.Sum256(bytes);json.NewEncoder(os.Stdout).Encode(map[string]string{"version":%q,"artifactSha256":hex.EncodeToString(sum[:]),"value":value})}
`, write, read, version)
	if err := os.WriteFile(source, []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-p=1", "-o", binary, source)
	command.Dir = filepath.Join("..", "..")
	// Compile only during the root-scheduled native test; suppress potentially
	// environment-bearing tool output. No application endpoint is passed here.
	if err := command.Run(); err != nil {
		t.Fatal("compile fixture application failed", version)
	}
	b, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	return rolloutFixtureApp{RolloutApplication{version, rolloutRehearsalHash(b)}, binary}
}
func rolloutRunFixtureApp(t *testing.T, a rolloutFixtureApp, endpoint, operation, value string, expectFailure bool) rolloutAppResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, a.path, operation, value)
	command.Env = append(os.Environ(), "NEUTRON_ROLLOUT_APP_DATABASE_URL="+endpoint)
	raw, err := command.Output()
	if expectFailure {
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 || ctx.Err() != nil || len(raw) != 0 {
			t.Fatal("retired application did not report its expected SQL failure")
		}
		return rolloutAppResult{}
	}
	if err != nil {
		t.Fatal("fixture application failed", a.identity.Version)
	}
	var result rolloutAppResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal("invalid application attestation")
	}
	if result.Version != a.identity.Version || result.ArtifactSHA256 != a.identity.ArtifactSHA256 {
		t.Fatal("application identity mismatch")
	}
	return result
}
func TestRolloutNativeApplicationRehearsal(t *testing.T) {
	h := newQ07Harness(t, "rollout")
	ctx := context.Background()
	endpoint, err := url.Parse(os.Getenv("NEUTRON_E2E_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid native fixture endpoint")
	}
	endpoint.Path = "/" + h.dbName
	h.exec(`CREATE TABLE public.source(id bigint PRIMARY KEY,src text); INSERT INTO public.source VALUES(1,'initial'); CREATE TABLE public.progress(job_id text PRIMARY KEY,job_digest text NOT NULL,format text NOT NULL,chunks bigint NOT NULL,updated_rows bigint NOT NULL)`)
	apply := func(m *MigrationFile) {
		t.Helper()
		session, e := h.client.LockMigrations(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer session.Release()
		if e = session.EnsureMigrationTableV2(ctx); e != nil {
			t.Fatal(e)
		}
		if m != nil {
			if e = session.ApplyMigration(ctx, *m); e != nil {
				t.Fatal(e)
			}
		}
	}
	apply(nil)
	base, err := h.client.IntrospectV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	model, err := ModelFromRoot(base.Root)
	if err != nil {
		t.Fatal(err)
	}
	table := model.Table(V2Identity{Schema: "public", Name: "source"})
	if table == nil {
		t.Fatal("source missing from base document")
	}
	source := table.Column("src")
	if source == nil {
		t.Fatal("source column absent")
	}
	source.Name = "dst"
	root, err := RootFromModel(model)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	target, err := ParseV2Document(raw)
	if err != nil {
		t.Fatal(err)
	}
	old := rolloutBuildFixtureApp(t, "old-src-v1", `SELECT src FROM public.source WHERE id=1`, `UPDATE public.source SET src=$1 WHERE id=1`)
	dual := rolloutBuildFixtureApp(t, "dual-src-v2", `SELECT src FROM public.source WHERE id=1`, `UPDATE public.source SET src=$1,dst=$1 WHERE id=1`)
	final := rolloutBuildFixtureApp(t, "target-v3", `SELECT dst FROM public.source WHERE id=1`, `UPDATE public.source SET dst=$1 WHERE id=1`)
	expand := MigrationFile{Version: "001", Name: "expand", SQL: `ALTER TABLE public.source ADD COLUMN dst text;`}
	contract := MigrationFile{Version: "002", Name: "contract", SQL: `ALTER TABLE public.source DROP COLUMN src;`}
	executor, err := os.ReadFile("backfill.go")
	if err != nil {
		t.Fatal(err)
	}
	validationSQL := `SELECT count(*)::text FROM public.source WHERE dst IS DISTINCT FROM src`
	artifact := RolloutArtifact{RolloutVersion: 1, WorkflowID: "native-controlled-application-rehearsal", BaseSchemaSHA256: base.SHA256Hex, TargetSchemaSHA256: target.SHA256Hex, Phases: []RolloutPhase{
		{Kind: "expand", Applications: []RolloutApplication{old.identity}, Migrations: []RolloutMigration{{"001", MigrationChecksum(expand.SQL)}}},
		{Kind: "compatible-deploy", Applications: []RolloutApplication{old.identity, dual.identity}},
		{Kind: "backfill", Applications: []RolloutApplication{dual.identity}, Backfill: &RolloutBackfill{rolloutRehearsalHash(executor), 2, 3000}},
		{Kind: "validate", Applications: []RolloutApplication{dual.identity}, ValidationSHA256: rolloutRehearsalHash([]byte(validationSQL))},
		{Kind: "cutover", Applications: []RolloutApplication{final.identity}},
		{Kind: "contract", Applications: []RolloutApplication{final.identity}, Migrations: []RolloutMigration{{"002", MigrationChecksum(contract.SQL)}}, RetiredVersions: []string{old.identity.Version, dual.identity.Version}, Destructive: true},
	}}
	canonical, err := CanonicalRolloutArtifact(artifact)
	if err != nil {
		t.Fatal(err)
	}
	artifactHash := rolloutRehearsalHash(canonical)
	observation := RolloutObservation{BaseSchemaSHA256: base.SHA256Hex, MigrationHashes: map[string]string{"001": MigrationChecksum(expand.SQL), "002": MigrationChecksum(contract.SQL)}}
	authorize := func(index int) {
		t.Helper()
		observation.ActiveApplications = artifact.Phases[index].Applications
		next, e := PlanRolloutNext(artifact, observation)
		if e != nil || next.Phase == nil || next.Phase.Kind != rolloutKinds[index] {
			t.Fatalf("phase authorization %d: %v", index, e)
		}
	}
	complete := func(index int, evidence any) {
		t.Helper()
		b, e := json.Marshal(evidence)
		if e != nil {
			t.Fatal(e)
		}
		observation.Completed = append(observation.Completed, RolloutCompletedPhase{rolloutKinds[index], artifactHash, rolloutRehearsalHash(b)})
	}
	run := func(app rolloutFixtureApp, op, value string) rolloutAppResult {
		return rolloutRunFixtureApp(t, app, endpoint.String(), op, value, false)
	}
	before := run(old, "write", "before-expand")
	if before.Value != "before-expand" {
		t.Fatal("old write/read mismatch")
	}
	authorize(0)
	apply(&expand)
	complete(0, []any{before, h.queryOne(`SELECT src FROM public.source WHERE id=1`)})
	authorize(1)
	both := run(dual, "write", "dual-compatible")
	rollback := run(old, "read", "")
	if both.Value != rollback.Value {
		t.Fatal("pre-cutover old reader rollback incompatibility")
	}
	oldWrite := run(old, "write", "old-still-active")
	if oldWrite.Value != h.queryOne(`SELECT src FROM public.source WHERE id=1`) {
		t.Fatal("independent old writer mismatch")
	}
	complete(1, []rolloutAppResult{both, rollback, oldWrite})
	authorize(2)
	spec := backfillFixtureSpec()
	spec.WriterPolicySHA256 = dual.identity.ArtifactSHA256
	job, err := InspectBackfillJob(ctx, h.client, spec)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := RunBackfillChunk(ctx, h.client, job)
	if err != nil || chunk.UpdatedRows != 1 {
		t.Fatalf("reconcile prior old write: %+v %v", chunk, err)
	}
	dualAfter := run(dual, "write", "dual-after-backfill")
	complete(2, []any{chunk, dualAfter})
	authorize(3)
	snapshot, err := ValidateBackfill(ctx, h.client, job)
	if err != nil || !snapshot.SnapshotValidated || snapshot.Mismatches != 0 || h.queryOne(validationSQL) != "0" {
		t.Fatalf("independent validation: %+v %v", snapshot, err)
	}
	complete(3, snapshot)
	authorize(4)
	cutover := run(final, "write", "target-only")
	stale := run(old, "read", "")
	if cutover.Value == stale.Value {
		t.Fatal("fixture failed to demonstrate unsafe old-reader rollback after cutover")
	}
	complete(4, []rolloutAppResult{cutover, stale})
	observation.ActiveApplications = []RolloutApplication{final.identity}
	observation.DestructiveConfirmationSHA256 = ""
	if _, err := PlanRolloutNext(artifact, observation); err == nil {
		t.Fatal("destructive contract admitted without exact artifact confirmation")
	}
	observation.ActiveApplications = []RolloutApplication{old.identity, final.identity}
	observation.DestructiveConfirmationSHA256 = artifactHash
	if _, err := PlanRolloutNext(artifact, observation); err == nil {
		t.Fatal("contract admitted old application")
	}
	if h.queryOne(`SELECT src FROM public.source WHERE id=1`) != "dual-after-backfill" {
		t.Fatal("refused contract changed schema/data")
	}
	authorize(5)
	apply(&contract)
	after := run(final, "write", "contracted")
	if after.Value != "contracted" {
		t.Fatal("final application failed after contraction")
	}
	rolloutRunFixtureApp(t, old, endpoint.String(), "read", "", true)
	actual, err := h.client.IntrospectV2(ctx)
	if err != nil || actual.SHA256Hex != target.SHA256Hex {
		t.Fatal("actual contracted schema differs from exact target artifact")
	}
	session, err := h.client.LockMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	records, err := session.AppliedMigrations(ctx)
	session.Release()
	if err != nil || len(records) != 2 {
		t.Fatal("unexpected native migration history")
	}
	for i, record := range records {
		expected := []MigrationFile{expand, contract}[i]
		if record.Format != "v2" || record.Version != expected.Version || record.Checksum == nil || *record.Checksum != MigrationChecksum(expected.SQL) {
			t.Fatal("migration-v2 exact SQL digest mismatch")
		}
	}
	complete(5, []any{after, actual.SHA256Hex, records})
	done, err := PlanRolloutNext(artifact, observation)
	if err != nil || !done.Complete {
		t.Fatal("rehearsal completion rejected", err)
	}
	t.Logf("controlled native rehearsal artifact=%s applications=%s,%s,%s phases=%d; no live deployment discovery or post-cutover rollback claim", artifactHash, old.identity.ArtifactSHA256, dual.identity.ArtifactSHA256, final.identity.ArtifactSHA256, len(observation.Completed))
}
