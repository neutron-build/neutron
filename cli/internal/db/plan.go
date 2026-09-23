package db

// Snapshot-based planning artifacts and chain management (M03).
//
// Offline `migrate generate` plans from the LAST ACCEPTED SNAPSHOT to the
// desired schema document — no database connection, no history writes, no
// locks (generation is offline by contract; the serialized runner owns
// apply-time behavior). Every generated migration records its planning base
// and target as canonical document hashes, plus a target snapshot, so the
// next generation plans against the state after all pending migrations.
//
// Artifact set per generated migration (written all-or-nothing):
//
//	migrations/{version}_{name}.up.sql        runnable by `neutron migrate`
//	migrations/{version}_{name}.down.sql
//	migrations/{version}_{name}.plan.json     structured plan + risk report
//	migrations/snapshots/{version}_{name}.snapshot.json
//
// `neutron schema baseline` writes migrations/snapshots/000_baseline.snapshot.json
// as the chain root for an existing database (read-only inspection; never a
// reset, never history adoption).
//
// Determinism is load-bearing: equal inputs (same desired document + same
// chain + same flags) produce byte-identical artifacts — no timestamps, no
// absolute paths, no environment-dependent bytes. Branch conflicts (two
// successors of one snapshot, duplicate or numerically-colliding versions,
// snapshots without their migration) fail loudly instead of overwriting.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// PlanFormatVersion is the artifact format implemented by this file.
const PlanFormatVersion = 1

// SnapshotWorkflowTag marks snapshot/plan artifacts written by this workflow.
const SnapshotWorkflowTag = "snapshot-v1"

// SnapshotDir is the per-project snapshot directory under the migrations dir.
const SnapshotDir = "snapshots"

// BaselineVersion is the reserved version text of the baseline snapshot.
const BaselineVersion = "000"

// BaselineName is the reserved name of the baseline snapshot.
const BaselineName = "baseline"

// reversibility vocabulary for plan operations.
const (
	ReversibilityReversible   = "reversible"   // a real down statement restores structure
	ReversibilityIrreversible = "irreversible" // destructive/lossy with no restoring down
	ReversibilityManual       = "manual"       // non-destructive, but no down recorded
)

// BaselineHistory records what migration history the baseline observed.
// It is a REPORT: baselining never adopts, writes or upgrades history.
type BaselineHistory struct {
	Shape           string   `json:"shape"`
	AppliedVersions []string `json:"appliedVersions"`
	Note            string   `json:"note,omitempty"`
}

// SnapshotArtifact is the on-disk snapshot: chain metadata plus the target
// schema document in canonical form (embedded verbatim, byte-stable).
type SnapshotArtifact struct {
	FormatVersion int              `json:"formatVersion"`
	Kind          string           `json:"kind"` // "baseline" | "migration"
	Version       string           `json:"version"`
	Name          string           `json:"name"`
	BaseSHA256    string           `json:"baseSha256,omitempty"` // empty on the baseline root
	TargetSHA256  string           `json:"targetSha256"`
	Covers        []string         `json:"covers,omitempty"` // baseline only: pre-workflow migration versions
	History       *BaselineHistory `json:"history,omitempty"`
	Document      json.RawMessage  `json:"document"`
}

// Stem is the migration filename stem ("001_add_users").
func (s *SnapshotArtifact) Stem() string { return s.Version + "_" + s.Name }

// PlanOperation is one planned statement with its risk classification.
type PlanOperation struct {
	Index         int    `json:"index"`
	SQL           string `json:"sql"`
	Down          string `json:"down"`
	Destructive   bool   `json:"destructive"`
	DataLoss      bool   `json:"dataLoss"`
	Reversibility string `json:"reversibility"`
}

// PlanRisk summarizes the operation classifications.
type PlanRisk struct {
	HasDestructive       bool   `json:"hasDestructive"`
	HasDataLoss          bool   `json:"hasDataLoss"`
	StatementCount       int    `json:"statementCount"`
	IrreversibleCount    int    `json:"irreversibleCount"`
	OverallReversibility string `json:"overallReversibility"`

	manual bool
}

// PlanArtifact is the structured plan recorded next to each generated
// migration: base/target identity, operations, risk report and caveats.
type PlanArtifact struct {
	FormatVersion    int             `json:"formatVersion"`
	Workflow         string          `json:"workflow"`
	Mode             string          `json:"mode"`
	MigrationVersion string          `json:"migrationVersion"`
	MigrationName    string          `json:"migrationName"`
	BaseSource       string          `json:"baseSource"`
	BaseSHA256       string          `json:"baseSha256"`
	TargetSHA256     string          `json:"targetSha256"`
	Renames          []string        `json:"renames"`
	Capabilities     []string        `json:"capabilities"`
	TransactionMode  string          `json:"transactionMode"`
	Operations       []PlanOperation `json:"operations"`
	Risk             PlanRisk        `json:"risk"`
	Caveats          []string        `json:"caveats"`
}

// ---------------------------------------------------------------------------
// Empty root document
// ---------------------------------------------------------------------------

// EmptyV2Document returns the canonical empty managed document (no schemas,
// no objects): the implicit planning base of a fresh snapshot chain.
func EmptyV2Document() (*V2Document, error) {
	root, err := RootFromModel(V2DocumentModel{
		Version:      SchemaDocumentVersionV2,
		Dialect:      "postgresql",
		Capabilities: []string{},
		Schemas:      []V2SchemaDecl{},
		Tables:       []V2Table{},
		Enums:        []V2EnumDecl{},
		Views:        []V2View{},
		Opaque:       []V2Opaque{},
	})
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	return ParseV2Document(raw)
}

var emptyDocHashOnce struct {
	called bool
	hash   string
	err    error
}

// EmptyDocumentSHA256 is the canonical hash of the empty root document.
func EmptyDocumentSHA256() (string, error) {
	if !emptyDocHashOnce.called {
		emptyDocHashOnce.called = true
		doc, err := EmptyV2Document()
		if err != nil {
			emptyDocHashOnce.err = err
		} else {
			emptyDocHashOnce.hash = doc.SHA256Hex
		}
	}
	return emptyDocHashOnce.hash, emptyDocHashOnce.err
}

// ---------------------------------------------------------------------------
// Chain loading and validation
// ---------------------------------------------------------------------------

// SnapshotChain is the validated snapshot history of a migrations directory.
type SnapshotChain struct {
	Baseline  *SnapshotArtifact
	Snapshots []SnapshotArtifact // migration snapshots, version-ascending

	Covers     map[string]bool // versions covered by the baseline
	RootSHA256 string          // baseline target hash, or the empty-document hash
	HeadSHA256 string          // newest snapshot target hash, or root
	HeadRef    string          // "empty" | "000_baseline" | migration stem
}

// Empty reports whether the chain has no baseline and no snapshots.
func (c *SnapshotChain) Empty() bool {
	return c.Baseline == nil && len(c.Snapshots) == 0
}

// HeadDocument parses and validates the head snapshot's document (or the
// empty root document for a fresh chain).
func (c *SnapshotChain) HeadDocument() (*V2Document, error) {
	if c.Baseline == nil && len(c.Snapshots) == 0 {
		return EmptyV2Document()
	}
	var raw []byte
	if len(c.Snapshots) > 0 {
		raw = c.Snapshots[len(c.Snapshots)-1].Document
	} else {
		raw = c.Baseline.Document
	}
	return ParseV2Document(raw)
}

// SnapshotForVersion returns the migration snapshot with the exact version
// text, or nil.
func (c *SnapshotChain) SnapshotForVersion(version string) *SnapshotArtifact {
	for i := range c.Snapshots {
		if c.Snapshots[i].Version == version {
			return &c.Snapshots[i]
		}
	}
	return nil
}

// LoadSnapshotChain reads and validates the snapshot chain of a migrations
// directory. A missing snapshots directory is a valid empty chain (fresh
// project). Every rule below fails loudly: silent chain repair would let
// divergent branches overwrite each other's state.
func LoadSnapshotChain(migrationsDir string) (*SnapshotChain, error) {
	chain := &SnapshotChain{Covers: map[string]bool{}}
	rootHash, err := EmptyDocumentSHA256()
	if err != nil {
		return nil, err
	}
	chain.RootSHA256 = rootHash
	chain.HeadSHA256 = rootHash
	chain.HeadRef = "empty"

	snapDir := filepath.Join(migrationsDir, SnapshotDir)
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			// A missing snapshots directory is a valid empty chain — but
			// NOT a valid excuse to skip the coverage check below: a
			// migrations directory full of .up.sql files with no chain at
			// all must fail, not plan from empty.
			entries = nil
		} else {
			return nil, fmt.Errorf("read snapshot directory %s: %w", snapDir, err)
		}
	}

	var snapshots []SnapshotArtifact
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".snapshot.json") {
			continue
		}
		path := filepath.Join(snapDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read snapshot %s: %w", path, err)
		}
		var snap SnapshotArtifact
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&snap); err != nil {
			return nil, fmt.Errorf("invalid snapshot %s: %w", path, err)
		}
		// The file's stem and the content's recorded identity must agree:
		// version allocation reads filenames, chain identity reads content,
		// and a divergence between the two is an ambiguous ID.
		fileStem := strings.TrimSuffix(e.Name(), ".snapshot.json")
		if fileStem != snap.Stem() {
			return nil, fmt.Errorf("snapshot file %s records identity %q — filename/content identity collision (renamed or edited artifact); the chain refuses ambiguous IDs", path, snap.Stem())
		}
		if snap.FormatVersion != PlanFormatVersion {
			return nil, fmt.Errorf("snapshot %s declares format version %d; this CLI understands version %d only", path, snap.FormatVersion, PlanFormatVersion)
		}
		switch snap.Kind {
		case "baseline":
			if chain.Baseline != nil {
				return nil, fmt.Errorf("two baseline snapshots: a chain has exactly one root; delete the stale one explicitly")
			}
			if snap.Version != BaselineVersion || snap.Name != BaselineName {
				return nil, fmt.Errorf("baseline snapshot %s must use version %q and name %q", path, BaselineVersion, BaselineName)
			}
			chain.Baseline = &snap
		case "migration":
			snapshots = append(snapshots, snap)
		default:
			return nil, fmt.Errorf("snapshot %s declares unknown kind %q", path, snap.Kind)
		}
	}

	// Deterministic order: numerically by version, stem as tie-break.
	sort.Slice(snapshots, func(i, j int) bool {
		if c := CompareVersions(snapshots[i].Version, snapshots[j].Version); c != 0 {
			return c < 0
		}
		return snapshots[i].Stem() < snapshots[j].Stem()
	})

	// Divergence: one base hash may have at most one successor. Two
	// snapshots following the same parent with DIFFERENT outcomes are
	// divergent branches merged into one directory — refuse, never pick a
	// winner. An identical base AND target is a copied artifact; the
	// duplicate-version and continuity checks below report those.
	successorStem := map[string]string{}   // base hash -> first successor stem
	successorTarget := map[string]string{} // base hash -> first successor target
	for i := range snapshots {
		s := &snapshots[i]
		if prev, dup := successorStem[s.BaseSHA256]; dup {
			if successorTarget[s.BaseSHA256] == s.TargetSHA256 {
				continue // same parent, same outcome: a copy, not a divergence
			}
			return nil, fmt.Errorf("divergent snapshot chain: %s and %s both plan from the same base state (base sha256 %s) — reconcile the branches by hand; generation refuses to pick a winner", prev, s.Stem(), shortHash(s.BaseSHA256))
		}
		successorStem[s.BaseSHA256] = s.Stem()
		successorTarget[s.BaseSHA256] = s.TargetSHA256
	}

	// Duplicate and colliding versions are hard errors (identity is text;
	// numerically-equal-but-distinct spellings can never be reconciled —
	// the same rule as migration history §1 of MIGRATIONS.md).
	byVersion := map[string]string{}
	for i := range snapshots {
		s := &snapshots[i]
		if prev, dup := byVersion[s.Version]; dup {
			return nil, fmt.Errorf("snapshot version %q is claimed by two snapshots (%s and %s): duplicate migration IDs are a reconciliation error, never auto-resolved", s.Version, prev, s.Stem())
		}
		byVersion[s.Version] = s.Stem()
	}
	for i := 1; i < len(snapshots); i++ {
		prev, cur := &snapshots[i-1], &snapshots[i]
		if prev.Version != cur.Version &&
			parseable(prev.Version) && parseable(cur.Version) {
			a, _ := strconv.ParseInt(prev.Version, 10, 64)
			b, _ := strconv.ParseInt(cur.Version, 10, 64)
			if a == b {
				return nil, fmt.Errorf("snapshots %s and %s carry numerically-equal but distinct versions: collision, never auto-resolved", prev.Stem(), cur.Stem())
			}
		}
	}

	// Chain continuity + document integrity.
	expectedBase := chain.RootSHA256
	if chain.Baseline != nil {
		doc, err := ParseV2Document(chain.Baseline.Document)
		if err != nil {
			return nil, fmt.Errorf("baseline snapshot document is invalid: %w", err)
		}
		if doc.SHA256Hex != chain.Baseline.TargetSHA256 {
			return nil, fmt.Errorf("baseline snapshot records target sha256 %s but its document hashes to %s — the artifact is corrupt", shortHash(chain.Baseline.TargetSHA256), shortHash(doc.SHA256Hex))
		}
		chain.RootSHA256 = chain.Baseline.TargetSHA256
		chain.HeadSHA256 = chain.Baseline.TargetSHA256
		chain.HeadRef = BaselineVersion + "_" + BaselineName
		expectedBase = chain.Baseline.TargetSHA256
		for _, v := range chain.Baseline.Covers {
			chain.Covers[v] = true
		}
	}
	for i := range snapshots {
		s := &snapshots[i]
		doc, err := ParseV2Document(s.Document)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s document is invalid: %w", s.Stem(), err)
		}
		if doc.SHA256Hex != s.TargetSHA256 {
			return nil, fmt.Errorf("snapshot %s records target sha256 %s but its document hashes to %s — the artifact is corrupt", s.Stem(), shortHash(s.TargetSHA256), shortHash(doc.SHA256Hex))
		}
		if s.BaseSHA256 != expectedBase {
			return nil, fmt.Errorf("snapshot %s plans from base sha256 %s, but the chain's previous state is %s — the snapshot chain has a gap or was reordered; re-baseline if this directory was partially merged", s.Stem(), shortHash(s.BaseSHA256), shortHash(expectedBase))
		}
		expectedBase = s.TargetSHA256
		chain.HeadSHA256 = s.TargetSHA256
		chain.HeadRef = s.Stem()
	}
	chain.Snapshots = snapshots

	// File-side identity: duplicate version texts among .up.sql files are
	// a reconciliation error, mirroring the snapshot-side rule above. This
	// is the ordinary residue of a hand merge that kept both branches' sql
	// files — the runner would apply both under one version, so the
	// directory is ambiguous regardless of what the snapshots say.
	upStems := readMigrationStems(migrationsDir)
	fileByVersion := map[string]string{}
	for _, stem := range upStems {
		v := stemVersion(stem)
		if prev, dup := fileByVersion[v]; dup {
			a, b := prev, stem
			if b < a {
				a, b = b, a
			}
			return nil, fmt.Errorf("migration version %q is claimed by two migration files (%s.up.sql and %s.up.sql): duplicate migration IDs are a reconciliation error, never auto-resolved", v, a, b)
		}
		fileByVersion[v] = stem
	}

	// Coverage is STEM-keyed, not version-keyed: identity is
	// "version_name", and a same-version different-name snapshot never
	// covers a migration file. Version-keyed coverage let a merge that
	// kept one branch's snapshot and both branches' sql files validate
	// cleanly while planning silently ignored the un-snapshotted pending
	// migration (review-1 MAJOR-1). With a baseline, pre-baseline
	// migration files remain covered by version text (the baseline's
	// covers list records versions).
	snapByStem := map[string]bool{}
	snapByVersion := map[string]string{}
	for _, s := range snapshots {
		snapByStem[s.Stem()] = true
		snapByVersion[s.Version] = s.Stem()
	}
	for _, stem := range upStems {
		if snapByStem[stem] || chain.Covers[stemVersion(stem)] {
			continue
		}
		if other, ok := snapByVersion[stemVersion(stem)]; ok {
			return nil, fmt.Errorf("migration file %s.up.sql has no snapshot of its own — snapshot %s shares version %q but a same-version snapshot covers only its own migration (merge residue or duplicate ID); reconcile the directory by hand", stem, other, stemVersion(stem))
		}
		return nil, fmt.Errorf("migration %s has a .up.sql file but no snapshot — the snapshot chain is incomplete (hand-authored migrations need `neutron schema baseline` at the current applied state, or generate them with `neutron migrate generate --mode snapshot`)", stem)
	}
	upStemSet := map[string]bool{}
	for _, stem := range upStems {
		upStemSet[stem] = true
	}
	for _, s := range snapshots {
		if !upStemSet[s.Stem()] {
			return nil, fmt.Errorf("snapshot %s has no matching .up.sql file — a snapshot without its migration cannot anchor the chain", s.Stem())
		}
	}

	return chain, nil
}

func parseable(v string) bool {
	_, err := strconv.ParseInt(v, 10, 64)
	return err == nil
}

// readMigrationStems lists the filename stems ("001_add_users") of .up.sql
// files, version-ascending. Unlike bare versions, stems are NOT collapsed:
// duplicate version texts among files are a reconciliation error detected
// by LoadSnapshotChain.
func readMigrationStems(migrationsDir string) []string {
	files, err := ReadMigrationFiles(migrationsDir)
	if err != nil {
		return nil // missing directory: no files
	}
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, f.Version+"_"+f.Name)
	}
	return out
}

// stemVersion returns the version text of a migration filename stem.
func stemVersion(stem string) string {
	if i := strings.IndexByte(stem, '_'); i >= 0 {
		return stem[:i]
	}
	return stem
}

// ---------------------------------------------------------------------------
// Version allocation
// ---------------------------------------------------------------------------

// NextMigrationVersion allocates the next numeric version: one past the
// highest numeric version among migration files and migration snapshots.
func NextMigrationVersion(migrationsDir string) (string, error) {
	highest := 0
	for _, stem := range readMigrationStems(migrationsDir) {
		if n, err := strconv.Atoi(stemVersion(stem)); err == nil && n > highest {
			highest = n
		}
	}
	snapDir := filepath.Join(migrationsDir, SnapshotDir)
	if entries, err := os.ReadDir(snapDir); err == nil {
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".snapshot.json") {
				continue
			}
			stem := strings.TrimSuffix(e.Name(), ".snapshot.json")
			v := stem
			if i := strings.IndexByte(stem, '_'); i >= 0 {
				v = stem[:i]
			}
			if n, err := strconv.Atoi(v); err == nil && n > highest {
				highest = n
			}
		}
	}
	return fmt.Sprintf("%03d", highest+1), nil
}

// ---------------------------------------------------------------------------
// Plan construction and risk classification
// ---------------------------------------------------------------------------

// BuildPlanArtifact classifies a diff result into the structured plan. The
// up/down lists must be index-paired (the v2 planner emits them in lockstep);
// unpaired output is a planner defect and fails closed rather than mispairing.
func BuildPlanArtifact(version, name, baseSource, baseSHA string, target *V2Document, renames map[string]string, res DiffResult) (*PlanArtifact, error) {
	if len(res.Up) != len(res.Down) {
		return nil, fmt.Errorf("diff produced %d up statements but %d down statements — refusing to pair them; this is a planner defect", len(res.Up), len(res.Down))
	}
	plan := &PlanArtifact{
		FormatVersion:    PlanFormatVersion,
		Workflow:         SnapshotWorkflowTag,
		Mode:             "snapshot",
		MigrationVersion: version,
		MigrationName:    name,
		BaseSource:       baseSource,
		BaseSHA256:       baseSHA,
		TargetSHA256:     target.SHA256Hex,
		Renames:          renameDisplayList(renames),
		Capabilities:     append([]string(nil), documentCapabilities(target)...),
		TransactionMode:  "single",
		Caveats:          append([]string(nil), res.Warnings...),
	}
	for i, up := range res.Up {
		op := PlanOperation{
			Index:         i + 1,
			SQL:           up,
			Down:          res.Down[i],
			Destructive:   statementDestructive(up),
			DataLoss:      statementDataLoss(up),
			Reversibility: ReversibilityReversible,
		}
		op.Reversibility = classifyReversibility(op.Destructive, op.DataLoss, op.Down)
		plan.Operations = append(plan.Operations, op)
	}
	plan.Risk = summarizeRisk(plan.Operations)
	return plan, nil
}

// classifyReversibility grades one operation from its flags and its down
// statement: a real down statement restores structure (reversible); a
// missing or comment-only down on a destructive/lossy operation can restore
// nothing (irreversible); a missing down on a benign operation is manual.
// Down SQL is never data restoration — the dataLoss flag carries that truth.
func classifyReversibility(destructive, dataLoss bool, down string) string {
	d := strings.TrimSpace(down)
	if d == "" || strings.HasPrefix(d, "--") {
		if destructive || dataLoss {
			return ReversibilityIrreversible
		}
		return ReversibilityManual
	}
	return ReversibilityReversible
}

func summarizeRisk(ops []PlanOperation) PlanRisk {
	risk := PlanRisk{StatementCount: len(ops), OverallReversibility: ReversibilityReversible}
	for _, op := range ops {
		reversibility := op.Reversibility
		if reversibility == "" {
			reversibility = classifyReversibility(op.Destructive, op.DataLoss, op.Down)
		}
		risk.HasDestructive = risk.HasDestructive || op.Destructive
		risk.HasDataLoss = risk.HasDataLoss || op.DataLoss
		if reversibility == ReversibilityIrreversible {
			risk.IrreversibleCount++
		}
		if reversibility == ReversibilityManual {
			risk.manual = true
		}
	}
	switch {
	case risk.IrreversibleCount > 0:
		risk.OverallReversibility = ReversibilityIrreversible
	case risk.StatementCount > 0 && risk.manual:
		risk.OverallReversibility = ReversibilityManual
	}
	return risk
}

// statementDestructive classifies statements this package's own planner
// emits, by their deterministic leading tokens. It is not a SQL parser and
// never executes anything.
func statementDestructive(sql string) bool {
	s := normalizeSQL(sql)
	for _, prefix := range []string{"drop table", "drop view", "drop index", "drop type", "drop schema"} {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	if strings.HasPrefix(s, "alter table") {
		return strings.Contains(s, " drop column ") || strings.Contains(s, " drop constraint ")
	}
	return false
}

// statementDataLoss marks operations that can destroy row data or column
// values. Constraint/index/view drops destroy structure, not data; column
// type conversions can truncate, so they carry the flag too.
func statementDataLoss(sql string) bool {
	s := normalizeSQL(sql)
	for _, prefix := range []string{"drop table", "drop type", "drop schema"} {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	if strings.HasPrefix(s, "alter table") {
		if strings.Contains(s, " drop column ") {
			return true
		}
		if strings.Contains(s, " alter column ") && strings.Contains(s, " type ") {
			return true
		}
	}
	return false
}

func normalizeSQL(sql string) string {
	s := strings.TrimSpace(strings.ToLower(sql))
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	return strings.TrimSpace(s)
}

func renameDisplayList(renames map[string]string) []string {
	if len(renames) == 0 {
		return nil
	}
	out := make([]string, 0, len(renames))
	for target, source := range renames {
		// target is "schema.table.newcolumn"; render "schema.table.old>schema.table.new"
		parts := strings.Split(target, ".")
		if len(parts) != 3 {
			out = append(out, source+">"+target)
			continue
		}
		table := parts[0] + "." + parts[1]
		out = append(out, table+"."+source+">"+target)
	}
	sort.Strings(out)
	return out
}

func documentCapabilities(doc *V2Document) []string {
	m, err := ModelFromRoot(doc.Root)
	if err != nil {
		return nil
	}
	return m.Capabilities
}

func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

// ---------------------------------------------------------------------------
// Atomic artifact writes
// ---------------------------------------------------------------------------

// ArtifactFile is one file of an all-or-nothing artifact set.
type ArtifactFile struct {
	Path    string
	Content []byte
}

// WriteArtifactSet writes every file or none of them: targets must not
// exist (branch conflicts and re-runs fail instead of overwriting), each
// body is staged in a temp file next to its target and hard-linked into
// place, and any failure removes everything this call created or staged.
func WriteArtifactSet(files []ArtifactFile) error {
	if len(files) == 0 {
		return nil
	}

	// Pre-flight: no target may exist. Failures here leave nothing behind.
	for _, f := range files {
		if _, err := os.Stat(f.Path); err == nil {
			return fmt.Errorf("refusing to overwrite existing file %s — migration artifacts are never replaced; if this is a re-run, the next version was expected", f.Path)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("stat %s: %w", f.Path, err)
		}
	}

	// Stage every body first: source/content failures must not leave any
	// half-written artifact.
	type staged struct {
		tmp, dst string
	}
	var stagedFiles []staged
	cleanupStaged := func() {
		for _, s := range stagedFiles {
			os.Remove(s.tmp)
		}
	}
	for _, f := range files {
		dir := filepath.Dir(f.Path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			cleanupStaged()
			return fmt.Errorf("create directory %s: %w", dir, err)
		}
		tmp, err := os.CreateTemp(dir, ".neutron-artifact-*")
		if err != nil {
			cleanupStaged()
			return fmt.Errorf("stage %s: %w", f.Path, err)
		}
		if _, err := tmp.Write(f.Content); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			cleanupStaged()
			return fmt.Errorf("write %s: %w", f.Path, err)
		}
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			cleanupStaged()
			return fmt.Errorf("sync %s: %w", f.Path, err)
		}
		if err := tmp.Close(); err != nil {
			os.Remove(tmp.Name())
			cleanupStaged()
			return fmt.Errorf("close %s: %w", f.Path, err)
		}
		if err := os.Chmod(tmp.Name(), 0o644); err != nil {
			os.Remove(tmp.Name())
			cleanupStaged()
			return fmt.Errorf("chmod %s: %w", f.Path, err)
		}
		stagedFiles = append(stagedFiles, staged{tmp: tmp.Name(), dst: f.Path})
	}

	// Link into place. os.Link fails with EEXIST if the target appeared
	// since the pre-flight (atomic create-if-absent on POSIX).
	var linked []string
	for _, s := range stagedFiles {
		if err := os.Link(s.tmp, s.dst); err != nil {
			for _, d := range linked {
				os.Remove(d)
			}
			cleanupStaged()
			if os.IsExist(err) {
				return fmt.Errorf("refusing to overwrite existing file %s — migration artifacts are never replaced", s.dst)
			}
			return fmt.Errorf("publish %s: %w", s.dst, err)
		}
		linked = append(linked, s.dst)
	}
	for _, s := range stagedFiles {
		os.Remove(s.tmp)
	}
	return nil
}

// WriteAtomicReplace writes a single generated output file atomically
// (temp + rename): export/pull outputs are regenerated artifacts, so
// replacing the previous output is expected.
func WriteAtomicReplace(path string, content []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create directory %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".neutron-out-*")
	if err != nil {
		return fmt.Errorf("stage %s: %w", path, err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return os.Rename(tmp.Name(), path)
}

// marshalDeterministic renders an artifact as fixed struct order, two-space
// indent, trailing newline, and NO HTML escaping: rename records carry `>`
// literally and stay readable in review (bytes are still deterministic).
func marshalDeterministic(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalPlanJSON renders a plan artifact deterministically.
func MarshalPlanJSON(plan *PlanArtifact) ([]byte, error) {
	return marshalDeterministic(plan)
}

// MarshalSnapshotJSON renders a snapshot artifact with the embedded document
// bytes preserved verbatim.
func MarshalSnapshotJSON(snap *SnapshotArtifact) ([]byte, error) {
	return marshalDeterministic(snap)
}

// BaselineSnapshotFor builds the chain-root snapshot of an existing
// database: the introspected document plus the observed history report.
// It performs no database writes and adopts nothing.
func BaselineSnapshotFor(doc *V2Document, covers []string, history BaselineHistory) *SnapshotArtifact {
	return &SnapshotArtifact{
		FormatVersion: PlanFormatVersion,
		Kind:          "baseline",
		Version:       BaselineVersion,
		Name:          BaselineName,
		TargetSHA256:  doc.SHA256Hex,
		Covers:        covers,
		History:       &history,
		Document:      append(json.RawMessage(nil), doc.Canonical...),
	}
}

// AppliedVersionsReadOnly reports the applied migration versions and the
// history shape WITHOUT creating, altering or adopting anything. The
// versions of a legacy history are read as decimal text for the report,
// but the shape says whether they can be trusted as applied state.
func (c *Client) AppliedVersionsReadOnly(ctx context.Context) ([]string, HistoryShape, error) {
	shape, err := c.InspectMigrationHistory(ctx)
	if err != nil {
		return nil, shape, err
	}
	switch shape {
	case HistoryAbsent:
		return nil, shape, nil
	case HistoryV2Text:
		rows, err := c.pool.Query(ctx, "SELECT version FROM _neutron_migrations ORDER BY version")
		if err != nil {
			return nil, shape, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				return nil, shape, err
			}
			out = append(out, v)
		}
		return out, shape, rows.Err()
	case HistoryV2Integer, HistoryLegacyInteger:
		rows, err := c.pool.Query(ctx, "SELECT version::text FROM _neutron_migrations ORDER BY version")
		if err != nil {
			return nil, shape, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				return nil, shape, err
			}
			out = append(out, v)
		}
		return out, shape, rows.Err()
	default: // legacy-text, incompatible
		rows, err := c.pool.Query(ctx, "SELECT version FROM _neutron_migrations ORDER BY version")
		if err != nil {
			return nil, shape, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				return nil, shape, err
			}
			out = append(out, v)
		}
		return out, shape, rows.Err()
	}
}

// MigrationArtifactSet builds the four files of one generated migration:
// up.sql, down.sql, plan.json and the target snapshot. All content is
// computed before any write so failures cannot leave partial artifacts.
func MigrationArtifactSet(migrationsDir, version, name string, plan *PlanArtifact, desired *V2Document, upSQL, downSQL string) ([]ArtifactFile, error) {
	slug, err := migrationNameSlug(name)
	if err != nil {
		return nil, err
	}
	stem := version + "_" + slug

	upContent := fmt.Sprintf("-- Migration: %s\n\n%s\n", name, strings.TrimRight(upSQL, "\n"))
	downContent := fmt.Sprintf("-- Rollback: %s\n\n%s\n", name, strings.TrimRight(downSQL, "\n"))
	planJSON, err := MarshalPlanJSON(plan)
	if err != nil {
		return nil, err
	}
	snap := &SnapshotArtifact{
		FormatVersion: PlanFormatVersion,
		Kind:          "migration",
		Version:       version,
		Name:          slug,
		BaseSHA256:    plan.BaseSHA256,
		TargetSHA256:  desired.SHA256Hex,
		Document:      append(json.RawMessage(nil), desired.Canonical...),
	}
	snapJSON, err := MarshalSnapshotJSON(snap)
	if err != nil {
		return nil, err
	}
	return []ArtifactFile{
		{Path: filepath.Join(migrationsDir, stem+".up.sql"), Content: []byte(upContent)},
		{Path: filepath.Join(migrationsDir, stem+".down.sql"), Content: []byte(downContent)},
		{Path: filepath.Join(migrationsDir, stem+".plan.json"), Content: planJSON},
		{Path: filepath.Join(migrationsDir, SnapshotDir, stem+".snapshot.json"), Content: snapJSON},
	}, nil
}
