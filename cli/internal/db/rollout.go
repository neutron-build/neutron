package db

// Rollout artifacts are pure operator planning inputs. Observed application
// versions and completed phases are trusted assertions, never live proof.
// This planner executes no SQL and does not replace migration protocol v2.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

type RolloutMigration struct {
	ID       string `json:"id"`
	UpSHA256 string `json:"upSha256"`
}
type RolloutApplication struct {
	Version        string `json:"version"`
	ArtifactSHA256 string `json:"artifactSha256"`
}
type RolloutBackfill struct {
	ImplementationSHA256 string `json:"implementationSha256"`
	MaxBatchRows         int64  `json:"maxBatchRows"`
	MaxBatchMilliseconds int64  `json:"maxBatchMilliseconds"`
}
type RolloutPhase struct {
	Kind             string               `json:"kind"`
	Applications     []RolloutApplication `json:"applications"`
	Migrations       []RolloutMigration   `json:"migrations"`
	Backfill         *RolloutBackfill     `json:"backfill,omitempty"`
	ValidationSHA256 string               `json:"validationSha256,omitempty"`
	RetiredVersions  []string             `json:"retiredVersions"`
	Destructive      bool                 `json:"destructive"`
}
type RolloutArtifact struct {
	RolloutVersion     int            `json:"rolloutVersion"`
	WorkflowID         string         `json:"workflowId"`
	BaseSchemaSHA256   string         `json:"baseSchemaSha256"`
	TargetSchemaSHA256 string         `json:"targetSchemaSha256"`
	Phases             []RolloutPhase `json:"phases"`
}
type RolloutCompletedPhase struct {
	Kind           string
	ArtifactSHA256 string
	EvidenceSHA256 string
}
type RolloutObservation struct {
	BaseSchemaSHA256              string
	MigrationHashes               map[string]string
	ActiveApplications            []RolloutApplication
	Completed                     []RolloutCompletedPhase
	DestructiveConfirmationSHA256 string
}
type RolloutNext struct {
	ArtifactSHA256 string
	Phase          *RolloutPhase
	Complete       bool
}

var rolloutKinds = []string{"expand", "compatible-deploy", "backfill", "validate", "cutover", "contract"}

func rolloutDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func rolloutIdentity(s string) bool {
	return s != "" && len(s) <= 128 && utf8.ValidString(s) && !strings.ContainsRune(s, '\ufffd') && !strings.ContainsAny(s, "\x00\r\n")
}

// ParseRolloutArtifact rejects unknown fields, trailing documents and malformed
// required values. Version 1 cannot represent arbitrary executable payloads.
func ParseRolloutArtifact(raw []byte) (*RolloutArtifact, error) {
	if len(raw) > 1<<20 {
		return nil, fmt.Errorf("rollout artifact exceeds 1 MiB")
	}
	if err := v2RejectDuplicateKeys(raw); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var a RolloutArtifact
	if err := dec.Decode(&a); err != nil {
		return nil, fmt.Errorf("rollout artifact: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("rollout artifact contains trailing data")
	}
	if _, err := CanonicalRolloutArtifact(a); err != nil {
		return nil, err
	}
	return &a, nil
}

// Canonical bytes are encoding/json compact UTF-8 with default HTML escaping,
// no trailing newline, struct field order as declared here, exact string values
// (no trimming or Unicode normalization), phase/migration order preserved,
// application/retired sets sorted bytewise, and all set/list empties encoded [].
// Integer fields are int64: fractional, overflow and exponent forms are refused
// by the decoder, avoiding float rounding. Text migration IDs remain exact.
// Version 1 pins this algorithm; changing it requires a new artifact version.
func CanonicalRolloutArtifact(a RolloutArtifact) ([]byte, error) {
	if a.RolloutVersion != 1 || !rolloutIdentity(a.WorkflowID) || !rolloutDigest(a.BaseSchemaSHA256) || !rolloutDigest(a.TargetSchemaSHA256) {
		return nil, fmt.Errorf("invalid rollout version, identity or schema digest")
	}
	if len(a.Phases) != len(rolloutKinds) {
		return nil, fmt.Errorf("rollout requires all six ordered phases")
	}
	a.Phases = append([]RolloutPhase(nil), a.Phases...)
	migrations := map[string]bool{}
	appDigests := map[string]string{}
	for i, p := range a.Phases {
		if p.Kind != rolloutKinds[i] || len(p.Applications) == 0 {
			return nil, fmt.Errorf("invalid ordered phase or empty application set at %d", i)
		}
		p.Applications = append([]RolloutApplication(nil), p.Applications...)
		seen := map[string]bool{}
		for _, app := range p.Applications {
			if !rolloutIdentity(app.Version) || !rolloutDigest(app.ArtifactSHA256) || seen[app.Version] {
				return nil, fmt.Errorf("invalid or duplicate phase application")
			}
			if old, ok := appDigests[app.Version]; ok && old != app.ArtifactSHA256 {
				return nil, fmt.Errorf("application version has conflicting artifact identity")
			}
			seen[app.Version] = true
			appDigests[app.Version] = app.ArtifactSHA256
		}
		sort.Slice(p.Applications, func(i, j int) bool { return p.Applications[i].Version < p.Applications[j].Version })
		p.Migrations = append([]RolloutMigration{}, p.Migrations...)
		if (p.Kind == "expand" || p.Kind == "contract") && len(p.Migrations) == 0 {
			return nil, fmt.Errorf("schema phase needs migration references")
		}
		if p.Kind != "expand" && p.Kind != "contract" && len(p.Migrations) != 0 {
			return nil, fmt.Errorf("migration references outside schema phases")
		}
		for _, m := range p.Migrations {
			if !rolloutIdentity(m.ID) || !rolloutDigest(m.UpSHA256) || migrations[m.ID] {
				return nil, fmt.Errorf("invalid or duplicate migration reference")
			}
			migrations[m.ID] = true
		}
		if p.Kind == "backfill" {
			if p.Backfill == nil || !rolloutDigest(p.Backfill.ImplementationSHA256) || p.Backfill.MaxBatchRows <= 0 || p.Backfill.MaxBatchRows > 1_000_000 || p.Backfill.MaxBatchMilliseconds <= 0 || p.Backfill.MaxBatchMilliseconds > 3_600_000 {
				return nil, fmt.Errorf("invalid bounded backfill specification")
			}
			copy := *p.Backfill
			p.Backfill = &copy
		} else if p.Backfill != nil {
			return nil, fmt.Errorf("backfill specification outside backfill phase")
		}
		if p.Kind == "validate" {
			if !rolloutDigest(p.ValidationSHA256) {
				return nil, fmt.Errorf("validation implementation digest required")
			}
		} else if p.ValidationSHA256 != "" {
			return nil, fmt.Errorf("validation digest outside validate phase")
		}
		p.RetiredVersions = append([]string{}, p.RetiredVersions...)
		retired := map[string]bool{}
		if p.Kind == "contract" {
			if !p.Destructive || len(p.RetiredVersions) == 0 {
				return nil, fmt.Errorf("contract requires destructive confirmation and explicit retired versions")
			}
		} else if p.Destructive || len(p.RetiredVersions) != 0 {
			return nil, fmt.Errorf("retirement/destructive flags outside contract")
		}
		for _, v := range p.RetiredVersions {
			if !rolloutIdentity(v) || retired[v] || seen[v] {
				return nil, fmt.Errorf("invalid retired application set")
			}
			retired[v] = true
		}
		sort.Strings(p.RetiredVersions)
		a.Phases[i] = p
	}
	return json.Marshal(a)
}

// PlanRolloutNext checks immutable inputs and returns only the next phase.
// Completed phase evidence digests bind caller-supplied evidence; this function
// neither verifies the evidence's truth nor certifies zero downtime.
func PlanRolloutNext(a RolloutArtifact, o RolloutObservation) (RolloutNext, error) {
	var next RolloutNext
	raw, err := CanonicalRolloutArtifact(a)
	if err != nil {
		return next, err
	}
	sum := sha256.Sum256(raw)
	hash := hex.EncodeToString(sum[:])
	next.ArtifactSHA256 = hash
	if o.BaseSchemaSHA256 != a.BaseSchemaSHA256 {
		return RolloutNext{}, fmt.Errorf("base schema precondition differs")
	}
	for _, p := range a.Phases {
		for _, m := range p.Migrations {
			if o.MigrationHashes[m.ID] != m.UpSHA256 {
				return RolloutNext{}, fmt.Errorf("migration digest precondition differs")
			}
		}
	}
	if len(o.Completed) > len(a.Phases) {
		return RolloutNext{}, fmt.Errorf("completed phase prefix too long")
	}
	for i, e := range o.Completed {
		if e.Kind != a.Phases[i].Kind || e.ArtifactSHA256 != hash || !rolloutDigest(e.EvidenceSHA256) {
			return RolloutNext{}, fmt.Errorf("completed phase prefix/evidence does not bind artifact")
		}
	}
	if len(o.Completed) == len(a.Phases) {
		next.Complete = true
		return next, nil
	}
	p := a.Phases[len(o.Completed)]
	if len(o.ActiveApplications) == 0 {
		return RolloutNext{}, fmt.Errorf("active application observation required")
	}
	allowed := map[string]string{}
	for _, app := range p.Applications {
		allowed[app.Version] = app.ArtifactSHA256
	}
	seen := map[string]bool{}
	for _, app := range o.ActiveApplications {
		if seen[app.Version] || allowed[app.Version] != app.ArtifactSHA256 || !rolloutDigest(app.ArtifactSHA256) {
			return RolloutNext{}, fmt.Errorf("application compatibility precondition differs")
		}
		seen[app.Version] = true
	}
	if p.Destructive && o.DestructiveConfirmationSHA256 != hash {
		return RolloutNext{}, fmt.Errorf("destructive confirmation must bind exact artifact")
	}
	// Return detached normalized metadata so caller mutation cannot alter input.
	var normalized RolloutArtifact
	_ = json.Unmarshal(raw, &normalized)
	phase := normalized.Phases[len(o.Completed)]
	next.Phase = &phase
	return next, nil
}
