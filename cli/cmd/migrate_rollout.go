package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/spf13/cobra"
)

const operatorArtifactLimit = 1 << 20

func operatorReadArtifact(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("artifact unreadable")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, operatorArtifactLimit+1))
	if err != nil || len(raw) > operatorArtifactLimit {
		return nil, errors.New("artifact unreadable or exceeds 1 MiB")
	}
	return raw, nil
}

// Strict token validation rejects duplicate keys and excessive nesting before
// typed decoding; errors never include raw artifact strings or file paths.
func operatorStrictJSON(raw []byte, destination any) error {
	if len(raw) > operatorArtifactLimit {
		return errors.New("artifact exceeds limit")
	}
	tokens := json.NewDecoder(bytes.NewReader(raw))
	tokens.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 128 {
			return errors.New("artifact nesting exceeds limit")
		}
		token, err := tokens.Token()
		if err != nil {
			return errors.New("malformed artifact")
		}
		delimiter, container := token.(json.Delim)
		if !container {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]bool{}
			for tokens.More() {
				key, err := tokens.Token()
				name, ok := key.(string)
				if err != nil || !ok || seen[name] {
					return errors.New("duplicate or invalid artifact key")
				}
				seen[name] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for tokens.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return errors.New("invalid artifact delimiter")
		}
		if _, err := tokens.Token(); err != nil {
			return errors.New("malformed artifact")
		}
		return nil
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := tokens.Token(); err != io.EOF {
		return errors.New("trailing artifact data")
	}
	if err := operatorExactJSONFields(raw, reflect.TypeOf(destination)); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("artifact fields or values refused")
	}
	return nil
}

// encoding/json matches struct names case-insensitively; protocol fields
// are exact, so aliases must not overwrite the same decoded observation.
func operatorExactJSONFields(raw []byte, target reflect.Type) error {
	for target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	switch target.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			return errors.New("artifact object expected")
		}
		allowed := map[string]reflect.Type{}
		for index := 0; index < target.NumField(); index++ {
			field := target.Field(index)
			if !field.IsExported() {
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			allowed[name] = field.Type
		}
		for name, value := range fields {
			member, ok := allowed[name]
			if !ok {
				return errors.New("unknown exact artifact field")
			}
			if err := operatorExactJSONFields(value, member); err != nil {
				return err
			}
		}
	case reflect.Slice:
		var values []json.RawMessage
		if json.Unmarshal(raw, &values) != nil {
			return errors.New("artifact list expected")
		}
		for _, value := range values {
			if err := operatorExactJSONFields(value, target.Elem()); err != nil {
				return err
			}
		}
	case reflect.Map:
		var values map[string]json.RawMessage
		if json.Unmarshal(raw, &values) != nil {
			return errors.New("artifact map expected")
		}
		for _, value := range values {
			if err := operatorExactJSONFields(value, target.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}

func operatorHash(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func operatorJSON(cmd *cobra.Command, value any) error {
	return json.NewEncoder(cmd.OutOrStdout()).Encode(value)
}
func operatorFailure(cmd *cobra.Command, status, code string) error {
	if err := operatorJSON(cmd, map[string]any{"operatorVersion": 1, "status": status, "code": code, "effectsAutomaticallyRetried": false}); err != nil {
		return errors.New("operator output failed")
	}
	return errors.New("operator refused or failed; inspect JSON status (native diagnostics suppressed)")
}

type rolloutOperatorObservation struct {
	ObservationVersion            int                        `json:"observationVersion"`
	BaseSchemaSHA256              string                     `json:"baseSchemaSha256"`
	MigrationHashes               map[string]string          `json:"migrationHashes"`
	ActiveApplications            []db.RolloutApplication    `json:"activeApplications"`
	Completed                     []rolloutOperatorCompleted `json:"completed"`
	DestructiveConfirmationSHA256 string                     `json:"destructiveConfirmationSha256"`
}
type rolloutOperatorCompleted struct {
	Kind           string `json:"kind"`
	ArtifactSHA256 string `json:"artifactSha256"`
	EvidenceSHA256 string `json:"evidenceSha256"`
}

func newRolloutOperatorCommand() *cobra.Command {
	group := &cobra.Command{Use: "rollout", Short: "Plan versioned rollout phases without executing effects"}
	plan := &cobra.Command{Use: "plan", Args: cobra.NoArgs, Short: "Validate exact artifacts and operator observations; emit next phase only", RunE: func(cmd *cobra.Command, _ []string) error {
		artifactPath, _ := cmd.Flags().GetString("artifact")
		observedPath, _ := cmd.Flags().GetString("observation")
		raw, err := operatorReadArtifact(artifactPath)
		if err != nil {
			return operatorFailure(cmd, "refused", "artifact_unreadable")
		}
		artifact, err := db.ParseRolloutArtifact(raw)
		if err != nil {
			return operatorFailure(cmd, "refused", "artifact_invalid")
		}
		observedRaw, err := operatorReadArtifact(observedPath)
		if err != nil {
			return operatorFailure(cmd, "refused", "observation_unreadable")
		}
		var observed rolloutOperatorObservation
		if operatorStrictJSON(observedRaw, &observed) != nil || observed.ObservationVersion != 1 {
			return operatorFailure(cmd, "refused", "observation_invalid")
		}
		observation := db.RolloutObservation{BaseSchemaSHA256: observed.BaseSchemaSHA256, MigrationHashes: observed.MigrationHashes, ActiveApplications: observed.ActiveApplications, DestructiveConfirmationSHA256: observed.DestructiveConfirmationSHA256}
		for _, item := range observed.Completed {
			observation.Completed = append(observation.Completed, db.RolloutCompletedPhase{Kind: item.Kind, ArtifactSHA256: item.ArtifactSHA256, EvidenceSHA256: item.EvidenceSHA256})
		}
		next, err := db.PlanRolloutNext(*artifact, observation)
		if err != nil {
			return operatorFailure(cmd, "refused", "preconditions_differ")
		}
		return operatorJSON(cmd, map[string]any{"operatorVersion": 1, "status": "planned", "artifactSha256": next.ArtifactSHA256, "observationSha256": operatorHash(observedRaw), "observationTrust": "operator assertions; not live application or deployment proof", "effects": false, "complete": next.Complete, "nextPhase": next.Phase})
	}}
	plan.Flags().String("artifact", "", "versioned rollout artifact JSON file (maximum 1 MiB)")
	plan.Flags().String("observation", "", "versioned operator observation JSON file (trusted assertions, maximum 1 MiB)")
	group.AddCommand(plan)
	return group
}
func init() { migrateCmd.AddCommand(newRolloutOperatorCommand()) }
