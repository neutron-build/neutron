package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/neutron-build/neutron/cli/internal/config"
	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/spf13/cobra"
)

var operatorDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func backfillOperatorInput(cmd *cobra.Command, action string) (db.BackfillSpec, string, time.Duration, error) {
	var empty db.BackfillSpec
	profile, _ := cmd.Flags().GetString("profile")
	if profile != "postgres-direct" {
		return empty, "", 0, errors.New("explicit postgres-direct profile required")
	}
	timeout, _ := cmd.Flags().GetDuration("timeout")
	if timeout <= 0 || timeout > 60*time.Second {
		return empty, "", 0, errors.New("timeout must be positive and at most 60s")
	}
	approved, _ := cmd.Flags().GetString("approve-job")
	if action == "chunk" && !operatorDigestPattern.MatchString(approved) {
		return empty, "", 0, errors.New("exact approved job digest required")
	}
	if action != "chunk" && approved != "" {
		return empty, "", 0, errors.New("job approval applies only to a chunk")
	}
	path, _ := cmd.Flags().GetString("spec")
	raw, err := operatorReadArtifact(path)
	if err != nil {
		return empty, "", 0, err
	}
	spec, err := db.ParseBackfillSpec(raw)
	if err != nil {
		return empty, "", 0, errors.New("backfill spec refused")
	}
	if time.Duration(spec.TimeoutMilliseconds)*time.Millisecond > timeout {
		return empty, "", 0, errors.New("command timeout must cover the admitted chunk budget")
	}
	return spec, approved, timeout, nil
}
func backfillOperatorDatabaseError(cmd *cobra.Command, err error) error {
	status := "failed"
	var backfill *db.BackfillError
	if errors.As(err, &backfill) && backfill.Indeterminate {
		status = "indeterminate"
	}
	sqlstate := ""
	var native *pgconn.PgError
	if errors.As(err, &native) {
		sqlstate = native.Code
	}
	if e := operatorJSON(cmd, map[string]any{"operatorVersion": 1, "status": status, "code": "database_operation_failed", "sqlState": sqlstate, "effectsAutomaticallyRetried": false, "guidance": "reconcile checkpoint and database before another approved chunk; native diagnostics suppressed"}); e != nil {
		return errors.New("operator output failed")
	}
	return errors.New("backfill failed; inspect JSON status (native diagnostics suppressed)")
}
func newBackfillOperatorCommand() *cobra.Command {
	group := &cobra.Command{Use: "backfill", Short: "Inspect and run one exact-approved bounded copy-column chunk", Long: `Direct PostgreSQL copy-column-v1 only. Provision the exact checkpoint table independently:
job_id text PRIMARY KEY, job_digest text NOT NULL, format text NOT NULL,
chunks bigint NOT NULL, updated_rows bigint NOT NULL; no defaults, RLS or user triggers.
Spec version 1 names jobId, source/checkpoint qualified {schema,name}, key,
from/to, transformation "copy-column-v1", writerPolicySha256, batchRows and
timeoutMilliseconds. The writer policy digest is an operator attestation,
not automatic proof of deployed compatible writes. Inspect emits jobDigest;
chunk requires that exact digest and re-inspects live identity before effects.
No DDL, automatic retries, loops, external transformations or automatic
completion. The direct endpoint profile is an operator assertion;
transaction poolers are unsupported. Uses the existing database config/env.`}
	for _, name := range []string{"inspect", "chunk", "validate"} {
		action := name
		command := &cobra.Command{Use: action, Args: cobra.NoArgs, Short: map[string]string{"inspect": "Read catalog identity and emit exact job approval digest without writes", "chunk": "Execute one bounded chunk with exact approved job digest", "validate": "Read one mismatch snapshot; never declare deployed completion"}[action], RunE: func(cmd *cobra.Command, _ []string) error {
			spec, approved, timeout, err := backfillOperatorInput(cmd, action)
			if err != nil {
				return operatorFailure(cmd, "refused", "input_invalid")
			}
			canceled, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			ctx, cancel := context.WithTimeout(canceled, timeout)
			defer cancel()
			client, err := db.Connect(ctx, config.DatabaseURL())
			if err != nil {
				return backfillOperatorDatabaseError(cmd, err)
			}
			defer client.Close()
			job, err := db.InspectBackfillJob(ctx, client, spec)
			if err != nil {
				return backfillOperatorDatabaseError(cmd, err)
			}
			canonical, _ := json.Marshal(spec)
			switch action {
			case "inspect":
				return operatorJSON(cmd, map[string]any{"operatorVersion": 1, "status": "admitted", "jobDigest": job.Digest, "specSha256": operatorHash(canonical), "spec": spec, "effects": false, "profile": "postgres-direct (operator asserted)", "approval": "chunk requires exact jobDigest; database and relation identity are re-inspected before writes"})
			case "chunk":
				if approved != job.Digest {
					return operatorFailure(cmd, "refused", "approved_job_differs")
				}
				result, err := db.RunBackfillChunk(ctx, client, job)
				if err != nil {
					return backfillOperatorDatabaseError(cmd, err)
				}
				if result.Status == "busy" {
					return operatorFailure(cmd, "busy", "checkpoint_worker_busy")
				}
				return operatorJSON(cmd, map[string]any{"operatorVersion": 1, "status": result.Status, "jobDigest": job.Digest, "specSha256": operatorHash(canonical), "chunk": result, "complete": false, "effectsAutomaticallyRetried": false, "guidance": "idle is not completion; compatible writers and separate final validation are required"})
			case "validate":
				result, err := db.ValidateBackfill(ctx, client, job)
				if err != nil {
					return backfillOperatorDatabaseError(cmd, err)
				}
				status := "snapshot_validated"
				if result.Mismatches != 0 {
					status = "snapshot_mismatches"
				}
				if err := operatorJSON(cmd, map[string]any{"operatorVersion": 1, "status": status, "jobDigest": job.Digest, "validation": result, "complete": false, "guidance": "one repeatable-read snapshot only; compatible write policy, application retirement and phase cutover remain external gates"}); err != nil {
					return err
				}
				if result.Mismatches != 0 {
					return errors.New("backfill snapshot has mismatches; no completion proof")
				}
				return nil
			}
			return operatorFailure(cmd, "refused", "action_invalid")
		}}
		command.Flags().String("spec", "", "versioned copy-column-v1 spec JSON file (maximum 1 MiB)")
		command.Flags().String("profile", "", "required explicit profile: postgres-direct")
		command.Flags().Duration("timeout", 30*time.Second, "positive total admission/execution deadline, at most 60s and covering the spec chunk deadline")
		command.Flags().String("approve-job", "", "chunk only: exact digest emitted by live inspect (no wildcard/force)")
		group.AddCommand(command)
	}
	return group
}
func init() { migrateCmd.AddCommand(newBackfillOperatorCommand()) }
