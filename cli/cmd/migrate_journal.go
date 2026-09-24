package cmd

// Journaled-migration wiring (M06): the effects inspection, the apply
// dispatch target and the progress printer for `-- neutron:journaled`
// migrations. The journal model itself lives in internal/db
// (migrate_journal.go); everything here composes it with the M05
// precondition/resolve plumbing unchanged.

import (
	"context"
	"errors"
	"fmt"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/ui"
)

// inspectJournaledEffects builds the effects report of a journaled
// migration: one entry per step, evaluated through the step's verification
// (built-in postcondition or declared verify query). Every journaled step
// is provable by contract — parse-time validation refused unverifiable
// steps — but an evaluation can still FAIL: a step whose index identity
// cannot be pinned (unresolvable unqualified table), or a verify query
// that errors against the current state. Those steps are NEVER counted as
// absent: they carry their own state ("unresolved" / "verify errored"),
// block ProvenClean and mark-applied (a verdict must not rest on a probe
// that could not run), and the report line carries the reason — the M06
// rework's answer to the false "nothing ran — retries safely" verdict
// over a present effect (review-1 MAJOR-2).
func inspectJournaledEffects(ctx context.Context, client *db.Client, jf *db.JournaledFile) (*effectsReport, error) {
	report := &effectsReport{}
	for i := range jf.Steps {
		step := &jf.Steps[i]
		effect := statementEffect{
			Index:        step.Index,
			FirstLine:    step.FirstLine,
			Kind:         step.VerifyLabel(),
			CreationSide: true,
			State:        "unsatisfied",
		}
		state, detail, err := client.EvaluateJournaledStep(ctx, step)
		var ident *db.JournaledIdentityError
		switch {
		case errors.As(err, &ident):
			effect.State = "unresolved"
			effect.Kind = "identity unresolved: " + step.VerifyLabel()
			effect.FirstLine = step.FirstLine + " [" + ident.Detail + "]"
			report.Unevaluable++
		case err != nil:
			effect.State = "verify errored"
			effect.Kind = "verify errored: " + step.VerifyLabel()
			effect.FirstLine = step.FirstLine + " [" + err.Error() + "]"
			report.Unevaluable++
		case state == db.JournalStateSatisfied:
			effect.State = "satisfied"
			report.CreationsPresent++
		case state == db.JournalStateInvalid:
			effect.State = "invalid"
			report.HasInvalid = true
		default:
			effect.State = "unsatisfied"
			if detail != "" {
				effect.FirstLine = step.FirstLine + " [" + detail + "]"
			}
		}
		report.CreationsTotal++
		report.Effects = append(report.Effects, effect)
	}
	return report, nil
}

// printJournaledEvent renders one executor event as CLI progress. Journaled
// runs print per-step lines instead of a spinner: operational migrations
// are long, and step progress is the point.
func printJournaledEvent(e db.JournaledRunEvent) {
	switch e.Action {
	case "skip":
		ui.Infof("  step %d [skip] %s", e.StepIndex, e.Detail)
	case "run":
		ui.Infof("  step %d [run] %d row(s): applied", e.StepIndex, e.Rows)
	case "debris-drop":
		ui.Infof("  step %d [debris] %s", e.StepIndex, e.Detail)
	case "progress":
		ui.Infof("  step %d [progress] %s", e.StepIndex, e.Detail)
	}
}

// applyJournaledMigration runs one journaled migration through the journal
// executor with CLI progress, translating the executor's honest failures
// into the runner's interruption report (same shape as the M05
// nontransactional path: partial effects REMAIN, recovery is explicit).
func applyJournaledMigration(ctx context.Context, client *db.Client, sess *db.MigrationSession, p pendingMigration, batchDone, batchTotal int) error {
	ui.Infof("Applying %s_%s (journaled, %d step(s))...", p.File.Version, p.File.Name, len(p.Journal.Steps))
	err := sess.ApplyJournaledMigration(ctx, client, p.File, p.Journal, printJournaledEvent)
	if err == nil {
		ui.Successf("Applied %s_%s", p.File.Version, p.File.Name)
		return nil
	}
	var partial *db.NontransactionalPartialError
	if errors.As(err, &partial) {
		return fmt.Errorf(
			"interrupted after %d of %d pending migration(s) (%s_%s failed at journaled step %d of %d; earlier steps' effects REMAIN — no rollback is pretended):\n%v\ninspect and recover explicitly: `neutron migrate resolve %s`",
			batchDone, batchTotal, p.File.Version, p.File.Name, partial.Applied+1, partial.Total, err, p.File.Version)
	}
	var ident *db.JournaledIdentityError
	if errors.As(err, &ident) {
		return fmt.Errorf(
			"interrupted after %d of %d pending migration(s) (%s_%s step %d REFUSED BEFORE EXECUTION — index identity: unresolvable, ambiguous or colliding; the statement did NOT run, and earlier steps' effects, if any, REMAIN):\n%v\nfix the named cause and re-run, or recover explicitly: `neutron migrate resolve %s`",
			batchDone, batchTotal, p.File.Version, p.File.Name, ident.StepIndex, err, p.File.Version)
	}
	var verify *db.JournaledVerifyError
	if errors.As(err, &verify) {
		return fmt.Errorf(
			"interrupted after %d of %d pending migration(s) (%s_%s step %d: the statement ran but its verification did not hold — the step's effect is NOT proven):\n%v\ninspect and recover explicitly: `neutron migrate resolve %s`",
			batchDone, batchTotal, p.File.Version, p.File.Name, verify.StepIndex, err, p.File.Version)
	}
	return err
}
