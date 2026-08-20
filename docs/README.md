# docs/

Cross-cutting documentation for the Neutron monorepo: adoption findings,
benchmarks, the framework-excellence program, and historical research.

**This is not the user documentation.** That lives at
[neutron.build/docs](https://neutron.build/docs), generated from
`typescript/apps/site`. This tree is working material for people changing
Neutron itself.

**This is not a status report either.** Roughly half of what follows is historical
and carries a banner saying so. Read the banner before believing a checklist.

## Where things actually live

| Question | Answer |
|---|---|
| How do I use Neutron? | [neutron.build/docs](https://neutron.build/docs) |
| How do I change Neutron? | [`AGENTS.md`](../AGENTS.md) at the repo root |
| What does an SDK have to implement? | [`FRAMEWORK_CONTRACT.md`](../FRAMEWORK_CONTRACT.md) |
| What is the engine's completion state? | [`nucleus/DATABASE_COMPLETION.md`](../nucleus/DATABASE_COMPLETION.md) |
| What numbers may I cite? | `_internal/GROUND_TRUTH.md` (private), machine-checked by `sh nucleus/scripts/metrics.sh --check` |
| What is being worked on next? | `_internal/HANDOFF.md` (private) — the entry point; `ORCHESTRATION.md` beside it is the ordered plan |
| What is NOT hardened? | [`RESIDUAL_RISKS.md`](RESIDUAL_RISKS.md) — public, and deliberately separate from the release notes |
| What bugs are known? | [`nucleus/AUDIT_FINDINGS.md`](../nucleus/AUDIT_FINDINGS.md), [`nucleus/docs/PROBES.md`](../nucleus/docs/PROBES.md) |

Files under `_internal/` are private planning material and are not part of any
checkout of the public repo.

## Current

Maintained, and safe to read as describing the project today.

- [`RESIDUAL_RISKS.md`](RESIDUAL_RISKS.md) — the standing register of what is
  **not** hardened: cross-model transactions, two index paths that read the whole
  table, what "formally verified" does and does not mean here, and the whole
  categories of performance nobody has measured. Twelve entries, each naming the
  test or measurement behind it, several of them naming a *characterization*
  test that passes today by asserting the current bad behaviour and fails the
  moment it improves. Kept separate from release notes on purpose, so reading
  only the good news is not possible. Written 2026-08-20.
- [`CLAIM_RECONCILIATION.md`](CLAIM_RECONCILIATION.md) — 93 public claims traced
  to their three evidence legs (the source that implements it, the test that
  exercises it, the workflow that runs that test): 44 supported, 10 true only
  with a missing constraint, 38 unsupported. Includes the ones that held,
  because a list of only the hits is not an audit. Written 2026-08-19; re-run it
  before any release that makes new claims.
- [`ADOPTION_FINDINGS.md`](ADOPTION_FINDINGS.md) — the single `A-###` log of
  problems found by **building real products on Neutron**, rather than by reading
  its docs or tests. Add to it whenever building on Neutron makes you work around
  something. This is the most useful file in this tree.
- [`framework-excellence/`](framework-excellence/) — the program driving each
  language framework to a defined bar, with a per-language gap analysis
  (`go.md`, `python.md`, `rust.md`, `ts.md`) and implementation specs. Written
  2026-06; the analysis still holds, the completion claims should be spot-checked.
- [`phases/PHASE4-INDEX.md`](phases/PHASE4-INDEX.md),
  [`operations/CI-CD-INFRASTRUCTURE.md`](operations/CI-CD-INFRASTRUCTURE.md),
  [`operations/DEVOPS-SUMMARY.md`](operations/DEVOPS-SUMMARY.md) — DevOps and
  deployment infrastructure, refreshed 2026-08.

## Benchmarks — read the warning first

- [`benchmarks/`](benchmarks/) — six files, **every one carrying a warning banner
  added 2026-08-15**. The numbers were produced by a harness that hard-wires the
  RAM-resident storage adapter with no flag to change it, and records failed
  operations as latency samples inside the timer. They therefore do not measure
  the engine `nucleus serve` runs, and they contradict
  [`nucleus/docs/BENCH_VS_POSTGRES.md`](../nucleus/docs/BENCH_VS_POSTGRES.md) by
  roughly 49x on single-row INSERT.

  **Do not cite anything in that directory.** The reproducible TypeScript
  framework harness in [`typescript/benchmarks/`](../typescript/benchmarks) is
  fine; this directory is the engine one, and it is not.

## Historical — March 2026

The Phase 1–4 Nucleus optimization program: SIMD vectorization, zone maps and
sparse indexing, a binary wire protocol, and the operations material written to
run it. **Kept for the research and the reasoning, which are genuinely good.**

Every file here now carries a banner. They are five months old and predate most
of the engine's current shape:

- The **binary TLV wire protocol** was cut outright — `nucleus/src/binary_wire/`
  does not exist and nothing references it.
- **Zone maps** and columnar pushdown landed but were later measured as dead ends
  for the query problem they were introduced to solve.
- Every count, checklist and "complete" marker predates five months of work.

Directories: [`research/`](research/) (SIMD analysis, the M3 mitigation summary,
the old optimization roadmap), [`phases/`](phases/) (Phase 1 and Phase 2A
guides, delivery reports and checklists), [`implementation/`](implementation/)
(Phase 2A statistics), and the March files under [`operations/`](operations/) —
`RUNBOOK.md`, `DEPLOYMENT-GUIDE.md`, `MONITORING-SETUP.md`,
`INTEGRATION-CHECKLIST.md`, `OPTIMIZATION-QUICK-REFERENCE.md`. Those five
describe operating features that in some cases were never shipped; treat them as
design intent, not as runbooks.

## Adding to this tree

Put it in the right half. A new document that describes current state goes under
**Current** and gets linked here; a document that is a point-in-time report gets
a date in its banner on the day it is written, not five months later.

Do not add a second status document, a second next-work list, or an undated
audit snapshot — `nucleus/CLAUDE.md` explains why, and this index exists because
that rule was not applied to `docs/` when it was applied everywhere else.
