# Cards (local-only)

Card files are `*.local.md` and are never committed (`.gitignore`). The live
set is in the v3 checkout, `~/Workspace/neutron-v3/nucleus/v3/cards/`; the
scripts find it through `NV3_CARDS` (that path is the default). Start with
`ORCHESTRATOR.local.md`, then `QUEUE.local.md`. The plan is
`PLAN-v3.local.md` in the same directory.
