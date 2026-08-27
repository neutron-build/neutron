# Maintenance — checkpoints, vacuum, statistics, integrity

The maintenance surface is deliberately small: three SQL commands that exist
because they are the recovery and space-reclaim paths operators actually need
mid-incident, plus one deliberate non-command. Generated command/config
reference: `docs/CLI_REFERENCE.md`, `docs/CONFIG_REFERENCE.md`.

## CHECKPOINT

`CHECKPOINT` forces the storage checkpoint: flushes dirty pages, seals the
WAL, and advances the LSN horizon. It is available in read-only degraded
mode — that availability is its purpose (see `INCIDENT.md`: the
disk-pressure recovery path runs CHECKPOINT, not a restart). Asserted in
`executor/tests/test_observability.rs` (LSN advances across the command).

## VACUUM — the compaction path

`VACUUM [table]` reclaims dead tuples and frees pages; **this is the
engine's compaction mechanism** — there is no separate COMPACT command and
none is planned. Specialty stores compact on their own checkpoints (each
specialty WAL's checkpoint is a rewrite that drops superseded state); VACUUM
covers the SQL heap and its indexes. Output reports pages scanned, dead
tuples reclaimed, pages freed, and bytes reclaimed.

## ANALYZE — statistics

`ANALYZE [table]` refreshes planner statistics and persists them next to the
catalog (`stats.json`). Stale statistics degrade plans, not answers.

## Integrity checking — deliberately not a command

Integrity verification lives in the probe fleet
(`sh scripts/probe.sh`), not in a SQL command, on purpose: a CHECK-type
command that runs inside the engine it verifies shares fate with a corrupt
engine — the same bug that corrupted the data can bless the check. The probes
are separate processes with their own harnesses (crash/recovery, coherence,
cross-model atomicity), which is the stronger design for the same coverage.
If an in-engine check is ever demanded, it belongs beside these three
commands with an explicit "advisory, not evidence" caveat.

## What is deliberately absent

- `REINDEX` — indexes are rebuilt from their tables at startup already
  (see `docs/RESIDUAL_RISKS.md` entry 4); a mid-run REINDEX adds a second
  rebuild path to maintain for the same outcome.
- `CLUSTER`/physical reorganization — no btree-heap locality model to
  exploit yet.
- Auto-vacuum — space reclamation is operator-scheduled. The disk watermarks
  (`storage.disk_*`) surface the pressure that would make scheduling urgent.
