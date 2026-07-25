# Nucleus runbooks

Operational procedures for a single-node Nucleus instance. Every step here is
written against the version of the engine in this tree — the CLI flags, config
keys and failure modes were read out of the source and out of
`docs/CLI_REFERENCE.md`, which is generated from the clap definitions and
cannot drift from the parser.

| Runbook | Covers |
|---|---|
| [BACKUP_RESTORE_PITR.md](BACKUP_RESTORE_PITR.md) | Choosing a backup method, taking one against a running server, restoring, point-in-time recovery, and what each method silently omits |
| [UPGRADE.md](UPGRADE.md) | Moving between versions, on-disk format compatibility, and the pre-flight that makes an upgrade reversible |
| [ROLLBACK.md](ROLLBACK.md) | Going back, and the cases where going back is not possible |
| [SECURITY.md](SECURITY.md) | Authentication, roles, RLS, encryption at rest, TLS, network exposure, and secret handling |
| [INCIDENT.md](INCIDENT.md) | Triage for the failure modes this engine actually has: read-only disk watermark, connection exhaustion, memory ceiling, slow recovery, WAL growth, corruption |

## Runbooks that are deliberately absent

**Cluster operations.** There is no cluster runbook because there is no
supported cluster. Raft hard state (term, voted-for, log, commit index) is
never persisted, and replication ships raw SQL strings, so `now()`, `random()`
and `nextval()` diverge across replicas. Any procedure written today would
describe a system that loses data on restart. This closes with Milestone 9;
see `DATABASE_COMPLETION.md`.

**Rolling upgrade.** Requires two installable versions and a cluster to roll
across. Blocked on the same milestone plus the first tagged release built by
the versioned-asset release workflow.

## Before using any of these

Read `README.md` §Support status. Nucleus is a developer preview. The
procedures below are correct for what the engine does; they do not make it
production-complete.

## Scope of every procedure here

- One node, one process, one data directory.
- The `nucleus` binary and the data directory are on the same host.
- Commands are shown for a systemd install (`deploy/systemd/`). For the
  container and k3s equivalents, substitute `docker exec` /
  `kubectl exec` — the engine-level steps are identical.
