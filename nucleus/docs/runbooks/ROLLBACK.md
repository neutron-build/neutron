# Runbook — rollback

Getting back to the previous version after an upgrade.

## 0. Correction, 2026-08-31 — read this before §1

This runbook used to route on `format_version` alone and call an unchanged
`format_version` "a binary swap. Cheap and safe." **That is wrong, and it is
wrong in the direction that loses data.** `format_version` covers the SQL
substrate's page format. The specialty-model WALs are versioned separately and
are not covered by it at all.

Measured 2026-08-31, v0.1.8 against HEAD (`c04a2a9f`), `format_version` 2 on
both sides, reproduced with native binaries and again end to end with the
published `:v0.1.8` and `:latest` images on linux/amd64:

- **One `KV_HSET` under the newer build is enough.** On the next start the older
  build fails the whole KV store open —
  `kv/collections.wal: record at offset N (op 74) failed its checksum` — logs
  **one** ERROR, marks the model VOLATILE, and comes up serving. Every key is
  gone, including keys the older build itself wrote before the upgrade. It then
  acknowledges new KV writes and loses them again on the next restart.
- A plain `KV_SET` loses that one key **with no log line at all**.
- Document and time-series writes made under the newer build vanish silently;
  records written before the upgrade survive in those two models.
- SST files are written with magic `LSM2` at HEAD; v0.1.8 accepts only `LSMS`
  and errors on the whole store. So a single flush or compaction closes the
  door independently of the WALs.
- SQL tables, roles, RLS policies, views, sequences and `catalog.json`'s new
  serde-defaulted keys all survive the round trip intact. A control directory
  taken v0.1.8 → v0.1.8 from the same seed kept everything, so the damage is
  entirely attributable to the round trip.

**The revised decision:**

```
Has the newer version served any traffic?
├── No  (it refused at boot, or you stopped it before any write)
│        → §1. Swap the binary back. The on-disk format check runs before WAL
│          recovery touches anything, so a refusal is provably non-destructive.
└── Yes → §2. Restore from the pre-upgrade backup. You WILL lose everything
          written since that backup. Do NOT try §1 first "to see" — an older
          binary opens the directory happily and only then discovers it cannot
          read the specialty WALs, and by then the KV model is gone.
```

`§1 first, it costs you a restart not your data` was the old advice. It is
withdrawn.

## Status of this procedure

§0 and §2 are measured. The rest is **written, never executed** — see the same
note in [UPGRADE.md](UPGRADE.md#status-of-this-procedure).

## Decision

```
Did the new version change format_version?
├── No  → §1. Swap the binary back. Minutes.
└── Yes → §2. Restore from the pre-upgrade backup. You WILL lose
           everything written since that backup.
```

If you do not know, try §1 first: the old binary **refuses** to open a
newer-format directory rather than corrupting it, so a failed §1 costs you a
restart, not your data.

## Exception: 1.0.0 → 0.1.8 is not a binary swap

That "try §1 first" advice is safe only because the format version is the thing
that changes when the on-disk layout changes. Once, it wasn't.

1.0.0 kept `DB_FORMAT_VERSION` at 2 — the page format genuinely did not move —
while S63 added four transaction-tagged record types to the **KV write-ahead
log**. 0.1.8 has no case for those tags and its replay fallthrough is
`RecordStep::Stop`, so it reads the first tagged record as end-of-log and
silently drops every KV record after it. The version check cannot catch this:
the number it compares did not change.

Tagged records appear only where a KV mutation runs inside a coordinating SQL
transaction. If nothing in your workload does that, §1 is safe. If you are not
certain it doesn't, treat this as §2 and restore the pre-upgrade snapshot — a
0.1.8 physical snapshot restores into either build, because the format version
is what governs that.

The general lesson, which applies to the next release too: **§1 is only sound
when no on-disk vocabulary changed without the format version changing.** Check
that, not just the constant.

## 1. Newer version never wrote — binary swap

```bash
systemctl stop nucleus
install -m 0755 /usr/local/bin/nucleus.previous /usr/local/bin/nucleus
nucleus version
systemctl start nucleus
journalctl -u nucleus -n 50 --no-pager
psql -c 'SELECT 1'
```

Keep the previous binary on disk during every upgrade. That single habit is
what makes this a two-minute operation.

## 2. Format version changed — restore

The old binary cannot read the migrated directory. Restoring the pre-upgrade
snapshot is the only path, and **every write since that snapshot is lost**.

Decide first whether that is acceptable. If it is not, the correct action is to
stay on the new version and fix forward.

```bash
systemctl stop nucleus

# Preserve the migrated directory. Do not delete it — it is the only copy of
# the post-upgrade writes, and it may be recoverable by fixing forward later.
mv /var/lib/nucleus /var/lib/nucleus.post-upgrade

install -m 0755 /usr/local/bin/nucleus.previous /usr/local/bin/nucleus
nucleus restore --input /backups/pre-upgrade-<date> --data /var/lib/nucleus
chown -R nucleus:nucleus /var/lib/nucleus
systemctl start nucleus
```

### 2.1 If the only pre-upgrade artifact is a logical dump

`nucleus dump` is data-only. A logical rollback restores tables and rows and
**drops roles, RLS policies, views and sequence state**.

```bash
nucleus load --input /backups/pre-upgrade.sql --data /var/lib/nucleus
```

Then, before letting any client connect, reapply from the captures taken in
[UPGRADE.md §3](UPGRADE.md#3-capture-what-a-logical-dump-loses):

1. Recreate roles and their grants.
2. Recreate every RLS policy, and re-enable RLS on each table. **Until this is
   done the data is unprotected** — a table whose policies did not survive is
   readable by anyone who can connect. Verify with `SELECT * FROM pg_policies`
   before opening the port.
3. Recreate views.
4. Recreate sequences at or above their recorded values. A restored table
   carries `DEFAULT nextval(...)` for a sequence the dump never created, so
   `SERIAL` inserts fail until you do.

## 3. Verify

Same checks as [UPGRADE.md §5](UPGRADE.md#5-verify), plus explicitly:

```bash
psql -c 'SELECT * FROM pg_policies'    # RLS is the one that fails silently
psql -c '\du'
```

## 4. What cannot be rolled back

- **Writes made after the backup you restore.** There is no merge path.
- **PITR does not help across a format change.** The archived WAL is replayed
  by the version that can read it; it does not translate between formats.
- **Datalog, sparse vectors, tensors.** No durable store at all — nothing to
  roll back, and nothing was persisted in the first place.
- **The FTS index** may be stale after any unclean stop (`fts_index.json` is
  rewritten non-atomically and load failures are swallowed). Rebuild it after
  any rollback rather than trusting it.
