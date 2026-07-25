# Runbook — rollback

Getting back to the previous version after an upgrade. Which procedure applies
depends entirely on whether the on-disk `format_version` changed.

## Status of this procedure

**Written, never executed.** See the same note in [UPGRADE.md](UPGRADE.md#status-of-this-procedure).

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

## 1. Same format version — binary swap

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
