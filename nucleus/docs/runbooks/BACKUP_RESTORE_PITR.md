# Runbook — backup, restore, point-in-time recovery

Authoritative background: [`DURABILITY.md`](../../DURABILITY.md). This runbook
is the procedure; that document is the evidence.

## 0. Choose a method before you need one

| Method | Server may be running | Captures | Does NOT capture | Restores to |
|---|---|---|---|---|
| `BACKUP DATABASE TO '<path>'` (SQL) | **Yes** | SQL substrate at a named consistent LSN, plus specialty-model WALs and catalog copied after it | Nothing crash-atomic across the SQL WAL and the model WALs | A directory you then `nucleus start` |
| `nucleus backup --online` | No — it opens the directory itself | Same coordinated snapshot | Encrypted or compressed data files (refuses) | Same |
| `nucleus backup` (plain) | **No — refuses a live directory** | Byte copy of the whole directory | Consistency, if forced with `--allow-in-use` | Same |
| `nucleus dump` / `nucleus load` | Stopped or quiesced | Tables and rows, as portable SQL | **Roles, RLS policies, views, sequence state** | A new data directory, via replay |
| `nucleus restore-pitr` | No | Base snapshot + archived WAL replayed to a target | Model-specific WALs beyond the base; encrypted WAL | Same |

Two facts that decide the choice:

1. **Only `BACKUP DATABASE TO` can snapshot a database that is serving
   traffic.** `nucleus backup` takes a liveness try-lock on `nucleus.lock` and
   refuses a directory a running instance holds, because a plain recursive copy
   of a database under write is torn.
2. **`nucleus dump` is a data-only export, not a backup.** It emits tables and
   rows. It omits roles, RLS policies, views and sequence state, and it writes
   `DEFAULT nextval(...)` without creating the sequence — so a restored table
   rejects inserts that rely on a `SERIAL` default. Use it for migration across
   format changes, never as your only backup.

## 1. Online backup of a running server (preferred)

The running server snapshots itself; the coordination happens inside the
process that owns the directory.

```sql
-- as a superuser, over pgwire
BACKUP DATABASE TO '/backups/nucleus-2026-07-24';
```

Requirements and refusals, all deliberate:

- Superuser only.
- The destination must be **outside** the data directory. A path inside it is
  refused — the tree copy would otherwise descend into the snapshot it is
  writing until the path exceeded the OS limit.
- Engines with no physical snapshot (memory, MVCC) return an explicit refusal
  rather than producing something that merely looks like a backup.

While the backup runs, WAL retention is pinned at the window's start LSN. On a
write-heavy database **a long backup grows the WAL for its whole duration and
there is no cap**. Watch free space (§5) during large backups. The pin is
released on every exit path including failure.

Verify a snapshot before you rely on it — a backup you have not restored is a
hypothesis:

```bash
nucleus restore --input /backups/nucleus-2026-07-24 --data /tmp/verify
nucleus start --data /tmp/verify --port 55432 &
psql -h 127.0.0.1 -p 55432 -c "SELECT count(*) FROM <your largest table>"
```

## 2. Cold backup

```bash
systemctl stop nucleus
nucleus backup --data /var/lib/nucleus --output /backups/nucleus-$(date +%F)
systemctl start nucleus
```

`--allow-in-use` exists for the case where the lock is held by a crashed
process. It stamps `taken_while_in_use: true` into the snapshot manifest, so
the caveat outlives the command that produced it. Do not use it to "back up"
a healthy running server — use §1.

## 3. Restore

```bash
systemctl stop nucleus
nucleus restore --input /backups/nucleus-2026-07-24 --data /var/lib/nucleus --force
chown -R nucleus:nucleus /var/lib/nucleus
systemctl start nucleus
```

Restore validates the manifest checksum, the on-disk **format version**, the
destination's liveness lock and the destination's database identity *before*
it removes anything. A refused restore leaves the destination byte-for-byte
unchanged.

Compatibility keys on `format_version`, not the release string, so patch
releases interoperate. Snapshots written before that field existed fall back to
an exact-version lock.

## 4. Point-in-time recovery

PITR needs continuous WAL archiving switched on **before** the incident. It is
not retroactive.

### 4.1 Enable archiving

```bash
# /etc/nucleus/nucleus.env
NUCLEUS_WAL_ARCHIVE_DIR=/var/backups/nucleus-wal
```

With the systemd unit, `ProtectSystem=strict` makes everything outside
`/var/lib/nucleus` read-only. Archiving fails with `EROFS` until you add the
path to the unit:

```ini
ReadWritePaths=/var/backups/nucleus-wal
```

Archiving is archive-on-seal, and `truncate_before` never deletes a segment
that has not been archived — so a broken archive destination stops WAL
reclamation rather than silently losing recoverability. That is the correct
trade and it means **a full archive volume eventually fills the WAL volume**.
Monitor both.

### 4.2 Take a base snapshot

PITR replays forward from a physical base. Take one (§1 or §2) after enabling
archiving, and keep it for at least as long as your recovery window.

### 4.3 Recover

```bash
systemctl stop nucleus

# To an exact LSN
nucleus restore-pitr \
  --base    /backups/nucleus-2026-07-24 \
  --archive /var/backups/nucleus-wal/<database-subdir> \
  --data    /var/lib/nucleus \
  --lsn     123456 \
  --force

# Or to a wall-clock time (Unix seconds, SEGMENT granularity — you land on
# the last segment archived at or before this time, not on the exact second)
nucleus restore-pitr ... --time 1753300000 --force

# Or to the latest archived point (omit both --lsn and --time)
nucleus restore-pitr ... --force

chown -R nucleus:nucleus /var/lib/nucleus
systemctl start nucleus
```

Recovery works by copying a byte-exact prefix of the archived WAL truncated at
the target and then replaying it through ordinary recovery — the same code path
a crash would take.

### 4.4 What PITR does not cover

Stated plainly, because a recovery plan built on a wrong assumption is worse
than none:

- **Segmented plaintext SQL WAL only.** Encrypted WAL is out of scope.
- **The SQL substrate only.** Model-specific WALs (KV, document, graph,
  vector, timeseries, columnar, streams, CDC, blob) are not replayed forward
  from the archive. They are restored at the base snapshot's state.
- **Datalog, sparse vectors and tensors have no durable store at all** — writes
  are acknowledged and lost on restart, with no error. Nothing can recover
  them. See `docs/MODEL_SEMANTICS.md`.
- **FTS.** `fts_index.json` is rewritten whole with no temp+rename and no
  fsync, and a parse failure on load is swallowed — so a crash mid-rewrite
  starts the server with a stale index and no complaint. After any crash,
  treat the FTS index as suspect and rebuild it.
- **There is no shared commit record between the SQL WAL and the model WALs**,
  so a transaction spanning both is not atomic across a crash by construction.

## 5. Retention and monitoring

- Verify one restore per retention cycle (§1). An unverified backup is not a
  backup.
- Alert on the WAL archive directory's free space, not just the data volume.
- Alert on backup duration. A backup that runs long pins WAL retention for its
  whole duration, and the checkpoint segment rescan gets more expensive as the
  WAL grows.
- Keep at least one base snapshot older than your longest plausible
  detection-to-recovery gap. PITR cannot rewind past the oldest base you kept.
