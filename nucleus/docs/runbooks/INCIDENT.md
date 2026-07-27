# Runbook — incident response

Triage for the failure modes this engine actually has. Every threshold and
error code below was read out of the source or the generated
[`CONFIG_REFERENCE.md`](../CONFIG_REFERENCE.md); nothing here is a generic
database checklist.

## 0. First three commands

```bash
systemctl status nucleus
journalctl -u nucleus -n 200 --no-pager
curl -s http://127.0.0.1:9100/metrics | grep -E 'nucleus_(active_connections|connections_rejected_total|memory_rss_bytes|memory_writes_rejected|wal_size_bytes|open_transactions)'
```

The metrics endpoint is off by default (`NUCLEUS_METRICS_ENABLED`) and binds
loopback only. Turn it on **before** you need it — mid-incident is too late.

## 1. "Cannot connect" / clients hang

| Symptom | Likely cause | Confirm | Act |
|---|---|---|---|
| `FATAL 53300`, connection refused immediately | Connection limit hit (`server.max_connections`, default **100**) | `nucleus_connections_rejected_total` climbing; `nucleus_active_connections` at the limit | Find the leaking client. Raise the limit only after; a database serving 100 concurrent connections badly will serve 500 worse |
| TCP accepts, then nothing | Server still replaying the WAL at startup | `journalctl -u nucleus -f` shows recovery | Wait. §4 |
| TCP refused, unit active | Bound to a different host/port than you think | `ss -lntp \| grep nucleus` | Fix `--host`/`--port` |
| Unit failed at startup | Config validation refused an invalid value | `journalctl -u nucleus -n 50` names the key | The server refuses to start on an invalid config on purpose — fix the key |

`nucleus status --host 127.0.0.1:5432` only opens a TCP connection. A green
`status` does **not** mean the engine can serve a query. Prove it with
`psql -c 'SELECT 1'`.

## 2. Writes rejected, reads fine

### SQLSTATE 53100 — disk watermark

The server stops accepting writes *before* the disk fills, rather than failing
mid-write. Defaults:

| Knob | Default | Meaning |
|---|---|---|
| `disk_warn_free_pct` | 10.0 | Logs an operator alert |
| `disk_readonly_free_pct` | 3.0 | **Refuses writes** |
| `disk_min_free_mb` | 256 | Absolute floor; triggers read-only independently, because a percentage is meaningless on a small volume |
| `disk_resume_free_pct` | 6.0 | Must climb back above this before writes resume (hysteresis, so it cannot flap) |
| `disk_check_interval_secs` | 30 | Sample interval; 0 disables the monitor entirely |

Act: free space, do not raise the threshold. Note the resume threshold is
higher than the trip threshold — freeing just enough to clear 3% will **not**
resume writes; you need 6%.

Where the space usually went, in order:

1. **An online backup pinning WAL retention.** While `BACKUP DATABASE TO` runs,
   WAL retention is pinned with **no cap**, so a long backup on a write-heavy
   database grows the WAL for its whole duration. Check for a running backup
   first.
2. **A failing WAL archive.** `truncate_before` never deletes an un-archived
   segment, so a broken or full `NUCLEUS_WAL_ARCHIVE_DIR` stops WAL
   reclamation. This is the correct trade — it protects recoverability — but it
   means a full archive volume eventually fills the data volume.
3. An abandoned open transaction pinning the GC watermark (§3).

### SQLSTATE 53200 — memory ceiling

`server.max_memory_mb` (default **512**) is a shared budget across buffer pool,
cache, KV, FTS and columnar. Exceeding it produces a clean `MemoryExceeded`
rather than an OOM kill. `nucleus_memory_writes_rejected` counts these.

Act: raise `NUCLEUS_MAX_MEMORY_MB` if the host has headroom, or reduce the
query's working set. There is **no spill-to-disk** — the engine materialises
result sets, so a query that does not fit in the budget cannot be made to fit
by waiting.

## 3. Database growing without bound / vacuum not reclaiming

Cause: an abandoned `BEGIN` pins the MVCC snapshot horizon, so `VACUUM` cannot
remove any version newer than it.

```bash
curl -s http://127.0.0.1:9100/metrics | grep nucleus_open_transactions
```

`idle_in_transaction_timeout_secs` defaults to **0, which disables it**. On any
long-running instance, set it:

```bash
# nucleus.toml, [server]
idle_in_transaction_timeout_secs = 300
```

Then `VACUUM` (or `VACUUM <table>`) to reclaim.

## 4. Slow startup

The server replays the WAL before it binds the listener. A long replay means
either a large WAL (§2) or an unclean stop.

```bash
journalctl -u nucleus -f
```

Contributors, and what to do:

- **SIGKILL during the shutdown flush.** The drain budget is a hard 2 s; the
  flush that follows is unbounded and scales with buffer-pool size. If
  `TimeoutStopSec` (120 s in the shipped unit) is too short for your buffer
  pool, systemd kills mid-flush and the next start pays for it in recovery.
  Raise it rather than accepting slow starts.
- **A checkpoint that never ran.** `wal.checkpoint_interval_secs` defaults to
  300.
- k3s only: `startupProbe` allows 60 × 5 s = 5 minutes before Kubernetes gives
  up and restarts the pod — which starts recovery over. Raise
  `failureThreshold` before assuming a crash loop is an engine bug.

## 5. Corruption or a WAL that will not replay

The WAL is CRC-protected per record; a truncated tail is discarded at the first
bad CRC, which is normal after a power loss and is **not** corruption.

Genuine corruption — a page that does not decode, a manifest checksum failure —
is a restore, not a repair:

1. Stop the server. Do not restart it repeatedly; each attempt can extend the
   damage.
2. **Copy the whole data directory somewhere else before touching it.** It is
   the only evidence and possibly the only copy.
3. Restore per [BACKUP_RESTORE_PITR.md](BACKUP_RESTORE_PITR.md), or recover to
   just before the incident with `nucleus restore-pitr --time`.

## 6. Wrong results rather than errors

Check this list before assuming an engine bug — each is a documented,
deliberate boundary:

- **After any crash, the FTS index is suspect.** `fts_index.json` is rewritten
  whole with no temp+rename and no fsync, and a parse failure on load is
  *swallowed* — so the server starts with a stale index and says nothing.
  Rebuild it.
- **Datalog facts, sparse vectors and tensors do not survive a restart at
  all.** Writes are acknowledged and lost, with no error. This is not a bug
  report; see `docs/MODEL_SEMANTICS.md`.
- **A transaction spanning the SQL WAL and a model WAL is not crash-atomic.**
  There is no shared commit record. In-process rollback works; a crash in the
  middle does not.
- **Geo has no persistent state** — the functions are computational only.

## 7. `transaction ID space exhausted`

64-bit transaction IDs, monotonic, never wrapping into reserved IDs. On
exhaustion, new transactions fail with
`transaction ID space exhausted; restart from a fresh logical backup`.

There is no in-place freeze/wraparound-vacuum path. Recovery is
`nucleus dump` → `nucleus load` into a fresh data directory — and remember the
dump is data-only, so re-apply roles, RLS policies, views and sequence state
afterwards ([ROLLBACK.md §2.1](ROLLBACK.md#21-if-the-only-pre-upgrade-artifact-is-a-logical-dump)).

## 8. What to collect before restarting

A restart destroys most of the evidence. Collect first:

```bash
journalctl -u nucleus --since '2 hours ago' > /tmp/incident-journal.txt
curl -s http://127.0.0.1:9100/metrics     > /tmp/incident-metrics.txt
ls -la /var/lib/nucleus /var/lib/nucleus/nucleus.wal.d > /tmp/incident-files.txt
df -h /var/lib/nucleus                   >> /tmp/incident-files.txt
nucleus version                          >> /tmp/incident-files.txt
```

The WAL directory listing matters: segment count and sizes are what
distinguish "the WAL grew because of a pinned backup" from "the WAL grew
because archiving is broken", and both look identical once you have restarted.
