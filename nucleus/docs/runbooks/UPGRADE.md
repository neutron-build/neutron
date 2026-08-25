# Runbook — upgrade

Single node, in place, with an outage. There is no rolling upgrade: that needs
a cluster, and distributed mode is unsupported (see
[`README.md`](README.md#runbooks-that-are-deliberately-absent)).

## Status of this procedure

**Written, never executed.** Nucleus has had one release (0.1.0) and the
versioned-asset release path landed with this milestone, so no two releases
have yet been installed on the same host. The steps below follow the on-disk
compatibility rules the engine actually enforces; treat the first real upgrade
as the test of this document and correct it afterwards.

## 1. Decide whether the upgrade is reversible

This is the only question that matters, and you answer it *before* upgrading.

Physical snapshots are keyed on **`format_version`**, not the release string.
So:

- **Same `format_version` in old and new:** rollback is a binary swap.
  Cheap and safe.
- **New `format_version`:** the new server rewrites the data directory into a
  format the old binary will refuse to open. **Rollback then requires a
  logical dump taken before the upgrade** — and `nucleus dump` omits roles,
  RLS policies, views and sequence state, so a rollback across a format change
  loses those unless you have captured them separately (§3).

Read the release notes for the target version. If they do not state the
`format_version`, assume it changed and take the logical dump.

## 1b. FTS indexes need rebuilding when the English stemmer changes

**Applies to any upgrade crossing the A-014 stemmer fix.**

An FTS index stores **stemmed** terms, and queries are stemmed with the same
rules before lookup. Change the rules and the two sides disagree: the index
holds terms produced by the old stemmer while queries produce new ones, so
affected searches silently return fewer rows — no error, no warning.

The A-014 fix changed English stems for the whole `-er` noun class (`number`
now stems to `number`, not `numb`), for `-eed`/`-ed` (`seed` stays `seed`), and
for short `-ly` words (`reply` stays `reply`). Any English FTS index built
before it must be rebuilt.

There is **no `REINDEX` statement and no CLI repair command** (see
`DATABASE_COMPLETION.md`), so rebuilding means dropping and recreating the
index:

```sql
DROP INDEX <name>;
CREATE INDEX <name> ON <table> USING FTS (<column>);
```

Do it in the maintenance window, before reopening to traffic — an index that is
half old-stem and half new-stem is worse than either, because which rows a
query finds then depends on when they were written. Non-English indexes are
unaffected; the other language stemmers did not change.

## 1c. ReplacingMergeTree tables: aggregates will change value

**Applies to any upgrade from v0.1.8 or earlier, on any database holding a
`replacing_mergetree` table.**

Before this fix, a table's `WITH (engine = …)` declaration was durable only in
the `engines.json` sidecar, which was added part-way through the product's life.
A table created before it had no durable record of its engine at all, nothing
re-registered it at boot, and read-time dedup was therefore skipped — silently.
Every `COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX` and `GROUP BY` over such a table
counted **every superseded version**. On the instance this was found on, a
window whose raw events proved 72 pageviews reported 158.

`CREATE TABLE IF NOT EXISTS … WITH (engine = …)` on an already-existing table
was also a no-op, so a migration could not repair one either.

### What happens on the first boot after the upgrade

Nothing is required of you. Two automatic paths repair it:

- Boot reconciliation recovers any `engines.json` declaration into `catalog.json`
  (which is now the durable record) and registers read-time dedup for it.
- `CREATE TABLE IF NOT EXISTS … WITH (engine = …)` now **adopts** the declaration
  for an existing table, so any application that re-runs its migrations on start
  repairs its own tables. This is the path that fixes a table with no
  `engines.json` entry at all.

Adoption is metadata only. **No rows are moved, rewritten or deleted.**

### What to expect

1. **Aggregates return smaller, correct numbers**, and `SELECT *` returns fewer
   rows — one per ORDER BY key. Anything baselined against the old numbers
   (dashboards, alert thresholds) shows a step change. Re-baseline them.
2. **`FINAL` is now a statement error** rather than a silently ignored table
   alias. Grep your queries for it before upgrading. Nucleus collapses replacing
   tables on every read, so the modifier has nothing to select — remove it.
   A quoted `AS "FINAL"` alias is still an alias.
3. **Two new WARN lines matter on first boot:**
   - `declared engine has no per-table storage on disk — the table's rows are in
     the default engine and are left there`. Correct and deliberate: the table
     predates per-table engine routing, so its rows are in the heap. Reads are
     collapsed for it, but the aggregate fast paths and the pushed-down LIMIT
     and projection are disabled, so **large scans of that table get slower**.
     To undo that, rebuild the table onto the columnar engine: create it afresh
     `WITH (engine='replacing_mergetree') ORDER BY (…)` under a new name,
     `INSERT … SELECT` across, then rename. There is no in-place migration, on
     purpose — moving a multi-gigabyte table between engines during boot is not
     something to do unasked.
   - `ORDER BY names a column the table does not have`. The declaration is
     wrong and dedup is **not** registered for that table. Fix the schema.
4. **Rollback stays a binary swap.** `catalog.json` gains a serde-defaulted
   `table_engines` array that an older binary ignores. Rolling back to v0.1.8
   reverts to the over-reporting behaviour; it does not corrupt anything.
5. **Do not hand-edit `engines.json`.** It is now a cache of `catalog.json`.

## 1a. Container images: check who owns the data directory

**Applies to every upgrade crossing v0.1.1 → v0.1.2 or later.** Skip only if
you already run v0.1.2+.

The image runs as **uid 10001** from v0.1.2 onward. v0.1.0 and v0.1.1 ran as
root. Nothing re-owns the data directory on upgrade, and the `chown` in the
Dockerfile happens at build time — it covers a named volume only while that
volume is still empty. So a directory written by the old image is owned by
root, and the new process cannot open it.

Left unhandled this is a restart loop, not a clean failure: before this was
caught the engine panicked inside the storage open (exit 101, no mention of
permissions) and the orchestrator restarted it forever. Current builds refuse
to start with the fix printed, but the work below is still yours to do.

```bash
# Host bind-mount: re-own before starting the new image.
chown -R 10001:10001 /var/lib/nucleus

# Docker named volume: --entrypoint, because the image's entrypoint is `nucleus`.
docker run --rm -u 0 --entrypoint chown -v nucleus-data:/data \
    ghcr.io/neutron-build/nucleus:latest -R 10001:10001 /data

# Confirm.
docker run --rm -v nucleus-data:/data ghcr.io/neutron-build/nucleus:latest \
    status --host 127.0.0.1:5432 || true
```

On Kubernetes, `deploy/k3s/nucleus.yaml` already sets `runAsUser: 10001` and
`fsGroup: 10001`; `fsGroup` re-owns the volume for you on most CSI drivers, but
it does **not** apply to a `hostPath` volume — chown those by hand.

Substitute your own uid:gid if you override the image's user.

## 2. Pre-flight

```bash
# 1. Record what you are on.
nucleus version
nucleus status --host 127.0.0.1:5432

# 2. Verify the new binary runs at all, without touching production data.
./nucleus-new version
./nucleus-new start --memory --port 55432 &
psql -h 127.0.0.1 -p 55432 -c 'SELECT 1'
kill %1

# 3. Take a backup of the running server and PROVE it restores.
psql -c "BACKUP DATABASE TO '/backups/pre-upgrade-$(date +%F)'"
./nucleus-new restore --input /backups/pre-upgrade-$(date +%F) --data /tmp/upgrade-rehearsal
./nucleus-new start --data /tmp/upgrade-rehearsal --port 55433 &
psql -h 127.0.0.1 -p 55433 -c 'SELECT count(*) FROM <largest table>'
kill %1
```

Step 3 is the whole pre-flight. Restoring the pre-upgrade snapshot **with the
new binary** is what tells you whether the new version can read your data,
before it has the chance to rewrite it.

## 3. Capture what a logical dump loses

Only needed when `format_version` changes (§1). `nucleus dump` is data-only.
Capture these separately, or a rollback silently drops your security model:

```bash
psql -Atc "SELECT * FROM pg_roles"                > /backups/pre-upgrade-roles.txt
psql -Atc "SELECT * FROM pg_policies"             > /backups/pre-upgrade-policies.txt
psql -Atc "SELECT * FROM pg_views"                > /backups/pre-upgrade-views.txt
nucleus dump --data /var/lib/nucleus -o /backups/pre-upgrade.sql
```

Sequence state is the one that bites: a logical restore emits
`DEFAULT nextval(...)` without creating the sequence, so `SERIAL` inserts fail
on the restored table until you recreate the sequence at the right value.
Record current sequence values now.

## 4. Upgrade

```bash
systemctl stop nucleus
# Confirm a clean stop — a SIGKILL during the flush means a longer recovery
# on first start of the NEW binary, which is exactly when you least want
# ambiguity about whose fault a slow start is.
journalctl -u nucleus -n 30 --no-pager | grep -i 'drain\|flush\|shutdown'

install -m 0755 nucleus-new /usr/local/bin/nucleus
nucleus version                       # confirm the swap took

systemctl start nucleus
```

## 5. Verify

```bash
systemctl is-active nucleus
journalctl -u nucleus -f              # watch recovery complete
nucleus status --host 127.0.0.1:5432

psql -c 'SELECT 1'
psql -c '\dt'                          # tables present
psql -c 'SELECT count(*) FROM <largest table>'    # row counts match §2
psql -c '\du'                          # roles survived
psql -c 'SELECT * FROM pg_policies'    # RLS policies survived
```

RLS is the one to check explicitly. A policy that fails to load is a silent
data-exposure change, not an error.

Then run your own read and write smoke test against the specialty models you
use. `docs/MODEL_SEMANTICS.md` lists which models are durable at all — do not
assume a model survived a restart just because the server started.

## 6. First 24 hours

Watch for:

- **Slow startup on subsequent restarts** — a format migration can leave a
  large WAL to replay once.
- **Metrics gaps.** If you scrape `http://127.0.0.1:9100/metrics`, confirm the
  counters you alert on still exist under the same names. They are not a
  stability contract.
- **Config keys that were removed or renamed.** The server validates its
  config at startup and refuses to start on an invalid one, so this shows up
  immediately rather than as drift — but check
  [`docs/CONFIG_REFERENCE.md`](../CONFIG_REFERENCE.md) for the new version.

## 7. If it goes wrong

Go to [ROLLBACK.md](ROLLBACK.md). Do not attempt to repair a half-migrated
data directory in place.
