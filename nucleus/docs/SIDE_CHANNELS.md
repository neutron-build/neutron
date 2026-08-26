# Side Channels

What an observer can learn beyond the data a policy authorizes — the honest
enumeration. RLS and masking protect row **content** and column **values**;
they do not and cannot close every channel that leaks *existence*, *timing*,
or *administrative metadata*. Each section states what is learned, who can
learn it, what is already closed, and where in the source the behavior lives.
Line numbers are from the tree at the time of writing; grep the quoted text if
they have drifted.

The companion doc is `RLS_SECURITY.md` ("Deliberate limitations"), which
covers the same boundary in one paragraph per item. This file is the long
form, with citations.

## Constraint existence (probing)

An INSERT/UPDATE/DELETE authorized by RLS still runs constraint enforcement,
and constraint errors carry information about rows the policy hides.

- **Unique/PK.** A duplicate key error names the constrained columns and the
  offending values — the value is the attacker's own input, but the error
  proves a hidden row already holds it:
  `src/executor/dml.rs:698`, `:715`, `:1027`
  ("duplicate key value violates unique constraint ... = (vals)").
  Probing `INSERT ... VALUES (k)` for candidate keys `k` is a membership
  oracle over hidden key space, one statement per guess.
- **Foreign keys.** Violation errors name both tables
  (`src/executor/dml.rs:1293`, `:1444`, `:1712`), so a write that references a
  hidden parent key reveals whether that key exists — and the error itself
  reveals that the FK relationship exists.
- **What is closed.** Row *content* never appears in a constraint error, and
  the RLS `WITH CHECK` runs before the write lands, so probing cannot create
  rows the policy forbids (`src/executor/mod.rs:3945`, `enforce_rls_new_row`).
  The existence channel itself is accepted as-designed, matching PostgreSQL
  (`RLS_SECURITY.md:303`).

Mitigation is operational, not engine-side: rate-limit writes, or keep secret
key spaces out of constrained columns. A per-principal write rate limit does
not exist today.

## Timing

- **RLS filtering cost scales with total rows, not visible rows.** Policies
  are evaluated per candidate row on scan and DML paths:
  `Executor::rls_allows_row` (`src/executor/mod.rs:3927`) →
  `RlsPolicy::check_row` (`src/security/mod.rs:575`) →
  `RlsPredicate::evaluate` (`src/security/mod.rs:368`); call sites include
  `src/executor/dml.rs:506` and `:2157`. A query over a protected table
  therefore takes time proportional to rows the session cannot see, which is a
  row-count oracle for anyone who can also insert known volumes of their own
  rows. There is no padding or batched evaluation.
- **Authentication timing distinguishes valid usernames.** An unknown user
  fails immediately (`src/wire/mod.rs:202-206`: `scram_credentials` returns
  `None`, error without proof work); a known user with a wrong password runs
  the full PBKDF2/SCRAM proof (`src/wire/mod.rs:176-178`) before failing. The
  error *message* is uniform (`InvalidPassword` either way) — the latency is
  the channel. Mitigations that exist: per-source-IP lockout at 5 failures /
  30 s (`src/wire/mod.rs:235-237`), and every failed attempt is audited with
  the source address (`RLS_SECURITY.md` "Security audit log").
- **What is closed.** `SHOW TABLE STATS` — raw planner statistics, i.e. exact
  row/null counts — is refused outright while RLS is active for the table
  (`src/executor/mod.rs:5989-5996`). EXPLAIN output is content-checked by the
  adversarial suite so no hidden row's value reaches it
  (`src/executor/tests/test_rls_surfaces.rs:774-794`), but plan shape and
  estimated costs are still visible, which leaks schema shape.

## Administrator surfaces

Superusers bypass RLS entirely (`RLS_SECURITY.md:16-17`;
`src/executor/mod.rs:3660`). These channels matter because they are reachable
WITHOUT that bypass, by any authenticated session, unless noted:

- **`pg_policies`** lists every policy's table and name to any session that
  can query catalog views (`src/executor/mod.rs:9725-9768`). It answers "which
  tables are protected, and what are the policies called" — an inventory of
  exactly the interesting tables. The predicate text is deliberately withheld
  (`qual`/`with_check` render NULL, `src/executor/mod.rs:9763-9764`).
- **`pg_roles`** enumerates roles and password expiry deadlines
  (`src/executor/mod.rs:9496+`, `RLS_SECURITY.md:174-176`). No verifiers are
  exposed — only metadata.
- **Specialty enumeration is closed.** `RETENTION_CHECK` (every protected
  table's name and row estimate) and the rest of the specialty surfaces are
  refused while any RLS policy is active for the principal
  (`src/executor/scalar_fns.rs:6977`, `is_specialty_surface`;
  `RLS_SECURITY.md:217-224`).
- **Masking narrows rather than hides.** A masked column changes the VALUE a
  role sees, not which rows exist, so equality/range predicates over the
  masked output still narrow row existence (`RLS_SECURITY.md:309-310`). Use
  RLS for row isolation; masking is value redaction only. `SHOW MASKING
  POLICIES` is superuser-only (`src/executor/masking_ddl.rs:199-200`).
- **The audit log** (`<data-dir>/audit/audit.log`) records logins and every
  authority change with actor names (`RLS_SECURITY.md:226-233`). It is a file
  on disk: whoever can read the data directory can read it. That is an
  operator trust boundary, not an SQL one.

## Physical backup

`BACKUP DATABASE TO '<path>'` is a **physical** snapshot: it copies the data
directory — raw pages, WAL, the role catalog with SCRAM verifiers, and the
RLS policies themselves — with no tenant filtering, by design. A restore
yields every row of every table to whoever can open the restored instance.

- The SQL surface is superuser-only: `backup_online_to` starts with
  `require_security_admin("take a physical backup")`
  (`src/executor/logical_dump.rs:1031-1036`; the check is
  `src/executor/admin.rs:27-38`).
- The copy path operates on the raw directory and the engine's backup
  coordinator (`src/executor/logical_dump.rs:1046-1062`,
  `src/backup.rs:440+`); no executor policy evaluation happens anywhere in it.
  This is stated as intended behavior in `RLS_SECURITY.md:305-306`.
- The CLI (`nucleus backup`) is offline and refuses a live data directory
  without an explicit override (`src/backup.rs:447-467`) — its boundary is
  filesystem access, not SQL roles.

Contrast: **logical** exports (`COPY TO` and the `COPY (query)` shapes) go
through the executor and DO filter by the session's policies
(`RLS_SECURITY.md:77`). If a tenant-filtered extract is what is wanted, that
is the path; a physical backup is a disaster-recovery artifact, not an export.

## What this file is for

A reader deciding whether Nucleus's RLS is safe for a multi-tenant deployment
should be able to answer "what still leaks" from this page instead of
discovering it in production. Items here are either accepted (with the
reason), closed (with the pin), or operational (whose responsibility they
are). Anything new in the first category should get a line here when found.
