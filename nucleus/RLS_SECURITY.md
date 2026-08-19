# Row-Level Security

Nucleus row-level security (RLS) is enforced by the executor against a catalog-backed,
authenticated session principal. It is not derived from client-writable SQL settings.

## Trust boundary

- PostgreSQL wire logins use per-role SCRAM-SHA-256 verifiers stored in the durable role catalog.
- The authenticated login role remains attached to the connection for its lifetime.
- `SET ROLE` is allowed only for the login role itself, recursively granted role memberships, or
  a superuser. Membership is revalidated on every statement, so revocation affects live sessions.
- `SET SESSION AUTHORIZATION` cannot impersonate another user unless the authenticated login is a
  superuser.
- `nucleus.tenant_id` cannot be set through SQL. A trusted proxy or authentication boundary must
  install it through the server API.
- `SUPERUSER` and catalog-backed `BYPASSRLS` roles bypass RLS. There is no implicit table-owner
  bypass.

The server's multi-user mode is SCRAM-only. The explicitly unauthenticated embedded/server mode
retains the trusted `nucleus` bootstrap identity and is not a tenant isolation boundary.

## Policy DDL

```sql
CREATE POLICY tenant_read ON invoices
  AS RESTRICTIVE
  FOR SELECT
  TO app_user
  USING (tenant_id = current_setting('nucleus.tenant_id'));

CREATE POLICY tenant_write ON invoices
  FOR ALL
  TO app_user
  USING (tenant_id = current_setting('nucleus.tenant_id'))
  WITH CHECK (tenant_id = current_setting('nucleus.tenant_id'));

ALTER TABLE invoices ENABLE ROW LEVEL SECURITY;
DROP POLICY tenant_read ON invoices;
```

Policy DDL requires superuser authority. It participates in `BEGIN`/`COMMIT`/`ROLLBACK` and SQL
savepoints, is persisted atomically with executor metadata, migrates on table rename, and is removed
on table drop. In a cluster, security catalog changes must be submitted to the leader and committed
through Raft before they are applied.

Supported policy expressions deliberately compile to a small auditable predicate representation:
boolean constants, `NOT`, `AND`, `OR`, equality between a column and a string/number/boolean,
`CURRENT_USER`/`SESSION_USER`, `current_setting('nucleus.tenant_id')`, `has_role('role')`,
ordering comparison between a column and a literal (`<`, `<=`, `>`, `>=`, `<>`),
`column IN (literal, …)`, and `column IS [NOT] NULL`.
Unsupported expressions are rejected instead of being accepted without enforcement.

Two properties of the comparison forms are load-bearing rather than incidental:

- **Comparison is numeric when both sides are numeric.** The predicate row map is
  stringly-typed, so a lexical compare would make `"9" > "100"` hold and admit a
  row that `amount > 100` excludes. Non-numeric operands keep lexical order,
  which is also chronological order for the ISO-8601 date and timestamp forms.
- **A NULL column denies every comparison.** NULL is represented by absence from
  the predicate row map, never by the string `"NULL"` — which is what the text
  value `'NULL'` also renders to, and which would compare greater than most
  numbers lexically. `IS NULL` reads that absence; every other form denies,
  matching SQL's rule that a comparison with NULL is unknown and unknown never
  grants.

## Enforcement semantics

- An RLS-enabled table with no applicable permissive policy returns no rows and rejects new rows.
- Permissive policies combine with OR; at least one must pass. Restrictive policies combine with
  AND and can only narrow access.
- `USING` filters existing rows for `SELECT`, `UPDATE`, and `DELETE`.
- `WITH CHECK` validates proposed rows for `INSERT` and `UPDATE`; when omitted, the applicable
  `USING` predicate is reused.
- Filtering occurs before joins, subqueries, CTE consumers, aggregates, and projections.
- Query/result caches and raw index, columnar, large-object, and wire point-query shortcuts are
  bypassed while RLS is active unless they enter the policy-aware path.
- `COPY TO` filters exports and `COPY FROM` applies write checks. Foreign-key cascades apply old-row
  and new-row policy checks.
- Specialty SQL surfaces without a policy-aware representation—including document, graph/Cypher,
  Datalog, KV, time-series, vector search/mutation, CDC, version/branch, tensor, encrypted-index,
  stream, blob, sparse, procedure, and pub/sub functions—fail closed while RLS is active.
- Materialized views fail closed for RLS sessions because their stored rows do not retain invoker
  policy provenance. Ordinary views re-execute under the invoking session.
- Follower reads execute through the same policy-aware executor. RLS-protected client writes are not
  SQL-forwarded to a leader because that transport cannot preserve the authenticated principal.
- Principal-less cluster query/DML messages and the binary protocol's SQL execution path fail closed
  whenever committed RLS policy exists; neither may silently execute as the bootstrap superuser.

## Adversarial surface matrix

`src/executor/tests/test_rls_surfaces.rs` is the attack suite behind the M5 exit
gate. For a table with one hidden row it attempts exfiltration through every
alternate path and fails if the hidden row's content escapes: scan fast paths
(index point lookup, index-only, SIMD, top-k, negated predicate, OFFSET),
aggregates and window functions, set operations (UNION/INTERSECT/EXCEPT),
CTEs, correlated and nested subqueries, `NOT IN`/`NOT EXISTS` probes, streaming
operators (scan, aggregate, distinct, join, sort), all five COPY export shapes
(text, CSV, binary, column subset, `COPY (query)`), write-path echoes
(`INSERT..SELECT`, `RETURNING` on hidden rows, upsert `ON CONFLICT DO UPDATE`,
`COPY FROM` write checks), views and materialized views, cache and prepared-plan
reuse across principals, specialty indexes over protected tables (vector KNN,
text search), diagnostics (`EXPLAIN`, `EXPLAIN ANALYZE`), constraint and
foreign-key cascade paths, and trigger bodies. The core attack set runs against
**all five storage engines** (memory, MVCC, columnar, LSM, disk), since each
implements its own scan and lookup paths.

Three defects that matrix found, all fixed:

- **Subquery identity loss.** Correlated subqueries are evaluated per row from
  a synchronous context through `sync_block_on`, which drives the future as a
  new tokio task. Task-locals are per-task and are not inherited, so the
  subquery lost `CURRENT_SESSION` and fell back to the bootstrap superuser
  session — executing with RLS fully bypassed, and with the wrong storage
  session (so it could also read past its own transaction). `sync_block_on`
  now re-establishes both scopes inside the new task. The regression pin reads
  the principal and the visible-row count from *inside* a subquery, so it fails
  on any recurrence rather than only when a hidden value reaches the projection.
- **Identity functions ignored the session.** `CURRENT_USER`, `CURRENT_ROLE`,
  and `SESSION_USER` returned the constant `nucleus` for every session. Policy
  predicates were unaffected (the policy compiler resolves the principal
  separately), so this was a wrong-answer bug rather than a bypass, but any
  client asking "who am I" got the bootstrap name. They now report the session
  principal: `SESSION_USER` is the authenticated login role, `CURRENT_USER` and
  `CURRENT_ROLE` the effective role after `SET ROLE`.

- **Schema-qualifying defeated the specialty fail-closed guard.** The guard
  that refuses specialty-store functions while RLS is active read the raw
  function name, and the `PG_CATALOG.` prefix strip ran AFTER it. So
  `kv_set(...)` was correctly denied while `pg_catalog.kv_set(...)` did not
  match the `KV_` prefix list, passed the check, and only then had its
  qualifier removed — executing normally. That is a one-token bypass of every
  specialty surface, and reachable by ordinary clients rather than only an
  attacker, since psql and ORMs schema-qualify builtins as a matter of course.
  The strip now runs BEFORE any policy decision, so every check sees the same
  canonical name the dispatcher executes. Pinned by
  `schema_qualifying_a_specialty_call_does_not_bypass_the_fail_closed_guard`,
  which also asserts an ordinary qualified builtin (`pg_catalog.upper`) still
  resolves, so the fix cannot be "corrected" into breaking psql.

Not covered by the in-process matrix: replica/follower reads (needs a live
cluster) and the wire-level protocol surfaces, which `compat/` covers
separately.

## Password lifecycle

Roles carry a SCRAM-SHA-256 verifier and an optional deadline:

```sql
CREATE ROLE app_user LOGIN PASSWORD 'secret' VALID UNTIL '2027-01-01 00:00:00';
ALTER ROLE app_user PASSWORD 'rotated';           -- replaces the verifier
ALTER ROLE app_user VALID UNTIL '2028-01-01';     -- moves the deadline
ALTER ROLE app_user VALID UNTIL 'infinity';       -- removes it
ALTER ROLE app_user PASSWORD NULL;                -- removes the credential
```

Enforced properties:

- **A raw password is never retained.** `store_password_literal` encodes a SCRAM
  verifier; an already-encoded verifier is stored verbatim so a logical dump
  round-trips credentials without re-hashing them.
- **The deadline is checked at both authentication gates**, not only beside the
  password: `scram_credentials` (the SCRAM path) and
  `bind_authenticated_session` (every authenticated session, including paths
  that never ask for a verifier). A check that lives only next to the password
  covers only the password.
- **The deadline is a moment, not a flag.** A live role stops authenticating
  when it passes, with no statement having run.
- **An unparseable `VALID UNTIL` fails the statement.** It does not create a
  role whose expiry silently did not apply, which is worse than an error
  because it looks like it worked.
- **It survives a restart and a dump.** `RoleSer` persists it (defaulting to
  "no expiry" for metadata files written before the field existed) and
  `CREATE ROLE` renders it.
- **`pg_roles.rolvaliduntil` and `pg_user.valuntil` report it.** Both columns
  existed and returned NULL for every role, which reads as "no role in this
  database has an expiry".
- **Lockout and rate limiting are per source IP**, checked BEFORE the credential
  is verified (`wire::LoginRateLimiter`), so a locked-out address is refused
  even with correct credentials.
- **Credentials are scrubbed from logged SQL** by `ops::redact::redact_sql`
  before any statement text is logged.

`src/executor/tests/test_password_lifecycle.rs` is the adversarial suite:
expired versus unexpired as controls in the same test, both gates checked
independently, expiry-versus-NOLOGIN reported as distinct denials, rotation
replacing the verifier, and the deadline surviving a restart.

The defect the suite was written around: `VALID UNTIL` parsed, succeeded, and
was **discarded** — `CreateRole::valid_until` and `RoleOption::ValidUntil` both
fell through unmatched arms, so an expired role authenticated indefinitely and
the catalog views said no role had a deadline. Same class as `FOR UPDATE SKIP
LOCKED` being parsed and never read: a clause carrying a guarantee, accepted
and dropped.

Deliberate, matching PostgreSQL: expiry applies at **login**. It does not
terminate sessions that are already connected, and it does not block `SET ROLE`
into the role — an expired role still exists, still owns its objects, and can
still be granted to. Only its ability to authenticate lapses. There is no
password history, no complexity policy, and no forced-rotation interval;
enforcing those belongs to whatever provisions the roles.

## Cluster trust boundary

Node-to-node traffic — the Raft transport and replication, which share one
`InternalTlsConfig` — is **mutual TLS**. `NUCLEUS_INTERNAL_TLS=1` requires
`_CERT`, `_KEY` and `_CA`; each node presents its certificate and requires its
peer to present one the CA signed, in both directions.

Both halves were previously absent. The acceptor was built with
`with_no_client_auth()`, so a node served any TLS client that reached it, and
the connector presented no certificate, so there was nothing to check even if a
peer had looked. The CA was used only to verify the server side. Node identity
therefore rested entirely on `NUCLEUS_CLUSTER_TOKEN`: one bearer secret, held
by every node, which anyone who learns it can replay.

`tls::mtls_tests` runs real handshakes over loopback rather than inspecting
configuration: a CA-signed peer connects, a peer with **no** certificate is
refused, a peer with a certificate from **another** CA is refused, and a
*server* whose certificate the CA did not sign is refused by the client — so a
rogue listener on a peer's address cannot collect replication traffic. With the
verifier removed, the two refusal tests fail, which is how they were checked.

`NUCLEUS_CLUSTER_TOKEN` remains and is still enforced at connection setup; mTLS
is a second, cryptographic factor rather than a replacement. The startup guard
that refuses a non-loopback cluster transport with no token is unchanged.

**Not closed by this**: message-level node identity is still self-asserted. A
peer that completes the handshake may claim any `node_id` in the envelopes it
sends, because the configuration does not express per-node certificate identity
— `NUCLEUS_INTERNAL_TLS_SERVER_NAME` is a single cluster-wide name, so
certificates are not expected to carry a per-node subject. Binding a claimed
`node_id` to the peer certificate needs that convention decided first; it is
filed in `OPEN_WORK.md` rather than half-built here. The boundary this closes
is admission to the cluster, which is what the shared token was carrying alone.

## Deliberate limitations

- Constraint errors (for example unique and foreign-key checks) can reveal that a hidden key exists,
  as in PostgreSQL. RLS protects row contents, not all timing or existence side channels.
- Physical backup, restore, and replication are privileged administrative surfaces over raw storage;
  they are not tenant-filtered exports.
- Column masking has a persisted internal policy engine but no SQL DDL or executor enforcement yet.
  Do not rely on column masking for isolation.
- Specialty stores have their own data and policy boundaries. RLS prevents SQL/binary cross-model
  bypasses; it does not reinterpret relational table policies as RESP KV-key or graph-edge policies.
