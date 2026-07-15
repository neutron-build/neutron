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
`CURRENT_USER`/`SESSION_USER`, `current_setting('nucleus.tenant_id')`, and `has_role('role')`.
Unsupported expressions are rejected instead of being accepted without enforcement.

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

## Deliberate limitations

- Constraint errors (for example unique and foreign-key checks) can reveal that a hidden key exists,
  as in PostgreSQL. RLS protects row contents, not all timing or existence side channels.
- Physical backup, restore, and replication are privileged administrative surfaces over raw storage;
  they are not tenant-filtered exports.
- Column masking has a persisted internal policy engine but no SQL DDL or executor enforcement yet.
  Do not rely on column masking for isolation.
- Specialty stores have their own data and policy boundaries. RLS prevents SQL/binary cross-model
  bypasses; it does not reinterpret relational table policies as RESP KV-key or graph-edge policies.
