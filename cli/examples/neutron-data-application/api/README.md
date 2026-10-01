# PostgreSQL reference API

Provision schema revision 1 and the finite tenant roles using the separate
`../database/provision.py` operator step before starting this service. The API
never migrates a database. PostgreSQL 17 is the tested profile; Nucleus is refused.

From this directory run `go run .`. The module uses the candidate SDK through
`replace github.com/neutron-build/neutron/go => ../../../../go`. Supply private
configuration through the process environment:

- `DATA_API_TENANTS`: JSON object with `tenant-a` and/or `tenant-b`, each containing
  `url` (its API-role PostgreSQL URL) and `role` (the expected login role from the
  provisioning manifest). Never use the migration owner URL here.
- `DATA_API_JWT_SECRET`: at least 32 random bytes of secret material. Generate
  demo tokens only in the operator/test harness. There is no token mint endpoint.
- `DATA_API_ERROR_LOG`: private log file path. The service creates it mode 0600
  and refuses a file readable by group/others. Native errors remain private;
  configured URLs, passwords and JWT secret are redacted.
- `DATA_API_REQUEST_TIMEOUT_MS`: optional, 100–30000, default 5000. Each tenant
  pool has at most two connections. Connections have a 3s connect timeout;
  PostgreSQL statement, lock and idle-transaction limits are 5s, 2s and 5s.
- `NEUTRON_HOST` / `NEUTRON_PORT`: existing Neutron service bind settings.

Every new connection verifies current/session role against the configured
allowlist, nonprivileged flags, absence of role memberships and ownership,
PostgreSQL 17, exact schema revision 1 and enabled/forced RLS on all five managed
tables. Request headers, bodies and URLs cannot select a connection string or
role. JWT middleware verifies HS256/expiration, then application policy requires
issuer `neutron-data-reference`, audience `neutron-data-reference-api`, a nonempty
subject and an exact configured `tenant_id`. Explicit request tenant values must
match that principal. `/health` is unauthenticated readiness after startup.

The endpoints are `POST/GET /api/projects`, `POST/GET /api/documents`,
`GET /api/documents/{id}`, and `POST /api/documents/{id}/note`. Lists return at
most 50 rows ordered by UUID, with an exclusive `after` UUID cursor; document
lists require `project_id`. Missing document detail returns 404. The shared
application contract defines the bodies. JSON objects reject duplicate/unknown
fields and trailing input, with a 1MiB HTTP body budget. `note` is required and
nullable; empty string and NULL remain distinct. Amount accepts at most 22
integer and 18 fractional digits and is normalized to 18 places without floats.
UUIDs normalize to lowercase ASCII form, byte payloads to padded standard
base64, versions remain int64 decimal strings, timestamps UTC with six fractional
digits. Content is immutable UTF-8 up to 65536 bytes.

Document creation atomically inserts document, immutable request and pending
job. Digest input has fixed keys in this order: id, project_id, content, amount,
note, payload, encoded by Go encoding/json.Marshal with no trailing newline;
UUID/amount/base64 values are normalized first. The tenant and
idempotency key scope the request independently. Reusing a key with the same
digest returns its original document; a different digest returns 409. A
concurrent uniqueness loser rolls back before reading the winning request. Note
updates use one conditional SQL UPDATE against the expected version; a stale or
exhausted version returns 409. No automatic write/commit retry occurs. A
confirmed server commit refusal returns a dependency error with a known aborted
outcome. Commit response loss or transport error returns safe `unknown-outcome`
problem details, requiring the operator or
caller to resolve the request by its idempotency key before choosing another
action. A same-payload POST performs that replay lookup; GET by a known document
ID reads current state. There is no separate GET-by-key endpoint.

Business reads/writes use the actual Nucleus SQL/Tx API. Per-connection startup
checks use the existing SDK `WithPoolConfig`/driver `AfterConnect` option because
the SDK has no public checked-out-connection metadata hook. Neutron raw routes
retain required-nullable presence and strict object validation; they do not
claim generated typed-handler OpenAPI coverage. Worker execution, migration,
backup/restore and full application acceptance are separate steps.

`go test ./...` checks exact boundary behavior and authentication. Actual HTTP
and role/oracle tests run against disposable provisioned databases in the private
application evidence harness; ordinary unit tests do not contact a database.
