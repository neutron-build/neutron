# Loopback data reference UI

This is a local operator fixture using Neutron SSR loaders/actions. One private
server token assigns every browser request to one configured API tenant. It has
no public multiuser sign-in and must remain bound to loopback. Never expose it
as an authenticated public application.

The server reads `NEUTRON_SERVICE_API_URL` (injected by the application's API
service dependency) and `DATA_WEB_API_TOKEN` from private environment variables.
The API origin must be HTTP loopback with no embedded credentials. The token
must be an operator/test-harness-issued API JWT with the reviewed issuer,
audience, subject and tenant claims. There is no web token issuer or browser
credential storage. Do not use a `VITE_` variable or put tokens in source/forms.
Only the `.server.ts` loader/action helper sends Authorization to the API.
Requests require a literal loopback hostname; mutations additionally require an
exact same-origin Origin header. The default server binds 127.0.0.1 and rejects a
nonloopback NEUTRON_HOST. These are local fixture boundaries, not public user
authentication.

Use normal package installation for an independent consumer, then `npm run
typecheck`, `npm run build`, and `npm run start` in this directory. This candidate
uses the repository-relative core/CLI packages. Validation reused already
installed dependencies through private links and freshly compiled candidate
core/CLI copies; no published-package equivalence is claimed.

At the application root, the new `neutron.toml` declares API and web with the native
neutron/v1 supervision profile. The coordinator supplies their loopback addresses, waits
for API readiness, then supplies NEUTRON_SERVICE_API_URL to web. Configure the
private API role map/JWT secret/error log and the web token before `neutron dev`.
Database setup is an explicit operator prerequisite: provision reviewed roles,
then run `neutron project run migrate` with DATABASE_URL set only for that task
to the migration-owner URL. Never carry that owner credential into runtime role
configuration. The coordinator does not run migrations automatically.
The API serves its reviewed static `/openapi.json`; the web enables the native
SSR discovery response and `/docs` through server.openapi. These remove the
missing discovery warnings. This fixture does not claim automatic raw-handler
schema generation or complete framework-contract compliance.

The worker currently emits a readiness line rather than serving a probe. The
coordinator understands HTTP/TCP probes, not that line, so the manifest honestly
provides `neutron project run worker-once` as a finite task. Supply private
WORKER_DATABASE_URL/WORKER_TENANT and the validated Python package/interpreter.
For polling, supervise `python main.py` separately in `../worker` and inspect its
startup result. No fake ready native service or HTTP worker contract is declared.
The complete coordinator config requires the separately reviewed worker source.

The UI lists projects and scoped document pages (50-row exclusive UUID cursors),
creates projects/documents, inspects optional job/results, and submits note CAS
against the displayed version. Amounts, versions, bytes/base64, nullable notes
and UTC microsecond timestamps remain strings; no floating point conversion.
NULL and empty text are visibly distinct. Form actions call the API once. Same
UUID/key/payload performs an explicit replay; different payload conflicts. The
submitted document form remains available after action results so a conflict or
unknown outcome does not silently replace its key. Inspect durable state first;
manual replay of the same request is distinct from starting a new request. A
stale note update reports conflict and keeps the submitted expected version;
an explicit inspection link refreshes current state. No automatic retry or
version replacement occurs. Unknown outcomes display safe guidance.
Form submission renders a result message within the SSR page; its status field
is a UI outcome, not a replacement HTTP API status contract.

API wire shapes in api-types.ts are generated from the one reviewed static
`../api/openapi.json` by `npm run generate:api`; `npm run check:api` detects
stale output and runs before npm typecheck. models.ts contains local UI state.
This bounded generator produces types, not runtime validation or typed handlers. The full three-language workflow, release/restore,
public authentication and shipping gates are separate from this bounded UI.
