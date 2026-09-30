# Authentication state and WebAuthn

The Go SDK requires Go 1.26 or later. WebAuthn uses
[`go-webauthn` v0.18.2](https://github.com/go-webauthn/webauthn/releases/tag/v0.18.2)
for parsing and registration/assertion verification. The default policy requires
user verification and requests ES256 credentials with `none` attestation. This
verifies a WebAuthn ceremony; it does not establish hardware provenance or an
attestation trust chain. Applications must establish the authenticated account
before adding a credential, authorize account changes, limit HTTP request bodies,
and rate-limit ceremony endpoints.

## Sessions

`SessionMiddleware` requires `VersionedSessionStore`. A store must atomically
compare the revision loaded by a request and replace, rotate or revoke that
identity. Every replacement needs a fresh, unpredictable revision, including
when an ID is reused after deletion or expiration. A stale request cannot save
its old authenticated state after logout or rotation. This fences persisted
writes; it does not cancel already executing requests or undo their other work.

`NewMemorySessionStore()` supplies a bounded single-process implementation
(default 10,000 records, optional capacity argument). Data is cloned on ingress
and egress. Use JSON-compatible session values; the memory store preserves Go
numeric types, while SQL JSON decoding uses the usual `float64` representation.
Do not put exact large integers in JSON numbers; store them as strings.

`NewSQLSessionStore(ctx, pool)` initializes `neutron_auth_sessions` and uses
conditional single-statement DML through a pgx pool. Existing sessions are
updated with their loaded revision, including atomic ID rotation. A stale update
never falls back to INSERT. Empty revision supports creation under the same ID
only; empty-revision rotation/revocation is refused. Middleware creates a new
identity when no persisted session exists. Applications must bound pool/connect
and query times and protect this table from untrusted database principals.

Legacy `SessionStore` implementations still compile but middleware refuses them
before running the handler. In particular, `NewNucleusSessionStore` is a legacy
KV adapter without compare-and-swap and cannot be used by this middleware.
Migrate to the SQL store, the memory store, or implement the versioned contract
using a genuinely atomic backend operation. Changing stores requires fresh
login; existing unversioned records are not silently promoted to trusted state.

Go handlers call `Save` after changes or `Regenerate` after authentication;
`Destroy` immediately revokes the persisted identity. A persistence failure,
including a stale revision, suppresses the handler's response and cookie and
returns a generic 503 when headers are finalized. Observe failures through
`WithSessionCommitErrorHandler`; do not acknowledge login/logout before
persistence succeeds. As with any HTTP middleware, operations after response
headers were already sent cannot replace that earlier response.

The TypeScript core exports `createSQLSessionStorage(pool, options)` with a
structural node-postgres pool interface; the application installs `pg` if needed.
It defaults persisted lifetime to 24 hours when middleware supplies none.
`sessionMiddleware` requires atomic `commitSession`; old custom stores
fail closed. `createMemorySessionStorage` implements that capability. SQL
middleware commits dirty data automatically; conflicts suppress the original
response/cookie with 503. Go session TTL defaults to 24 hours. Neither language
uses unconditional administrative `Set`/`setSession` in its request path.

Expired rows cannot authenticate but physical SQL retention requires scheduled
maintenance: call Go `SQLSessionStore.PurgeExpired(ctx)` or TypeScript
`SQLSessionStorage.purgeExpiredSessions()`. Go administrative TTL values of zero
mean no expiry; choose a positive TTL for authentication records.

## WebAuthn stores and migration

`NewMemoryWebAuthnStore()` implements bounded, single-process credential and
ceremony storage (default 10,000 entries per collection, optional capacity).
`NewSQLWebAuthnStore(ctx, pool)` initializes private SQL credential and ceremony
tables for shared persistent state. Calls through the legacy store interface
have a five-second database budget. Schedule `PurgeExpiredCeremonies(ctx)` to
remove abandoned expired SQL ceremonies; expired state is always refused.

Custom stores must persist the **entire serialized library SessionData**, enforce
its TTL, and atomically consume it exactly once in `GetChallenge`. Treat state
as opaque JSON, not a raw challenge. The service retains one active registration
and one active authentication ceremony per user: beginning another of the same
kind replaces the previous ceremony. User IDs must be stable, opaque 1–64 byte
account identifiers. Configure the exact relying-party ID and allowed origin;
production browser ceremonies require a secure origin.

Persist the complete `VerifiedCredential` and its `Revision`, not just the old
SEC1 `PublicKey` field. `SaveCredential` must reject duplicate credential IDs.
Authentication requires `AtomicWebAuthnStore.CompareAndSwapCredential`, which
atomically compares the old revision and publishes the entire verified result
with a new revision. Counter clone warnings and failed CAS receipts reject
login. Authenticators that consistently report counter zero follow the
library's WebAuthn counter semantics; zero does not prove absence of cloning.

Old byte-scanned registrations do not contain trustworthy ceremony metadata and
must be re-enrolled through a separately authenticated account-recovery path.
The service refuses such credentials and unversioned stores rather than inferring
trust from an old public key or calling the legacy blind `UpdateSignCount`.
Replay, expired/malformed state, wrong origin/RP ID/challenge, missing required
presence/verification flags, invalid signatures and stale credential revisions
all fail authentication. A failed attempt consumes that ceremony; begin a fresh
one to retry. Protect credentials and ceremony tables with a trusted principal.

Live adapter regressions use `NEUTRON_AUTH_TEST_DATABASE_URL`; setting
`NEUTRON_LIVE_REQUIRED=1` makes a missing URL an error. TypeScript live tests also
need application-provided `pg`. Each fixture deletes only its uniquely named
rows, never shared tables. These tests distinguish persistent-state checks from
full library-verified registration and assertion fixtures; they do not claim
universal browser/authenticator or database isolation parity.
