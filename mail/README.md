# neutron-mail

A mail connector. It reads a user's existing mailbox — Gmail, Microsoft 365,
Fastmail, any IMAP host — and mirrors it into Nucleus, then serves that mirror
over HTTP.

**It never receives mail.** Nothing here provisions addresses or accepts
inbound SMTP. Mail stays where it already lives; this logs in and reads it,
the way a desktop mail client does. That keeps deliverability, abuse handling,
and custodian-of-record liability out of scope entirely.

```
Gmail / Graph / IMAP / JMAP     mail lives here
        |  connect, sync
        v
   neutron-mail                 one engine
        |
        +---------------+
        v               v
   inbox UI        agent tools
```

## Layout

| Path | What |
|---|---|
| `types.go` | Canonical model — JMAP-shaped |
| `identity.go` | Message identity, upgrades, fingerprints, threading |
| `adapter.go` | The `Adapter` interface every provider implements |
| `sync.go` | Sync engine: pagination, reset recovery, deletion sweep |
| `store.go` | `PgStore` — the mirror, over pgwire |
| `schema.go` | DDL; runs on Nucleus or stock PostgreSQL |
| `scheduler.go` | Periodic sync, per-account backoff |
| `tokensource.go` | Token callback — how background sync gets a credential |
| `send.go` | SMTP submission, reply threading |
| `service.go` | HTTP surface, RFC 7807 errors |
| `imap/` | Hand-rolled IMAP client, CONDSTORE + QRESYNC |
| `jmap/` | JMAP (RFC 8620/8621) |
| `gmail/` | Gmail API, historyId incremental sync |
| `graph/` | Microsoft Graph, delta queries |
| `cmd/neutron-mail/` | The service binary |

The TypeScript client and agent tools live in the monorepo at
`typescript/packages/neutron-mail` (`@neutron-build/mail`) — not part of this
module's tree when it is vendored (see VENDOR.md downstream).

## The two ideas worth knowing

**The model is JMAP-shaped, not IMAP-shaped.** Of the four protocols, JMAP is
the only one with stable message identity. IMAP has none — UIDs are
per-mailbox, a move is a delete-and-append, and a UIDVALIDITY change
invalidates every UID at once. Modelling on IMAP would bake that damage into
every other provider. See the comment block at the top of `identity.go`.

**The mirror is derived, never authoritative.** The provider is the source of
truth, and dropping the local copy must cost nothing but a resync. That is a
testable claim, unlike "stateless", and
`TestIntegrationRebuildFromZero` tests it.

## Running

```bash
DATABASE_URL=postgres://user:pass@localhost:5432/mail go run ./cmd/neutron-mail
```

Serves on `:8090`. `GET /health` reports `{status, nucleus, version}`.

Adapters are built per request from the credential the caller sends, so the
engine stores none of its own. Background sync uses a token callback: the
engine asks the application for a fresh token, because with no request in
flight there is nothing to carry one. See `tokensource.go`.

## Testing

```bash
go test ./...
```

Engine tests use a store double and cover what no real provider will produce
on demand: a UIDVALIDITY reset, a repeated reset, a crash between writing
messages and advancing the cursor, an identity upgrade.

`PgStore` is covered separately, against a real engine, because a double
cannot tell you whether the SQL is right:

```bash
nucleus start --port 55432 --memory &
NEUTRON_MAIL_TEST_DATABASE_URL='postgres://postgres@127.0.0.1:55432/postgres?sslmode=disable' \
  go test ./...
```

Those tests drop and recreate their tables. Never point them at a database
you care about.

The **differential oracle** in `imap/live_test.go` is the one test that proves
an adapter reads a real server correctly. It syncs, changes the mailbox from
outside over SMTP, resyncs, and compares. It needs a real IMAP server, so
`mail/testdata/dovecot` ships a throwaway one (Dovecot 2.3 for IMAP, with
QRESYNC and CONDSTORE; Postfix in front as the SMTP delivery path; one seeded
user and mailbox with unread, flagged, Drafts, Sent and Trash mail; no real
accounts), driven by `scripts/live-imap.sh`:

```bash
mail/scripts/live-imap.sh up          # docker build + run, waits until seeded
eval "$(mail/scripts/live-imap.sh env)"
(cd mail && go test -race ./imap/...)
mail/scripts/live-imap.sh down

mail/scripts/live-imap.sh test        # all of the above; a skipped live test fails
```

`CONTAINER_CLI=podman` swaps the runtime. CI runs the same thing in
`.github/workflows/mail.yml`. Without `NEUTRON_MAIL_TEST_IMAP` the live tests
skip, so a plain `go test ./...` stays hermetic. Any other disposable server
works for the core tests too (GreenMail: `NEUTRON_MAIL_TEST_IMAP` and
`NEUTRON_MAIL_TEST_SMTP` only); the `Seeded` tests need the fixture and skip
without `NEUTRON_MAIL_TEST_IMAP_SEEDED=1`.

It has already earned its place: it caught `Apply` failing against every
server, because syncing selects a mailbox with EXAMINE (read-only, so reading
never sets `\Seen`) and `STORE` is refused on a read-only selection. No unit
test would have found that.

## Status

The module's Go tests pass, including the live oracle against GreenMail and
the store suite against a real Nucleus (counts move with every change; CI is
the source of truth).

**Gmail and Graph have never been run against live servers.** They are written
against the documented APIs and unit-tested over their normalization logic,
but verifying them needs real accounts and registered OAuth apps. JMAP is
unit-tested against a stub server rather than a live one, for the same reason.

Search matches with Nucleus's `@@` — stemmed terms, stopwords dropped, every
term required — rather than `LIKE`. It runs without an index: the
table-attached FTS index needs an integer `PRIMARY KEY`, and `mail_messages` is
keyed on `(account_id, id)`, text, because message identity comes from provider
IDs and Message-ID headers rather than being minted locally. `@@` is defined
row-locally so it stays correct unindexed; it just scans. Results order by
recency, which is the right default for mail anyway.

One caveat worth knowing: Nucleus's English stemmer currently stems singular
and plural forms of many nouns differently, so searching "folder" will not find
"folders". See `docs/ADOPTION_FINDINGS.md` A-014 in the monorepo.

## Before serving Gmail users

Every useful Gmail scope is restricted. That means OAuth verification plus an
annual CASA assessment — roughly $500–$1,800/year, and four to eight weeks end
to end. Start it in parallel with development; the schedule is the constraint,
not the cost.

Google permits running a model over a user's own mail as a user-facing
feature. It prohibits training or improving a general model on that data, and
restricts human review. Route mail content only to providers on no-training,
zero-retention terms, and list them as subprocessors.

## Identity transactions and product state

Use `OpenWithIdentityPolicy(ctx, url, admit, remap)` when a product stores filing,
receipts, Undo authority or other message references. `IdentityAdmission` runs
on the supplied `IdentityTx` immediately after transaction begin, before the
engine account advisory lock. Acquire and verify product owner/account row locks
there. `IdentityRemapper` uses that same transaction after the replacement
message, memberships, body and staged seen evidence exist, before old-ID
retirement. Never open a separate transaction or acquire owner locks in the
remapper. Callback errors roll back the page and cursor; neither callback may
publish external effects. A nonnil remapper requires nonnil admission.

The engine carries explicit `IdentityPair` mappings through delta, intermediate
scan and terminal scan transactions. Old thread keys remain unchanged. Existing
target identities without a previously committed matching alias refuse with
`ErrIdentityCollision`; there is no implicit last-writer merge. A committed alias
makes page replay idempotent. New mappings, including those whose old mirror row
has expired, require a remapper. Pure mirror applications can explicitly supply
no-op admission/remapping. Older store implementations compile, but promotion
refuses without `IdentityPageStore`; it never falls back to separate retirement.

For Graph accounts, use `OpenWithGraphIdentityPolicy(ctx, url, admit, remap,
inventory)`. `GraphReferenceInventory` enumerates product-only message IDs under
those locks, including retained/decrypted reply references absent from the
mirror. Do not alter encrypted payloads, AAD or request hashes: resolve old IDs
with account-qualified `ResolveIdentity` at dispatch. Direct provider Raw,
Attachment and product reply callers must resolve aliases themselves. Notes
keyed only by threads are outside message-ID remapping.

`MigrateGraphIdentity` requires the account's verified authenticated `/me` ID,
a `GraphTranslator` (implemented by `graph.Adapter`) and a batch size of 1–1000.
An email address is not a verified mailbox binding. Historical accounts need
explicit binding verification. Each call durably stages one translation batch;
missing, duplicate or conflicting results refuse. While mapping is pending,
`GraphIdentityFormat` and mirror ingestion refuse. A final transaction rechecks
all live/scan/product references, remaps dependent state and aliases, and then
publishes `GraphImmutableIDs`. A failed cutover leaves old rows readable and the
mapping resumable. `AbortGraphIdentity` discards only uncommitted mappings; a
committed format cannot be silently downgraded. Retention may remove a staged
source; its translation remains a reference-only alias and does not resurrect
expired mail. The full cutover is one transaction and needs representative
mailbox sizing and backup/recovery rehearsal.

Keep legacy headers until this marker commits, including on new empty accounts:
they still need the verified binding and explicit reference inventory. Configure
`dialer.NewWithPolicy` with the store's `GraphIdentityFormat` and product-owned
`JMAPAllowedOrigins`. Product send/reply paths must hold account admission across
format lookup, alias resolution and bounded provider dispatch so cutover cannot
interleave. Refresh credentials before acquiring these database locks. Use
`graph.WithIdentityPreference` only for the authenticated Graph API client,
never preauthenticated upload URLs. Other Prefer values are retained. A real
same-mailbox move/translation rehearsal remains required provider acceptance.

## Attachment certainty and credential origins

`Envelope.AttachmentPresence` is `unknown`, `present` or `absent`.
`HasAttachment` remains a compatibility flag; false does not prove absence.
Gmail metadata sync does not download full MIME solely for a badge. A complete
body fetch updates the body and certainty together, and later metadata cannot
erase known evidence. Existing false badges migrate to unknown; old caches are
not guessed to be complete. Consumers must expose unknown honestly.

JMAP defaults credential destinations to the configured discovery origin.
Explicitly configure additional HTTPS origins with `Config.AllowedOrigins` or
the resolver policy for separate API/download hosts. Session metadata cannot
authorize an origin. Initial discovery, expanded downloads, redirects and custom
HTTP clients all pass mandatory checks before dispatch. Effective default ports
are normalized. Private HTTPS servers and existing loopback development HTTP
remain supported; a redirect never grants a new plaintext origin.

The production MIME renderer folds at existing syntactic whitespace and refuses
unsplittable lines over 998 bytes, including generated part headers. Use the
renderer before durable send admission and persist its prepared representation.
No automatic uncertain-send retry is introduced by identity or header handling.
