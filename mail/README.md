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
| `send.go` | SMTP submission, reply threading |
| `service.go` | HTTP surface, RFC 7807 errors |
| `imap/` | Hand-rolled IMAP client, CONDSTORE + QRESYNC |
| `jmap/` | JMAP (RFC 8620/8621) |
| `gmail/` | Gmail API, historyId incremental sync |
| `graph/` | Microsoft Graph, delta queries |
| `cmd/neutron-mail/` | The service binary |

The TypeScript client and agent tools live in
`typescript/packages/neutron-mail` (`@neutron-build/mail`).

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

Adapters are not wired in `main.go`, because constructing one needs a
credential per account and credential custody is deployment-specific. With
none wired the service serves the mirror read-only, which is a safe default.

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
outside over SMTP, resyncs, and compares:

```bash
docker run -d -p 13143:3143 -p 13025:3025 \
  -e GREENMAIL_OPTS='-Dgreenmail.setup.test.all -Dgreenmail.hostname=0.0.0.0 -Dgreenmail.auth.disabled' \
  greenmail/standalone:latest

NEUTRON_MAIL_TEST_IMAP=127.0.0.1:13143 NEUTRON_MAIL_TEST_SMTP=127.0.0.1:13025 \
  go test ./imap/...
```

It has already earned its place: it caught `Apply` failing against every
server, because syncing selects a mailbox with EXAMINE (read-only, so reading
never sets `\Seen`) and `STORE` is refused on a read-only selection. No unit
test would have found that.

## Status

130 Go tests and 16 TypeScript tests, all passing, including the live oracle
against GreenMail and the store suite against a real Nucleus.

**Gmail and Graph have never been run against live servers.** They are written
against the documented APIs and unit-tested over their normalization logic,
but verifying them needs real accounts and registered OAuth apps. JMAP is
unit-tested against a stub server rather than a live one, for the same reason.

Adapters are not wired in `main.go`; see Running above.

Search currently uses `LIKE`. The table-attached full-text index
(`CREATE INDEX ... USING FTS`, `@@`, `BM25`) is the intended implementation
and lives on an unmerged Nucleus branch; swapping the body of `PgStore.Search`
is the only change required.

## Before serving Gmail users

Every useful Gmail scope is restricted. That means OAuth verification plus an
annual CASA assessment — roughly $500–$1,800/year, and four to eight weeks end
to end. Start it in parallel with development; the schedule is the constraint,
not the cost.

Google permits running a model over a user's own mail as a user-facing
feature. It prohibits training or improving a general model on that data, and
restricts human review. Route mail content only to providers on no-training,
zero-retention terms, and list them as subprocessors.
