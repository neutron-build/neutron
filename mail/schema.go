package mail

// Schema is the DDL for the local mirror, in dependency order.
//
// Everything here is derived state. The provider is the source of truth, and
// dropping these tables must cost nothing but a resync — that property is
// asserted directly by the rebuild-from-zero test, and it is what makes the
// cache safe to blow away when a sync goes wrong.
//
// The DDL is deliberately plain: TEXT, BIGINT, BOOLEAN, TIMESTAMP, composite
// primary keys. It runs on Nucleus over pgwire and on stock PostgreSQL
// unchanged, so a deployment can point at either.
//
// Address lists, keywords, and part trees are stored as JSON text rather than
// as normalised child tables. They are always read and written whole, with
// the message, and never queried by their internals — splitting them out
// would buy joins nobody issues.
var Schema = []string{
	`CREATE TABLE IF NOT EXISTS mail_accounts (
		id           TEXT PRIMARY KEY,
		provider     TEXT NOT NULL,
		email        TEXT NOT NULL,
		name         TEXT,
		needs_reauth BOOLEAN NOT NULL DEFAULT FALSE
	)`,

	`CREATE TABLE IF NOT EXISTS mail_mailboxes (
		account_id TEXT NOT NULL,
		id         TEXT NOT NULL,
		name       TEXT NOT NULL,
		role       TEXT,
		parent_id  TEXT,
		native     TEXT NOT NULL,
		PRIMARY KEY (account_id, id)
	)`,

	// references_header, not "references": the bare word is reserved SQL.
	`CREATE TABLE IF NOT EXISTS mail_messages (
		account_id        TEXT NOT NULL,
		id                TEXT NOT NULL,
		thread_id         TEXT,
		fingerprint       TEXT,
		subject           TEXT,
		sent_at           TIMESTAMP,
		received_at       TIMESTAMP,
		from_addrs        TEXT,
		to_addrs          TEXT,
		cc_addrs          TEXT,
		bcc_addrs         TEXT,
		reply_to_addrs    TEXT,
		keywords          TEXT,
		has_attachment    BOOLEAN NOT NULL DEFAULT FALSE,
		size              BIGINT NOT NULL DEFAULT 0,
		preview           TEXT,
		message_id_header TEXT,
		in_reply_to       TEXT,
		references_header TEXT,
		PRIMARY KEY (account_id, id)
	)`,

	// Mailbox membership is a separate table because it is many-to-many:
	// Gmail labels and JMAP mailboxes both let one message live in several
	// places at once, and IMAP's one-folder-per-message is just the
	// degenerate case.
	`CREATE TABLE IF NOT EXISTS mail_message_mailboxes (
		account_id TEXT NOT NULL,
		message_id TEXT NOT NULL,
		mailbox_id TEXT NOT NULL,
		PRIMARY KEY (account_id, message_id, mailbox_id)
	)`,

	// Bodies are fetched lazily and may be absent for most messages.
	// Separating them keeps the envelope table small enough to scan.
	`CREATE TABLE IF NOT EXISTS mail_bodies (
		account_id TEXT NOT NULL,
		message_id TEXT NOT NULL,
		text_body  TEXT,
		html_body  TEXT,
		parts      TEXT,
		fetched_at TIMESTAMP,
		PRIMARY KEY (account_id, message_id)
	)`,

	// One cursor per mailbox. Account-level cursors — Gmail's historyId,
	// Graph's deltaLink — are stored under the empty mailbox id, so the
	// sync engine reads and writes both shapes through one path.
	`CREATE TABLE IF NOT EXISTS mail_sync_state (
		account_id TEXT NOT NULL,
		mailbox_id TEXT NOT NULL,
		cursor     TEXT NOT NULL,
		synced_at  TIMESTAMP,
		PRIMARY KEY (account_id, mailbox_id)
	)`,

	`CREATE INDEX IF NOT EXISTS mail_messages_received
		ON mail_messages (account_id, received_at)`,

	// Unified-inbox deduplication looks messages up by content identity
	// across accounts, so this index is deliberately not account-scoped.
	`CREATE INDEX IF NOT EXISTS mail_messages_fingerprint
		ON mail_messages (fingerprint)`,

	`CREATE INDEX IF NOT EXISTS mail_messages_thread
		ON mail_messages (account_id, thread_id)`,

	`CREATE INDEX IF NOT EXISTS mail_message_mailboxes_by_mailbox
		ON mail_message_mailboxes (account_id, mailbox_id)`,
}

// ScanSchema adds staged reconciliation scans (audit SYNC-03/SYNC-02).
// mirror_scans holds one durable scan per (account, mailbox) — its
// continuation is the resume point after a crash or page-budget cut;
// mirror_scan_seen accumulates the authoritative presence set page by
// page. Deletion of anything live happens only in FinishScan's single
// completion transaction, never against these tables' contents directly.
//
// No FK from mirror_scan_seen to mirror_scans: the rows are always
// deleted together by the store, and the DDL stays portable to Nucleus.
var ScanSchema = []string{
	`CREATE TABLE IF NOT EXISTS mirror_scans (
		id           TEXT PRIMARY KEY,
		account_id   TEXT NOT NULL,
		mailbox_id   TEXT NOT NULL,
		continuation TEXT NOT NULL DEFAULT '',
		started_at   TIMESTAMP,
		UNIQUE (account_id, mailbox_id)
	)`,
	`CREATE TABLE IF NOT EXISTS mirror_scan_seen (
		scan_id    TEXT NOT NULL,
		message_id TEXT NOT NULL,
		PRIMARY KEY (scan_id, message_id)
	)`,
}

// GenerationSchema gives staged scans a policy-generation identity and a
// per-generation completion marker (audit 5 SYNC-05). A reconciliation
// retry used to call BeginScan for every mailbox, discarding the previous
// attempt's durable progress and re-enumerating mailboxes that had already
// finished. With generations:
//
//   - mirror_scans.generation tags each scan with the policy version it
//     is enumerating for (0 = a policy-agnostic recovery scan).
//   - mail_scan_done records, per (account, generation, mailbox), that
//     this generation's scan of that mailbox FINISHED — the knowledge
//     FinishScan used to lose when it dropped the scan rows. A retry of
//     the same generation resumes in-progress scans and skips completed
//     mailboxes instead of restarting them.
var GenerationSchema = []string{
	`ALTER TABLE mirror_scans ADD COLUMN generation BIGINT NOT NULL DEFAULT 0`,
	`CREATE TABLE IF NOT EXISTS mail_scan_done (
		account_id  TEXT NOT NULL,
		generation  BIGINT NOT NULL,
		mailbox_id  TEXT NOT NULL,
		PRIMARY KEY (account_id, generation, mailbox_id)
	)`,
}

// ReferentialSchema hardens the mirror against retention/write races
// (audit SYNC-04): bodies and memberships may never reference a message
// the mirror no longer holds. The orphan deletes run first so an existing
// installation converges before the constraints land; NOT VALID makes the
// ADD CONSTRAINT instant by skipping the existing-row scan while still
// enforcing every NEW write, and VALIDATE then proves the back-catalogue
// in the same migration. ON DELETE CASCADE keeps the manual child deletes
// (retention, account deletion) correct rather than fighting them.
//
// This migration requires real PostgreSQL DDL support; the product runs
// on PostgreSQL (see OPS-02), and deployments on engines without ALTER
// TABLE ... ADD CONSTRAINT must not adopt this version blindly.
var ReferentialSchema = []string{
	`DELETE FROM mail_bodies b
	  WHERE NOT EXISTS (
	    SELECT 1 FROM mail_messages m
	     WHERE m.account_id = b.account_id AND m.id = b.message_id)`,
	`DELETE FROM mail_message_mailboxes mm
	  WHERE NOT EXISTS (
	    SELECT 1 FROM mail_messages m
	     WHERE m.account_id = mm.account_id AND m.id = mm.message_id)`,
	`ALTER TABLE mail_bodies ADD CONSTRAINT mail_bodies_message_fk
		FOREIGN KEY (account_id, message_id)
		REFERENCES mail_messages (account_id, id)
		ON DELETE CASCADE NOT VALID`,
	`ALTER TABLE mail_message_mailboxes ADD CONSTRAINT mail_membership_message_fk
		FOREIGN KEY (account_id, message_id)
		REFERENCES mail_messages (account_id, id)
		ON DELETE CASCADE NOT VALID`,
	`ALTER TABLE mail_bodies VALIDATE CONSTRAINT mail_bodies_message_fk`,
	`ALTER TABLE mail_message_mailboxes VALIDATE CONSTRAINT mail_membership_message_fk`,
}

// Account lock order (audit SYNC-04): every transaction that writes or
// deletes mirror rows — PutEnvelopes, PutBody, DeleteMessages,
// RemoveFromMailbox, PutMailboxes, scan staging and completion on the
// engine side; retention sweeps, orphan cleanup, and account deletion on
// the product side — executes SELECT pg_advisory_xact_lock(
// AccountLockKey(account_id)) as its FIRST statement. One lock per
// transaction, acquired before any other resource: that is the whole
// order, and it cannot deadlock across the two connection pools.

// DropSchema tears the mirror down, in reverse dependency order.
//
// This exists for the rebuild-from-zero path, which is a supported operation
// rather than a test fixture: when a provider reports that a cursor is no
// longer usable, discarding and refetching is the correct recovery.
var DropSchema = []string{
	`DROP TABLE IF EXISTS mail_scan_done`,
	`DROP TABLE IF EXISTS mirror_scan_seen`,
	`DROP TABLE IF EXISTS mirror_scans`,
	`DROP TABLE IF EXISTS mail_sync_state`,
	`DROP TABLE IF EXISTS mail_bodies`,
	`DROP TABLE IF EXISTS mail_message_mailboxes`,
	`DROP TABLE IF EXISTS mail_messages`,
	`DROP TABLE IF EXISTS mail_mailboxes`,
	`DROP TABLE IF EXISTS mail_accounts`,
	`DROP TABLE IF EXISTS mail_migrations`,
}
