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

// DropSchema tears the mirror down, in reverse dependency order.
//
// This exists for the rebuild-from-zero path, which is a supported operation
// rather than a test fixture: when a provider reports that a cursor is no
// longer usable, discarding and refetching is the correct recovery.
var DropSchema = []string{
	`DROP TABLE IF EXISTS mail_sync_state`,
	`DROP TABLE IF EXISTS mail_bodies`,
	`DROP TABLE IF EXISTS mail_message_mailboxes`,
	`DROP TABLE IF EXISTS mail_messages`,
	`DROP TABLE IF EXISTS mail_mailboxes`,
	`DROP TABLE IF EXISTS mail_accounts`,
}
