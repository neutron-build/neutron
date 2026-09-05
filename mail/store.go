package mail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the local mirror of one or more accounts.
//
// There is exactly one implementation, backed by Nucleus over pgwire. That is
// deliberate: a second in-memory implementation would be the natural thing to
// test against, and it would quietly stop testing the path that production
// runs. Correctness here is asserted against a real engine or not at all.
type Store interface {
	Migrate(ctx context.Context) error

	PutAccount(ctx context.Context, a *Account) error
	Account(ctx context.Context, id AccountID) (*Account, error)
	Accounts(ctx context.Context) ([]Account, error)
	SetNeedsReauth(ctx context.Context, id AccountID, needs bool) error

	PutMailboxes(ctx context.Context, acct AccountID, boxes []Mailbox) error
	Mailboxes(ctx context.Context, acct AccountID) ([]Mailbox, error)

	PutEnvelopes(ctx context.Context, acct AccountID, envs []Envelope) error
	Envelope(ctx context.Context, acct AccountID, id MessageID) (*Envelope, error)
	EnvelopeIDs(ctx context.Context, acct AccountID, box MailboxID) ([]MessageID, error)
	DeleteMessages(ctx context.Context, acct AccountID, ids []MessageID) error
	RemoveFromMailbox(ctx context.Context, acct AccountID, box MailboxID, ids []MessageID) error

	PutBody(ctx context.Context, acct AccountID, b *Body) error
	Body(ctx context.Context, acct AccountID, id MessageID) (*Body, error)

	Cursor(ctx context.Context, acct AccountID, box MailboxID) (Cursor, error)
	PutCursor(ctx context.Context, acct AccountID, box MailboxID, cur Cursor) error

	// ResetMailbox discards every message and the cursor for one mailbox,
	// so the next sync refetches from empty. This is the recovery path for
	// a provider reporting that a cursor is no longer usable.
	ResetMailbox(ctx context.Context, acct AccountID, box MailboxID) error

	Search(ctx context.Context, acct AccountID, query string, limit int) ([]Envelope, error)
	Thread(ctx context.Context, acct AccountID, thread ThreadID) ([]Envelope, error)

	Close()
}

// ErrNoStore is returned when a lookup finds nothing locally. It is distinct
// from ErrNotFound, which means the provider has no such message: absent
// locally is an ordinary cache miss and a reason to fetch, while absent at the
// provider is a reason to delete.
var ErrNoStore = errors.New("mail: not present in local store")

// PgStore is the Nucleus-backed Store.
type PgStore struct {
	pool *pgxpool.Pool
}

// Open connects to Nucleus (or PostgreSQL) at the given URL.
func Open(ctx context.Context, url string) (*PgStore, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("mail: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("mail: ping: %w", err)
	}
	return &PgStore{pool: pool}, nil
}

func (s *PgStore) Close() { s.pool.Close() }

func (s *PgStore) Migrate(ctx context.Context) error {
	for _, stmt := range Schema {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("mail: migrate: %w", err)
		}
	}
	return nil
}

// Drop removes every mail table. Callers use this to prove the mirror is
// rebuildable; nothing in the sync path calls it.
func (s *PgStore) Drop(ctx context.Context) error {
	for _, stmt := range DropSchema {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("mail: drop: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Accounts
// ---------------------------------------------------------------------------

func (s *PgStore) PutAccount(ctx context.Context, a *Account) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO mail_accounts (id, provider, email, name, needs_reauth)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO UPDATE SET
		   provider = $2, email = $3, name = $4, needs_reauth = $5`,
		string(a.ID), string(a.Provider), a.Email, a.Name, a.NeedsReauth)
	if err != nil {
		return fmt.Errorf("mail: put account: %w", err)
	}
	return nil
}

func (s *PgStore) Account(ctx context.Context, id AccountID) (*Account, error) {
	var a Account
	var provider string
	err := s.pool.QueryRow(ctx,
		`SELECT id, provider, email, name, needs_reauth
		   FROM mail_accounts WHERE id = $1`, string(id)).
		Scan(&a.ID, &provider, &a.Email, &a.Name, &a.NeedsReauth)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoStore
	}
	if err != nil {
		return nil, fmt.Errorf("mail: account: %w", err)
	}
	a.Provider = Provider(provider)
	return &a, nil
}

func (s *PgStore) Accounts(ctx context.Context) ([]Account, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, provider, email, name, needs_reauth FROM mail_accounts ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("mail: accounts: %w", err)
	}
	defer rows.Close()

	var out []Account
	for rows.Next() {
		var a Account
		var provider string
		if err := rows.Scan(&a.ID, &provider, &a.Email, &a.Name, &a.NeedsReauth); err != nil {
			return nil, fmt.Errorf("mail: accounts scan: %w", err)
		}
		a.Provider = Provider(provider)
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *PgStore) SetNeedsReauth(ctx context.Context, id AccountID, needs bool) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE mail_accounts SET needs_reauth = $2 WHERE id = $1`, string(id), needs)
	if err != nil {
		return fmt.Errorf("mail: set needs_reauth: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Mailboxes
// ---------------------------------------------------------------------------

func (s *PgStore) PutMailboxes(ctx context.Context, acct AccountID, boxes []Mailbox) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("mail: put mailboxes: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, b := range boxes {
		_, err := tx.Exec(ctx,
			`INSERT INTO mail_mailboxes (account_id, id, name, role, parent_id, native)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (account_id, id) DO UPDATE SET
			   name = $3, role = $4, parent_id = $5, native = $6`,
			string(acct), string(b.ID), b.Name, string(b.Role), string(b.ParentID), b.Native)
		if err != nil {
			return fmt.Errorf("mail: put mailbox %s: %w", b.ID, err)
		}
	}

	wanted := make(map[MailboxID]bool, len(boxes))
	for _, b := range boxes {
		wanted[b.ID] = true
	}
	staleSet := map[MailboxID]bool{}
	for _, query := range []string{
		`SELECT id FROM mail_mailboxes WHERE account_id = $1`,
		`SELECT mailbox_id FROM mail_sync_state WHERE account_id = $1`,
		`SELECT mailbox_id FROM mail_message_mailboxes WHERE account_id = $1`,
	} {
		rows, err := tx.Query(ctx, query, string(acct))
		if err != nil {
			return fmt.Errorf("mail: list stored mailbox state: %w", err)
		}
		for rows.Next() {
			var id MailboxID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("mail: scan stored mailbox state: %w", err)
			}
			// The empty ID is reserved for account-level provider cursors.
			if id != "" && !wanted[id] {
				staleSet[id] = true
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}
	stale := make([]MailboxID, 0, len(staleSet))
	for id := range staleSet {
		stale = append(stale, id)
	}

	for _, box := range stale {
		messageRows, err := tx.Query(ctx,
			`SELECT message_id FROM mail_message_mailboxes WHERE account_id = $1 AND mailbox_id = $2`,
			string(acct), string(box))
		if err != nil {
			return fmt.Errorf("mail: list messages in stale mailbox %s: %w", box, err)
		}
		var ids []MessageID
		for messageRows.Next() {
			var id MessageID
			if err := messageRows.Scan(&id); err != nil {
				messageRows.Close()
				return err
			}
			ids = append(ids, id)
		}
		if err := messageRows.Err(); err != nil {
			messageRows.Close()
			return err
		}
		messageRows.Close()

		for _, stmt := range []string{
			`DELETE FROM mail_message_mailboxes WHERE account_id = $1 AND mailbox_id = $2`,
			`DELETE FROM mail_sync_state WHERE account_id = $1 AND mailbox_id = $2`,
			`DELETE FROM mail_mailboxes WHERE account_id = $1 AND id = $2`,
		} {
			if _, err := tx.Exec(ctx, stmt, string(acct), string(box)); err != nil {
				return fmt.Errorf("mail: remove stale mailbox %s: %w", box, err)
			}
		}
		for _, id := range ids {
			var remaining int
			if err := tx.QueryRow(ctx,
				`SELECT COUNT(*) FROM mail_message_mailboxes WHERE account_id = $1 AND message_id = $2`,
				string(acct), string(id)).Scan(&remaining); err != nil {
				return err
			}
			if remaining != 0 {
				continue
			}
			if _, err := tx.Exec(ctx, `DELETE FROM mail_bodies WHERE account_id = $1 AND message_id = $2`, string(acct), string(id)); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM mail_messages WHERE account_id = $1 AND id = $2`, string(acct), string(id)); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (s *PgStore) Mailboxes(ctx context.Context, acct AccountID) ([]Mailbox, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, role, parent_id, native
		   FROM mail_mailboxes WHERE account_id = $1 ORDER BY name`, string(acct))
	if err != nil {
		return nil, fmt.Errorf("mail: mailboxes: %w", err)
	}
	defer rows.Close()

	var out []Mailbox
	for rows.Next() {
		var b Mailbox
		var role, parent *string
		if err := rows.Scan(&b.ID, &b.Name, &role, &parent, &b.Native); err != nil {
			return nil, fmt.Errorf("mail: mailboxes scan: %w", err)
		}
		if role != nil {
			b.Role = Role(*role)
		}
		if parent != nil {
			b.ParentID = MailboxID(*parent)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

func (s *PgStore) PutEnvelopes(ctx context.Context, acct AccountID, envs []Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("mail: put envelopes: %w", err)
	}
	defer tx.Rollback(ctx)

	for i := range envs {
		e := &envs[i]
		if err := e.ID.Validate(); err != nil {
			return err
		}
		if e.Fingerprint == "" {
			e.Fingerprint = ComputeFingerprint(e)
		}

		from, _ := json.Marshal(e.From)
		to, _ := json.Marshal(e.To)
		cc, _ := json.Marshal(e.Cc)
		bcc, _ := json.Marshal(e.Bcc)
		replyTo, _ := json.Marshal(e.ReplyTo)
		kw, _ := json.Marshal(e.Keywords)
		inReplyTo, _ := json.Marshal(e.InReplyTo)
		refs, _ := json.Marshal(e.References)

		_, err := tx.Exec(ctx,
			`INSERT INTO mail_messages (
				account_id, id, thread_id, fingerprint, subject,
				sent_at, received_at, from_addrs, to_addrs, cc_addrs,
				bcc_addrs, reply_to_addrs, keywords, has_attachment, size,
				preview, message_id_header, in_reply_to, references_header)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
			 ON CONFLICT (account_id, id) DO UPDATE SET
			   thread_id = $3, fingerprint = $4, subject = $5,
			   sent_at = $6, received_at = $7, from_addrs = $8, to_addrs = $9,
			   cc_addrs = $10, bcc_addrs = $11, reply_to_addrs = $12,
			   keywords = $13, has_attachment = $14, size = $15, preview = $16,
			   message_id_header = $17, in_reply_to = $18, references_header = $19`,
			string(acct), string(e.ID), string(e.ThreadID), string(e.Fingerprint), e.Subject,
			nullTime(e.SentAt), nullTime(e.ReceivedAt), string(from), string(to), string(cc),
			string(bcc), string(replyTo), string(kw), e.HasAttachment, e.Size,
			e.Preview, e.MessageIDHeader, string(inReplyTo), string(refs))
		if err != nil {
			return fmt.Errorf("mail: put envelope %s: %w", e.ID, err)
		}

		if e.MailboxIDsComplete {
			if _, err := tx.Exec(ctx,
				`DELETE FROM mail_message_mailboxes WHERE account_id = $1 AND message_id = $2`,
				string(acct), string(e.ID)); err != nil {
				return fmt.Errorf("mail: clear mailboxes for %s: %w", e.ID, err)
			}
		}
		for _, box := range e.MailboxIDs {
			if _, err := tx.Exec(ctx,
				`INSERT INTO mail_message_mailboxes (account_id, message_id, mailbox_id)
				 VALUES ($1, $2, $3)
				 ON CONFLICT (account_id, message_id, mailbox_id) DO NOTHING`,
				string(acct), string(e.ID), string(box)); err != nil {
				return fmt.Errorf("mail: link %s to %s: %w", e.ID, box, err)
			}
		}
	}
	return tx.Commit(ctx)
}

const envelopeColumns = `id, thread_id, fingerprint, subject, sent_at, received_at,
	from_addrs, to_addrs, cc_addrs, bcc_addrs, reply_to_addrs, keywords,
	has_attachment, size, preview, message_id_header, in_reply_to, references_header`

func scanEnvelope(row pgx.Row) (*Envelope, error) {
	var e Envelope
	var threadID, fingerprint, subject, preview, msgIDHdr *string
	var sentAt, receivedAt *time.Time
	var from, to, cc, bcc, replyTo, kw, inReplyTo, refs *string

	err := row.Scan(&e.ID, &threadID, &fingerprint, &subject, &sentAt, &receivedAt,
		&from, &to, &cc, &bcc, &replyTo, &kw,
		&e.HasAttachment, &e.Size, &preview, &msgIDHdr, &inReplyTo, &refs)
	if err != nil {
		return nil, err
	}

	deref := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	e.ThreadID = ThreadID(deref(threadID))
	e.Fingerprint = Fingerprint(deref(fingerprint))
	e.Subject = deref(subject)
	e.Preview = deref(preview)
	e.MessageIDHeader = deref(msgIDHdr)
	if sentAt != nil {
		e.SentAt = *sentAt
	}
	if receivedAt != nil {
		e.ReceivedAt = *receivedAt
	}

	unmarshal := func(p *string, dst any) {
		if p != nil && *p != "" {
			_ = json.Unmarshal([]byte(*p), dst)
		}
	}
	unmarshal(from, &e.From)
	unmarshal(to, &e.To)
	unmarshal(cc, &e.Cc)
	unmarshal(bcc, &e.Bcc)
	unmarshal(replyTo, &e.ReplyTo)
	unmarshal(kw, &e.Keywords)
	unmarshal(inReplyTo, &e.InReplyTo)
	unmarshal(refs, &e.References)

	return &e, nil
}

func (s *PgStore) Envelope(ctx context.Context, acct AccountID, id MessageID) (*Envelope, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+envelopeColumns+` FROM mail_messages WHERE account_id = $1 AND id = $2`,
		string(acct), string(id))
	e, err := scanEnvelope(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoStore
	}
	if err != nil {
		return nil, fmt.Errorf("mail: envelope: %w", err)
	}
	if err := s.loadMailboxes(ctx, acct, e); err != nil {
		return nil, err
	}
	return e, nil
}

func (s *PgStore) loadMailboxes(ctx context.Context, acct AccountID, e *Envelope) error {
	rows, err := s.pool.Query(ctx,
		`SELECT mailbox_id FROM mail_message_mailboxes
		  WHERE account_id = $1 AND message_id = $2 ORDER BY mailbox_id`,
		string(acct), string(e.ID))
	if err != nil {
		return fmt.Errorf("mail: load mailboxes: %w", err)
	}
	defer rows.Close()

	e.MailboxIDs = nil
	for rows.Next() {
		var id MailboxID
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("mail: load mailboxes scan: %w", err)
		}
		e.MailboxIDs = append(e.MailboxIDs, id)
	}
	return rows.Err()
}

func (s *PgStore) EnvelopeIDs(ctx context.Context, acct AccountID, box MailboxID) ([]MessageID, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT message_id FROM mail_message_mailboxes
		  WHERE account_id = $1 AND mailbox_id = $2 ORDER BY message_id`,
		string(acct), string(box))
	if err != nil {
		return nil, fmt.Errorf("mail: envelope ids: %w", err)
	}
	defer rows.Close()

	var out []MessageID
	for rows.Next() {
		var id MessageID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("mail: envelope ids scan: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *PgStore) DeleteMessages(ctx context.Context, acct AccountID, ids []MessageID) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("mail: delete messages: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, id := range ids {
		for _, stmt := range []string{
			`DELETE FROM mail_message_mailboxes WHERE account_id = $1 AND message_id = $2`,
			`DELETE FROM mail_bodies WHERE account_id = $1 AND message_id = $2`,
			`DELETE FROM mail_messages WHERE account_id = $1 AND id = $2`,
		} {
			if _, err := tx.Exec(ctx, stmt, string(acct), string(id)); err != nil {
				return fmt.Errorf("mail: delete %s: %w", id, err)
			}
		}
	}
	return tx.Commit(ctx)
}

// RemoveFromMailbox drops mailbox membership without deleting the message.
//
// A message that leaves its last mailbox is deleted outright: it is no longer
// reachable at the provider, so keeping the row would strand it in the mirror
// with no way to ever notice it had gone.
func (s *PgStore) RemoveFromMailbox(ctx context.Context, acct AccountID, box MailboxID, ids []MessageID) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("mail: remove from mailbox: %w", err)
	}
	defer tx.Rollback(ctx)

	var orphaned []MessageID
	for _, id := range ids {
		if _, err := tx.Exec(ctx,
			`DELETE FROM mail_message_mailboxes
			  WHERE account_id = $1 AND message_id = $2 AND mailbox_id = $3`,
			string(acct), string(id), string(box)); err != nil {
			return fmt.Errorf("mail: unlink %s: %w", id, err)
		}

		var remaining int
		if err := tx.QueryRow(ctx,
			`SELECT COUNT(*) FROM mail_message_mailboxes
			  WHERE account_id = $1 AND message_id = $2`,
			string(acct), string(id)).Scan(&remaining); err != nil {
			return fmt.Errorf("mail: count mailboxes for %s: %w", id, err)
		}
		if remaining == 0 {
			orphaned = append(orphaned, id)
		}
	}

	for _, id := range orphaned {
		for _, stmt := range []string{
			`DELETE FROM mail_bodies WHERE account_id = $1 AND message_id = $2`,
			`DELETE FROM mail_messages WHERE account_id = $1 AND id = $2`,
		} {
			if _, err := tx.Exec(ctx, stmt, string(acct), string(id)); err != nil {
				return fmt.Errorf("mail: delete orphan %s: %w", id, err)
			}
		}
	}
	return tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

func (s *PgStore) PutBody(ctx context.Context, acct AccountID, b *Body) error {
	parts, _ := json.Marshal(b.Parts)
	_, err := s.pool.Exec(ctx,
		`INSERT INTO mail_bodies (account_id, message_id, text_body, html_body, parts, fetched_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (account_id, message_id) DO UPDATE SET
		   text_body = $3, html_body = $4, parts = $5, fetched_at = $6`,
		string(acct), string(b.MessageID), b.Text, b.HTML, string(parts), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("mail: put body: %w", err)
	}
	return nil
}

func (s *PgStore) Body(ctx context.Context, acct AccountID, id MessageID) (*Body, error) {
	var b Body
	var text, html, parts *string
	err := s.pool.QueryRow(ctx,
		`SELECT text_body, html_body, parts FROM mail_bodies
		  WHERE account_id = $1 AND message_id = $2`,
		string(acct), string(id)).Scan(&text, &html, &parts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoStore
	}
	if err != nil {
		return nil, fmt.Errorf("mail: body: %w", err)
	}

	b.MessageID = id
	if text != nil {
		b.Text = *text
	}
	if html != nil {
		b.HTML = *html
	}
	if parts != nil && *parts != "" {
		_ = json.Unmarshal([]byte(*parts), &b.Parts)
	}
	return &b, nil
}

// ---------------------------------------------------------------------------
// Sync state
// ---------------------------------------------------------------------------

func (s *PgStore) Cursor(ctx context.Context, acct AccountID, box MailboxID) (Cursor, error) {
	var cur string
	err := s.pool.QueryRow(ctx,
		`SELECT cursor FROM mail_sync_state WHERE account_id = $1 AND mailbox_id = $2`,
		string(acct), string(box)).Scan(&cur)
	if errors.Is(err, pgx.ErrNoRows) {
		// No cursor means "never synced", which is a legitimate starting
		// state rather than an error: an empty cursor tells the adapter to
		// sync from the beginning.
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("mail: cursor: %w", err)
	}
	return Cursor(cur), nil
}

func (s *PgStore) PutCursor(ctx context.Context, acct AccountID, box MailboxID, cur Cursor) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO mail_sync_state (account_id, mailbox_id, cursor, synced_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (account_id, mailbox_id) DO UPDATE SET cursor = $3, synced_at = $4`,
		string(acct), string(box), string(cur), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("mail: put cursor: %w", err)
	}
	return nil
}

func (s *PgStore) ResetMailbox(ctx context.Context, acct AccountID, box MailboxID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("mail: reset mailbox: %w", err)
	}
	defer tx.Rollback(ctx)

	ids, err := s.EnvelopeIDs(ctx, acct, box)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM mail_sync_state WHERE account_id = $1 AND mailbox_id = $2`,
		string(acct), string(box)); err != nil {
		return fmt.Errorf("mail: clear cursor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return s.RemoveFromMailbox(ctx, acct, box, ids)
}

// ---------------------------------------------------------------------------
// Reads for consumers
// ---------------------------------------------------------------------------

// Search finds messages matching a query across subject, preview, and sender.
//
// Matching uses Nucleus's `@@` operator rather than LIKE: it stems, drops
// stopwords, and requires every term, so "quarterly numbers" finds "the
// quarterly number" and does not match a substring buried inside an unrelated
// word. LIKE does none of that.
//
// It runs without a full-text index. The table-attached index needs an integer
// PRIMARY KEY, and mail_messages is keyed on (account_id, id) — text, because
// message identity is derived from provider IDs and Message-ID headers, not
// minted by us. `@@` is defined row-locally so it stays correct unindexed; it
// just scans. See docs/NEUTRON_GAPS.md.
//
// BM25 ranking is likewise unavailable without the index, so results are
// ordered by recency, which is the right default for mail anyway.
func (s *PgStore) Search(ctx context.Context, acct AccountID, query string, limit int) ([]Envelope, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+envelopeColumns+` FROM mail_messages
		  WHERE account_id = $1
		    AND (subject @@ $2 OR preview @@ $2 OR from_addrs @@ $2)
		  ORDER BY received_at DESC
		  LIMIT $3`,
		string(acct), query, limit)
	if err != nil {
		return nil, fmt.Errorf("mail: search: %w", err)
	}
	return s.collectEnvelopes(ctx, acct, rows)
}

func (s *PgStore) Thread(ctx context.Context, acct AccountID, thread ThreadID) ([]Envelope, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+envelopeColumns+` FROM mail_messages
		  WHERE account_id = $1 AND thread_id = $2
		  ORDER BY sent_at`,
		string(acct), string(thread))
	if err != nil {
		return nil, fmt.Errorf("mail: thread: %w", err)
	}
	return s.collectEnvelopes(ctx, acct, rows)
}

func (s *PgStore) collectEnvelopes(ctx context.Context, acct AccountID, rows pgx.Rows) ([]Envelope, error) {
	defer rows.Close()

	var out []Envelope
	for rows.Next() {
		e, err := scanEnvelope(rows)
		if err != nil {
			return nil, fmt.Errorf("mail: scan envelope: %w", err)
		}
		out = append(out, *e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Mailbox membership is loaded after the cursor is drained: issuing a
	// second query while rows are still open would deadlock on a
	// single-connection pool.
	for i := range out {
		if err := s.loadMailboxes(ctx, acct, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

var _ Store = (*PgStore)(nil)
