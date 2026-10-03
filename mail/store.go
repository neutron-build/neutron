package mail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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

// MessageLocator is implemented by stores that can say which mailboxes hold a
// message. It is optional so that Store implementations outside this module
// keep compiling; PgStore implements it, and the engine uses it to satisfy a
// MailboxSelector adapter before a body fetch.
type MessageLocator interface {
	// MessageMailboxes returns every mailbox the message is filed in, or
	// ErrNoStore when the message is not mirrored locally.
	MessageMailboxes(ctx context.Context, acct AccountID, id MessageID) ([]MailboxID, error)
}

// PgStore is the Nucleus-backed Store.
type PgStore struct {
	pool *pgxpool.Pool

	// advOnce probes advisory-lock support once per store (see
	// lockAccountTx); Nucleus over pgwire has no pg_advisory_* family.
	advOnce sync.Once
	advOK   bool
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

// advisoryReady probes advisory-lock support once per store.
func (s *PgStore) advisoryReady() bool {
	s.advOnce.Do(func() {
		if advisoryUnsupported.Load() {
			s.advOK = false
			return
		}
		if advisorySupported.Load() {
			s.advOK = true
			return
		}
		conn, err := s.pool.Acquire(context.Background())
		if err != nil {
			s.advOK = false
			return
		}
		defer conn.Release()
		ok, err := advisoryCheck(context.Background(), conn.Conn())
		if err != nil {
			s.advOK = false
			return
		}
		s.advOK = ok
	})
	return s.advOK
}

// lockAccountTx takes the account maintenance advisory lock as the
// transaction's FIRST statement — the documented lock order every mirror
// writer shares with the product's retention/deletion transactions (audit
// SYNC-04). Transaction-scoped: it releases at commit or rollback, and a
// backend without advisory support (Nucleus) skips it after one probe.
func (s *PgStore) lockAccountTx(ctx context.Context, tx pgx.Tx, acct AccountID) error {
	if !s.advisoryReady() {
		return nil
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, AccountLockKey(acct)); err != nil {
		return fmt.Errorf("mail: account maintenance lock: %w", err)
	}
	return nil
}

// Migrate lives in migrate.go: the versioned runner with ledger,
// checksums, and advisory-lock serialization (audit OPS-02).

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
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return err
	}

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
		`SELECT mailbox_id FROM mirror_scans WHERE account_id = $1`,
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
			`DELETE FROM mirror_scan_seen WHERE scan_id IN (SELECT id FROM mirror_scans WHERE account_id = $1 AND mailbox_id = $2)`,
			`DELETE FROM mirror_scans WHERE account_id = $1 AND mailbox_id = $2`,
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
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return err
	}
	if err := putEnvelopesTx(ctx, tx, acct, envs); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// putEnvelopesTx is the envelope upsert shared by the delta path and
// staged scan pages, so both write with identical semantics inside a
// caller-owned transaction.
func putEnvelopesTx(ctx context.Context, tx pgx.Tx, acct AccountID, envs []Envelope) error {
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
	return nil
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
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return err
	}

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
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return err
	}

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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("mail: put body: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO mail_bodies (account_id, message_id, text_body, html_body, parts, fetched_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (account_id, message_id) DO UPDATE SET
		   text_body = $3, html_body = $4, parts = $5, fetched_at = $6`,
		string(acct), string(b.MessageID), b.Text, b.HTML, string(parts), time.Now().UTC()); err != nil {
		return fmt.Errorf("mail: put body: %w", err)
	}
	return tx.Commit(ctx)
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

func (s *PgStore) MessageMailboxes(ctx context.Context, acct AccountID, id MessageID) ([]MailboxID, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT mailbox_id FROM mail_message_mailboxes
		  WHERE account_id = $1 AND message_id = $2 ORDER BY mailbox_id`,
		string(acct), string(id))
	if err != nil {
		return nil, fmt.Errorf("mail: message mailboxes: %w", err)
	}
	defer rows.Close()
	var boxes []MailboxID
	for rows.Next() {
		var box string
		if err := rows.Scan(&box); err != nil {
			return nil, fmt.Errorf("mail: message mailboxes: %w", err)
		}
		boxes = append(boxes, MailboxID(box))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mail: message mailboxes: %w", err)
	}
	if len(boxes) == 0 {
		return nil, ErrNoStore
	}
	return boxes, nil
}

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

// ResetMailbox discards every message and the cursor for one mailbox,
// so the next sync refetches from empty.
//
// LEGACY RECOVERY PATH (audit SYNC-03): deleting before the replacement
// enumeration exists is exactly the destructive ordering the staged-scan
// machinery replaced. The engine now calls this only for stores that do
// not implement ScanStore; it remains on PgStore for compatibility with
// callers that ask for a hard reset explicitly.
func (s *PgStore) ResetMailbox(ctx context.Context, acct AccountID, box MailboxID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("mail: reset mailbox: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return err
	}

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

// ---------------------------------------------------------------------------
// Staged reconciliation scans (audit SYNC-03)
// ---------------------------------------------------------------------------

var _ ScanStore = (*PgStore)(nil)
var _ FinalScanPageStore = (*PgStore)(nil)

// BeginScan starts a staged scan, discarding any previous staged progress
// for the mailbox. Nothing live is touched.
func (s *PgStore) BeginScan(ctx context.Context, acct AccountID, box MailboxID) (*Scan, error) {
	return s.BeginScanGeneration(ctx, acct, box, 0)
}

// BeginScanGeneration starts a generation-tagged staged scan (audit 5
// SYNC-05); generation 0 is the policy-agnostic recovery scan.
func (s *PgStore) BeginScanGeneration(ctx context.Context, acct AccountID, box MailboxID, generation int64) (*Scan, error) {
	scan := &Scan{
		ID:           NewScanID(),
		Account:      acct,
		Mailbox:      box,
		Continuation: "",
		StartedAt:    time.Now().UTC(),
		Generation:   generation,
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("mail: begin scan: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return nil, err
	}
	for _, stmt := range []string{
		`DELETE FROM mirror_scan_seen WHERE scan_id IN
		   (SELECT id FROM mirror_scans WHERE account_id = $1 AND mailbox_id = $2)`,
		`DELETE FROM mirror_scans WHERE account_id = $1 AND mailbox_id = $2`,
	} {
		if _, err := tx.Exec(ctx, stmt, string(acct), string(box)); err != nil {
			return nil, fmt.Errorf("mail: discard previous scan: %w", err)
		}
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO mirror_scans (id, account_id, mailbox_id, continuation, started_at, generation)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		string(scan.ID), string(acct), string(box), string(scan.Continuation), scan.StartedAt, scan.Generation); err != nil {
		return nil, fmt.Errorf("mail: insert scan: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return scan, nil
}

// ScanDone reports whether a generation's scan of one mailbox already
// completed (audit 5 SYNC-05): the marker outlives the scan rows.
func (s *PgStore) ScanDone(ctx context.Context, acct AccountID, box MailboxID, generation int64) (bool, error) {
	var done bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM mail_scan_done
		  WHERE account_id = $1 AND mailbox_id = $2 AND generation = $3)`,
		string(acct), string(box), generation).Scan(&done)
	if err != nil {
		return false, fmt.Errorf("mail: scan done: %w", err)
	}
	return done, nil
}

// PruneScanDone drops every other generation's completion markers for the
// account (audit 5 SYNC-05).
func (s *PgStore) PruneScanDone(ctx context.Context, acct AccountID, keepGeneration int64) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM mail_scan_done WHERE account_id = $1 AND generation <> $2`,
		string(acct), keepGeneration)
	if err != nil {
		return fmt.Errorf("mail: prune scan done: %w", err)
	}
	return nil
}

func scanFromRow(acct AccountID, id, mailbox, continuation string, startedAt *time.Time, generation int64) *Scan {
	scan := &Scan{ID: ScanID(id), Account: acct, Mailbox: MailboxID(mailbox), Continuation: Cursor(continuation), Generation: generation}
	if startedAt != nil {
		scan.StartedAt = *startedAt
	}
	return scan
}

func (s *PgStore) RunningScan(ctx context.Context, acct AccountID, box MailboxID) (*Scan, error) {
	var id, mailbox, continuation string
	var startedAt *time.Time
	var generation int64
	err := s.pool.QueryRow(ctx,
		`SELECT id, mailbox_id, continuation, started_at, generation FROM mirror_scans
		  WHERE account_id = $1 AND mailbox_id = $2`,
		string(acct), string(box)).Scan(&id, &mailbox, &continuation, &startedAt, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoStore
	}
	if err != nil {
		return nil, fmt.Errorf("mail: running scan: %w", err)
	}
	return scanFromRow(acct, id, mailbox, continuation, startedAt, generation), nil
}

func (s *PgStore) RunningScans(ctx context.Context, acct AccountID) ([]Scan, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, mailbox_id, continuation, started_at, generation FROM mirror_scans
		  WHERE account_id = $1 ORDER BY mailbox_id`, string(acct))
	if err != nil {
		return nil, fmt.Errorf("mail: running scans: %w", err)
	}
	defer rows.Close()
	var out []Scan
	for rows.Next() {
		var id, mailbox, continuation string
		var startedAt *time.Time
		var generation int64
		if err := rows.Scan(&id, &mailbox, &continuation, &startedAt, &generation); err != nil {
			return nil, fmt.Errorf("mail: running scans scan: %w", err)
		}
		out = append(out, *scanFromRow(acct, id, mailbox, continuation, startedAt, generation))
	}
	return out, rows.Err()
}

// ApplyScanPage stages one page atomically: envelopes upserted, seen IDs
// recorded, destroyed IDs removed from the staged seen set AND this
// mailbox's live membership, continuation advanced — one transaction
// under the account maintenance lock. A failure stages nothing.
func (s *PgStore) ApplyScanPage(ctx context.Context, scan ScanID, envs []Envelope, seen, destroyed []MessageID, next Cursor) error {
	_, err := s.applyScanPage(ctx, scan, envs, seen, destroyed, next, false)
	return err
}

// ApplyFinalScanPage stages the final page and finalizes the scan in the
// SAME transaction. There is no durable state containing a terminal cursor
// without its completion marker, and any failure rolls back the whole page.
func (s *PgStore) ApplyFinalScanPage(ctx context.Context, scan ScanID, envs []Envelope, seen, destroyed []MessageID, terminal Cursor) (int, error) {
	return s.applyScanPage(ctx, scan, envs, seen, destroyed, terminal, true)
}

func (s *PgStore) applyScanPage(ctx context.Context, scan ScanID, envs []Envelope, seen, destroyed []MessageID, next Cursor, final bool) (int, error) {
	var acct AccountID
	var box MailboxID
	if err := s.pool.QueryRow(ctx,
		`SELECT account_id, mailbox_id FROM mirror_scans WHERE id = $1`,
		string(scan)).Scan(&acct, &box); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNoStore
		}
		return 0, fmt.Errorf("mail: scan page lookup: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("mail: apply scan page: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return 0, err
	}

	// The scope lookup precedes the account lock. A replacement scan may
	// have won that lock first; reject this stale page before writing any
	// envelopes or seen rows for a scan that no longer exists.
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM mirror_scans WHERE id = $1 AND account_id = $2 AND mailbox_id = $3)`,
		string(scan), string(acct), string(box)).Scan(&exists); err != nil {
		return 0, fmt.Errorf("mail: recheck scan page: %w", err)
	}
	if !exists {
		return 0, ErrNoStore
	}

	if err := putEnvelopesTx(ctx, tx, acct, envs); err != nil {
		return 0, err
	}

	batch := &pgx.Batch{}
	for _, id := range seen {
		batch.Queue(`INSERT INTO mirror_scan_seen (scan_id, message_id) VALUES ($1, $2)
		             ON CONFLICT (scan_id, message_id) DO NOTHING`, string(scan), string(id))
	}
	// Negative evidence from the enumeration's own catch-up (audit 5
	// SYNC-04): a message this scan previously staged as seen and that
	// the provider has since destroyed must leave both the seen set and
	// the live membership, in the same transaction as the page.
	for _, id := range destroyed {
		batch.Queue(`DELETE FROM mirror_scan_seen WHERE scan_id = $1 AND message_id = $2`,
			string(scan), string(id))
		batch.Queue(`DELETE FROM mail_message_mailboxes
		              WHERE account_id = $1 AND mailbox_id = $2 AND message_id = $3`,
			string(acct), string(box), string(id))
	}
	batch.Queue(`UPDATE mirror_scans SET continuation = $2 WHERE id = $1`, string(scan), string(next))
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return 0, fmt.Errorf("mail: stage scan page: %w", err)
	}
	var pruned int
	if final {
		pruned, err = finishScanTx(ctx, tx, acct, box, scan, next)
		if err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return pruned, nil
}

// FinishScan completes the scan authoritatively in one transaction under
// the account maintenance lock: memberships of this mailbox absent from
// the seen set are pruned, messages left with no membership anywhere are
// deleted (their bodies explicitly, so the semantics hold on backends
// without FK enforcement), the terminal cursor is published, the
// generation-completion marker is recorded (audit 5 SYNC-05), and the
// scan rows drop. This is the ONLY deletion in the recovery path.
func (s *PgStore) FinishScan(ctx context.Context, acct AccountID, box MailboxID, scan ScanID, terminal Cursor) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("mail: finish scan: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return 0, err
	}

	pruned, err := finishScanTx(ctx, tx, acct, box, scan, terminal)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return pruned, nil
}

// finishScanTx requires the account maintenance lock and commits nothing;
// both standalone finalization and atomic final-page staging use it.
func finishScanTx(ctx context.Context, tx pgx.Tx, acct AccountID, box MailboxID, scan ScanID, terminal Cursor) (int, error) {
	var generation int64
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT generation, EXISTS(SELECT 1 FROM mirror_scans WHERE id = $1 AND account_id = $2 AND mailbox_id = $3)
		   FROM mirror_scans WHERE id = $1`,
		string(scan), string(acct), string(box)).Scan(&generation, &exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNoStore
		}
		return 0, err
	}
	if !exists {
		return 0, ErrNoStore
	}

	// Prune only what a COMPLETE enumeration omitted, and only for this
	// mailbox: a message still filed elsewhere keeps its other
	// memberships.
	tag, err := tx.Exec(ctx,
		`DELETE FROM mail_message_mailboxes mm
		  WHERE mm.account_id = $1 AND mm.mailbox_id = $2
		    AND NOT EXISTS (
		      SELECT 1 FROM mirror_scan_seen s
		       WHERE s.scan_id = $3 AND s.message_id = mm.message_id)`,
		string(acct), string(box), string(scan))
	if err != nil {
		return 0, fmt.Errorf("mail: prune absent memberships: %w", err)
	}
	pruned := int(tag.RowsAffected())

	// A message with no membership left anywhere is unreachable at the
	// provider and strands in the mirror otherwise. The orphan-body sweep
	// after it is what keeps the semantics identical on backends without
	// FK enforcement (on PostgreSQL the ON DELETE CASCADE already did it).
	for _, stmt := range []string{
		`DELETE FROM mail_messages m
		  WHERE m.account_id = $1
		    AND NOT EXISTS (
		      SELECT 1 FROM mail_message_mailboxes mm
		       WHERE mm.account_id = m.account_id AND mm.message_id = m.id)`,
		`DELETE FROM mail_bodies b
		  WHERE b.account_id = $1
		    AND NOT EXISTS (
		      SELECT 1 FROM mail_messages m
		       WHERE m.account_id = b.account_id AND m.id = b.message_id)`,
	} {
		if _, err := tx.Exec(ctx, stmt, string(acct)); err != nil {
			return 0, fmt.Errorf("mail: delete unmapped messages: %w", err)
		}
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO mail_sync_state (account_id, mailbox_id, cursor, synced_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (account_id, mailbox_id) DO UPDATE SET cursor = $3, synced_at = $4`,
		string(acct), string(box), string(terminal), time.Now().UTC()); err != nil {
		return 0, fmt.Errorf("mail: publish scan cursor: %w", err)
	}

	// Record this generation's completion BEFORE dropping the scan rows:
	// the marker is what lets a reconciliation retry skip a mailbox whose
	// scan already finished instead of re-enumerating it (audit 5 SYNC-05).
	if _, err := tx.Exec(ctx,
		`INSERT INTO mail_scan_done (account_id, generation, mailbox_id)
		 VALUES ($1, $2, $3) ON CONFLICT (account_id, generation, mailbox_id) DO NOTHING`,
		string(acct), generation, string(box)); err != nil {
		return 0, fmt.Errorf("mail: record scan completion: %w", err)
	}

	for _, stmt := range []string{
		`DELETE FROM mirror_scan_seen WHERE scan_id = $1`,
		`DELETE FROM mirror_scans WHERE id = $1`,
	} {
		if _, err := tx.Exec(ctx, stmt, string(scan)); err != nil {
			return 0, fmt.Errorf("mail: drop scan rows: %w", err)
		}
	}

	return pruned, nil
}
