package mail

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrIdentityTransactionRequired = errors.New("mail: identity remapping requires a transaction-capable store and remapper")
	ErrIdentityCollision           = errors.New("mail: identity collision requires explicit reconciliation")
)

// IdentityPair is account-scoped. Neither ID is a guessed content match.
type IdentityPair struct{ OldID, NewID MessageID }

// IdentityTx exposes the current SQL transaction without commit/rollback.
// Remappers must use only this transaction, never another pool. The account
// maintenance lock is already held; do not acquire an owner lock here (products
// requiring an owner lock must arrange it BEFORE entering the engine write).
// Refuse conflicting filing/receipt/reference state instead of dropping it.
// Do not dispatch notifications or change external state inside this callback.
type IdentityTx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
type IdentityRemapper func(context.Context, IdentityTx, AccountID, []IdentityPair) error

// IdentityAdmission runs on the SAME transaction before the engine account
// advisory lock. Products acquire/verify their owner lifecycle lock here, then
// check active account admission; all hooks must obey owner -> account order.
// It cannot commit or open another transaction. Configure before publication.
type IdentityAdmission func(context.Context, IdentityTx, AccountID) error

// BeforeAccountWrite is an alias retained for the initial candidate handoff.
type BeforeAccountWrite = IdentityAdmission

// OpenWithIdentityPolicy requires admission whenever a remapper is configured.
// Admission runs immediately after Begin, on the same SQL transaction, before
// the engine account lock. Product owner/account row locks survive through commit.
func OpenWithIdentityPolicy(ctx context.Context, url string, admit IdentityAdmission, remap IdentityRemapper) (*PgStore, error) {
	if remap != nil && admit == nil {
		return nil, ErrIdentityTransactionRequired
	}
	s, err := Open(ctx, url)
	if err == nil {
		s.beforeAccountWrite = admit
		s.remap = remap
	}
	return s, err
}
func OpenWithIdentityHooks(ctx context.Context, url string, before BeforeAccountWrite, remap IdentityRemapper) (*PgStore, error) {
	return OpenWithIdentityPolicy(ctx, url, before, remap)
}

// GraphReferenceInventory supplies product-only old references (including
// decrypted queued parent IDs) without mutating their ciphertext or request hash.
// It runs under owner/account locks at snapshot and again before cutover. Empty
// is an explicit attestation, not an inference from an empty mirror.
type GraphReferenceInventory func(context.Context, IdentityTx, AccountID) ([]MessageID, error)

func OpenWithGraphIdentityPolicy(ctx context.Context, url string, admit IdentityAdmission, remap IdentityRemapper, inventory GraphReferenceInventory) (*PgStore, error) {
	if inventory == nil {
		return nil, ErrIdentityTransactionRequired
	}
	s, err := OpenWithIdentityPolicy(ctx, url, admit, remap)
	if err == nil {
		s.graphInventory = inventory
	}
	return s, err
}

// IdentityPageStore commits replacement/dependents/remap/retirement and cursor
// as one unit. An older Store remains source compatible, but promotion refuses
// rather than reverting to delete/upsert across transactions.
type IdentityPageStore interface {
	ApplyIdentityPage(context.Context, AccountID, MailboxID, []Envelope, []IdentityPair, []MessageID, Cursor) error
	ApplyIdentityScanPage(context.Context, ScanID, []Envelope, []IdentityPair, []MessageID, []MessageID, Cursor, bool) (int, error)
}

// OpenWithIdentityRemapper is retained for source compatibility with the initial
// candidate API; a nonnil remapper without admission refuses. Use the policy
// constructor; pure engine users can explicitly supply no-op admission/remap.
func OpenWithIdentityRemapper(ctx context.Context, url string, remap IdentityRemapper) (*PgStore, error) {
	return OpenWithIdentityPolicy(ctx, url, nil, remap)
}

func (s *PgStore) ApplyIdentityPage(ctx context.Context, acct AccountID, box MailboxID, envs []Envelope, pairs []IdentityPair, destroyed []MessageID, next Cursor) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return err
	}
	if err := s.mirrorWriteReadyTx(ctx, tx, acct); err != nil {
		return err
	}
	if err := s.identityUpsertsTx(ctx, tx, acct, envs, pairs); err != nil {
		return err
	}
	for _, id := range destroyed {
		if _, err := tx.Exec(ctx, `DELETE FROM mail_message_mailboxes WHERE account_id=$1 AND mailbox_id=$2 AND message_id=$3`, string(acct), string(box), string(id)); err != nil {
			return err
		}
	}
	// Only remove actual destroyed messages whose last membership disappeared.
	for _, id := range destroyed {
		if _, err := tx.Exec(ctx, `DELETE FROM mail_bodies WHERE account_id=$1 AND message_id=$2 AND NOT EXISTS (SELECT 1 FROM mail_message_mailboxes WHERE account_id=$1 AND message_id=$2)`, string(acct), string(id)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM mail_messages WHERE account_id=$1 AND id=$2 AND NOT EXISTS (SELECT 1 FROM mail_message_mailboxes WHERE account_id=$1 AND message_id=$2)`, string(acct), string(id)); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO mail_sync_state(account_id,mailbox_id,cursor,synced_at) VALUES($1,$2,$3,$4) ON CONFLICT(account_id,mailbox_id) DO UPDATE SET cursor=$3,synced_at=$4`, string(acct), string(box), string(next), time.Now().UTC()); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PgStore) ApplyIdentityScanPage(ctx context.Context, scan ScanID, envs []Envelope, pairs []IdentityPair, seen, destroyed []MessageID, next Cursor, final bool) (int, error) {
	return s.applyScanPageWithIdentities(ctx, scan, envs, pairs, seen, destroyed, next, final)
}

func (s *PgStore) identityUpsertsTx(ctx context.Context, tx pgx.Tx, acct AccountID, envs []Envelope, pairs []IdentityPair) error {
	return s.identityUpsertsWithRemapTx(ctx, tx, acct, envs, pairs, s.remap)
}
func (s *PgStore) identityUpsertsWithRemapTx(ctx context.Context, tx pgx.Tx, acct AccountID, envs []Envelope, pairs []IdentityPair, remap IdentityRemapper) error {
	if err := validateIdentityPairs(envs, pairs); err != nil {
		return err
	}
	active := make([]IdentityPair, 0, len(pairs))
	newMappings := make([]IdentityPair, 0, len(pairs))
	// Validate ALL collisions before upsert can overwrite a canonical envelope.
	for _, p := range pairs {
		var old, target bool
		var alias *string
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mail_messages WHERE account_id=$1 AND id=$2), EXISTS(SELECT 1 FROM mail_messages WHERE account_id=$1 AND id=$3), (SELECT new_id FROM mail_identity_aliases WHERE account_id=$1 AND old_id=$2)`, string(acct), string(p.OldID), string(p.NewID)).Scan(&old, &target, &alias); err != nil {
			return err
		}
		if old && target || target && alias == nil || alias != nil && *alias != string(p.NewID) {
			return fmt.Errorf("%w: %s -> %s", ErrIdentityCollision, p.OldID, p.NewID)
		}
		if alias == nil {
			newMappings = append(newMappings, p)
		}
		if !old && target && alias != nil {
			var retainedThread *string
			if err := tx.QueryRow(ctx, `SELECT thread_id FROM mail_messages WHERE account_id=$1 AND id=$2`, string(acct), string(p.NewID)).Scan(&retainedThread); err != nil {
				return err
			}
			if retainedThread != nil && *retainedThread != "" {
				for i := range envs {
					if envs[i].ID == p.NewID {
						envs[i].ThreadID = ThreadID(*retainedThread)
					}
				}
			}
		}
		if old {
			var oldThread *string
			if err := tx.QueryRow(ctx, `SELECT thread_id FROM mail_messages WHERE account_id=$1 AND id=$2`, string(acct), string(p.OldID)).Scan(&oldThread); err != nil {
				return err
			}
			if oldThread != nil && *oldThread != "" {
				for i := range envs {
					if envs[i].ID == p.NewID {
						envs[i].ThreadID = ThreadID(*oldThread)
					}
				}
			}
			active = append(active, p)
		}
	}
	if len(newMappings) > 0 && remap == nil {
		return ErrIdentityTransactionRequired
	}
	if err := putEnvelopesTx(ctx, tx, acct, envs); err != nil {
		return err
	}
	for _, p := range active {
		for _, stmt := range []string{
			`INSERT INTO mail_message_mailboxes(account_id,message_id,mailbox_id) SELECT account_id,$3,mailbox_id FROM mail_message_mailboxes WHERE account_id=$1 AND message_id=$2 ON CONFLICT DO NOTHING`,
			`INSERT INTO mail_bodies(account_id,message_id,text_body,html_body,parts,fetched_at) SELECT account_id,$3,text_body,html_body,parts,fetched_at FROM mail_bodies WHERE account_id=$1 AND message_id=$2`,
			`INSERT INTO mirror_scan_seen(scan_id,message_id) SELECT seen.scan_id,$3 FROM mirror_scan_seen seen JOIN mirror_scans scan ON scan.id=seen.scan_id WHERE scan.account_id=$1 AND seen.message_id=$2 ON CONFLICT DO NOTHING`,
			`DELETE FROM mirror_scan_seen WHERE message_id=$2 AND scan_id IN (SELECT id FROM mirror_scans WHERE account_id=$1)`,
		} {
			// The final statement uses only two parameters.
			args := []any{string(acct), string(p.OldID), string(p.NewID)}
			if stmt[0:6] == "DELETE" {
				args = args[:2]
			}
			if _, err := tx.Exec(ctx, stmt, args...); err != nil {
				return err
			}
		}
		// Body-derived certainty takes precedence over metadata-only envelopes.
		if _, err := tx.Exec(ctx, `UPDATE mail_messages SET has_attachment=old.has_attachment, attachment_presence=old.attachment_presence FROM mail_messages old WHERE mail_messages.account_id=$1 AND mail_messages.id=$3 AND old.account_id=$1 AND old.id=$2 AND mail_messages.attachment_presence='unknown'`, string(acct), string(p.OldID), string(p.NewID)); err != nil {
			return err
		}
	}
	if len(newMappings) > 0 {
		if err := remap(ctx, tx, acct, newMappings); err != nil {
			return fmt.Errorf("mail: product identity remap: %w", err)
		}
	}
	for _, p := range newMappings {
		if _, err := tx.Exec(ctx, `INSERT INTO mail_identity_aliases(account_id,old_id,new_id) VALUES($1,$2,$3)`, string(acct), string(p.OldID), string(p.NewID)); err != nil {
			return err
		}
	}
	for _, p := range active {
		for _, stmt := range []string{
			`DELETE FROM mail_bodies WHERE account_id=$1 AND message_id=$2`,
			`DELETE FROM mail_message_mailboxes WHERE account_id=$1 AND message_id=$2`,
			`DELETE FROM mail_messages WHERE account_id=$1 AND id=$2`,
		} {
			args := []any{string(acct), string(p.OldID), string(p.NewID)}
			if stmt[0:6] == "DELETE" {
				args = args[:2]
			}
			if _, err := tx.Exec(ctx, stmt, args...); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateIdentityPairs(envs []Envelope, pairs []IdentityPair) error {
	targets := map[MessageID]bool{}
	for _, e := range envs {
		targets[e.ID] = true
	}
	old, newIDs := map[MessageID]bool{}, map[MessageID]bool{}
	for _, p := range pairs {
		if err := p.OldID.Validate(); err != nil {
			return err
		}
		if err := p.NewID.Validate(); err != nil {
			return err
		}
		if p.OldID == p.NewID || !targets[p.NewID] || old[p.OldID] || newIDs[p.NewID] {
			return ErrIdentityCollision
		}
		old[p.OldID] = true
		newIDs[p.NewID] = true
	}
	for id := range old {
		if newIDs[id] {
			return ErrIdentityCollision
		}
	}
	return nil
}

// ResolveIdentity preserves old-client/encrypted request references without
// modifying their hashes or AAD. Resolve before provider Raw/Attachment calls.
func (s *PgStore) ResolveIdentity(ctx context.Context, acct AccountID, id MessageID) (MessageID, error) {
	visited := map[MessageID]bool{}
	for n := 0; n < 16; n++ {
		if visited[id] {
			return "", ErrIdentityCollision
		}
		visited[id] = true
		var next MessageID
		err := s.pool.QueryRow(ctx, `SELECT new_id FROM mail_identity_aliases WHERE account_id=$1 AND old_id=$2`, string(acct), string(id)).Scan(&next)
		if errors.Is(err, pgx.ErrNoRows) {
			return id, nil
		}
		if err != nil {
			return "", err
		}
		id = next
	}
	return "", ErrIdentityCollision
}
