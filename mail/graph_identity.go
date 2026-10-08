package mail

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const GraphLegacyIDs = "restId"
const GraphImmutableIDs = "restImmutableEntryId"

var ErrIdentityMigrationPending = errors.New("mail: Graph identity migration is pending")

// GraphTranslator must address /me with the SAME authenticated mailbox as the
// account. expectedMailbox comes from the product's verified account binding,
// not an email/display-name guess. Translation preserves case and rejects gaps.
type GraphTranslator interface {
	GraphMailboxKey(context.Context) (string, error)
	TranslateGraphIDs(context.Context, []MessageID) ([]IdentityPair, error)
}

// GraphIdentityFormat is also the resolver's per-account header policy. Pending
// migration refuses provider operations, rather than letting mixed formats flow.
func (s *PgStore) GraphIdentityFormat(ctx context.Context, acct AccountID) (string, error) {
	var format string
	err := s.pool.QueryRow(ctx, `SELECT format FROM mail_graph_identity WHERE account_id=$1`, string(acct)).Scan(&format)
	if errors.Is(err, pgx.ErrNoRows) {
		return GraphLegacyIDs, nil
	}
	if err != nil {
		return "", err
	}
	if format == "mapping" {
		return "", ErrIdentityMigrationPending
	}
	if format != GraphImmutableIDs && format != GraphLegacyIDs {
		return "", ErrIdentityCollision
	}
	return format, nil
}

// MigrateGraphIdentity stages at most batchSize translations per call (1..1000).
// No headers flip or live row retires until every mapping and product callback
// commits in the final transaction. False means resume, including after restart.
// Once begun, failures keep the account fenced. AbortGraphIdentity safely drops
// uncommitted mappings; a completed cutover cannot be downgraded automatically.
func (s *PgStore) MigrateGraphIdentity(ctx context.Context, acct AccountID, expectedMailbox string, provider GraphTranslator, batchSize int) (bool, error) {
	if expectedMailbox == "" || provider == nil || batchSize < 1 || batchSize > 1000 {
		return false, errors.New("mail: verified mailbox binding and bounded batch required")
	}
	if s.remap == nil || s.graphInventory == nil {
		return false, ErrIdentityTransactionRequired
	}
	actual, err := provider.GraphMailboxKey(ctx)
	if err != nil {
		return false, err
	}
	if actual != expectedMailbox {
		return false, errors.New("mail: Graph translation mailbox does not match verified account")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return false, err
	}
	var kind string
	if err := tx.QueryRow(ctx, `SELECT provider FROM mail_accounts WHERE id=$1`, string(acct)).Scan(&kind); err != nil {
		return false, err
	}
	if kind != string(ProviderGraph) {
		return false, errors.New("mail: not a Graph account")
	}
	var key, format string
	err = tx.QueryRow(ctx, `SELECT mailbox_key,format FROM mail_graph_identity WHERE account_id=$1`, string(acct)).Scan(&key, &format)
	if errors.Is(err, pgx.ErrNoRows) {
		// Snapshot under account lock. Normal mirror writers are fenced thereafter.
		if _, err := tx.Exec(ctx, `INSERT INTO mail_graph_identity(account_id,mailbox_key,format) VALUES($1,$2,'mapping')`, string(acct), expectedMailbox); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO mail_graph_id_map(account_id,old_id) SELECT account_id,id FROM mail_messages WHERE account_id=$1`, string(acct)); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO mail_graph_id_map(account_id,old_id) SELECT $1,seen.message_id FROM mirror_scan_seen seen JOIN mirror_scans scan ON scan.id=seen.scan_id WHERE scan.account_id=$1 ON CONFLICT DO NOTHING`, string(acct)); err != nil {
			return false, err
		}
		references, err := s.graphInventory(ctx, tx, acct)
		if err != nil {
			return false, err
		}
		for _, id := range references {
			if !strings.HasPrefix(string(id), "n:graph:") {
				return false, ErrIdentityCollision
			}
			if err := id.Validate(); err != nil {
				return false, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO mail_graph_id_map(account_id,old_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, string(acct), string(id)); err != nil {
				return false, err
			}
		}
	} else if err != nil {
		return false, err
	} else {
		if key != expectedMailbox {
			return false, errors.New("mail: persisted Graph mailbox binding changed")
		}
		if format == GraphImmutableIDs {
			return true, nil
		}
		if format != "mapping" {
			return false, ErrIdentityCollision
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	rows, err := s.pool.Query(ctx, `SELECT old_id FROM mail_graph_id_map WHERE account_id=$1 AND new_id IS NULL ORDER BY old_id LIMIT $2`, string(acct), batchSize)
	if err != nil {
		return false, err
	}
	var ids []MessageID
	for rows.Next() {
		var id MessageID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	if len(ids) > 0 {
		pairs, err := provider.TranslateGraphIDs(ctx, ids)
		if err != nil {
			return false, err
		}
		if err := validateGraphTranslations(ids, pairs); err != nil {
			return false, err
		}
		// Re-verify after network work, before durable provider proof is accepted.
		key, err := provider.GraphMailboxKey(ctx)
		if err != nil {
			return false, err
		}
		if key != expectedMailbox {
			return false, errors.New("mail: Graph mailbox changed during translation")
		}
		if err := s.stageGraphTranslations(ctx, acct, expectedMailbox, pairs); err != nil {
			return false, err
		}
	}
	return s.finishGraphIdentity(ctx, acct, expectedMailbox)
}

func validateGraphTranslations(ids []MessageID, pairs []IdentityPair) error {
	if len(ids) != len(pairs) {
		return errors.New("mail: incomplete Graph translation")
	}
	wanted := map[MessageID]bool{}
	targets := map[MessageID]bool{}
	for _, id := range ids {
		if !strings.HasPrefix(string(id), "n:graph:") {
			return ErrIdentityCollision
		}
		wanted[id] = true
	}
	for _, p := range pairs {
		if !wanted[p.OldID] || targets[p.NewID] || !strings.HasPrefix(string(p.NewID), "n:graph:") || strings.TrimPrefix(string(p.NewID), "n:graph:") == "" {
			return ErrIdentityCollision
		}
		if err := p.NewID.Validate(); err != nil {
			return err
		}
		delete(wanted, p.OldID)
		targets[p.NewID] = true
	}
	if len(wanted) > 0 {
		return ErrIdentityCollision
	}
	return nil
}
func (s *PgStore) stageGraphTranslations(ctx context.Context, acct AccountID, key string, pairs []IdentityPair) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return err
	}
	if err := checkGraphMappingTx(ctx, tx, acct, key); err != nil {
		return err
	}
	for _, p := range pairs {
		tag, err := tx.Exec(ctx, `UPDATE mail_graph_id_map SET new_id=$3 WHERE account_id=$1 AND old_id=$2 AND (new_id IS NULL OR new_id=$3)`, string(acct), string(p.OldID), string(p.NewID))
		if err != nil {
			return fmt.Errorf("%w: translation target duplicate: %v", ErrIdentityCollision, err)
		}
		if tag.RowsAffected() != 1 {
			return ErrIdentityCollision
		}
	}
	return tx.Commit(ctx)
}
func checkGraphMappingTx(ctx context.Context, tx pgx.Tx, acct AccountID, key string) error {
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mail_graph_identity WHERE account_id=$1 AND mailbox_key=$2 AND format='mapping')`, string(acct), key).Scan(&valid); err != nil {
		return err
	}
	if !valid {
		return ErrIdentityMigrationPending
	}
	return nil
}
func (s *PgStore) finishGraphIdentity(ctx context.Context, acct AccountID, key string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return false, err
	}
	if err := checkGraphMappingTx(ctx, tx, acct, key); err != nil {
		return false, err
	}
	var missing bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mail_graph_id_map WHERE account_id=$1 AND new_id IS NULL)`, string(acct)).Scan(&missing); err != nil {
		return false, err
	}
	if missing {
		return false, nil
	}
	rows, err := tx.Query(ctx, `SELECT old_id,new_id FROM mail_graph_id_map WHERE account_id=$1 ORDER BY old_id`, string(acct))
	if err != nil {
		return false, err
	}
	var pairs []IdentityPair
	for rows.Next() {
		var p IdentityPair
		if err := rows.Scan(&p.OldID, &p.NewID); err != nil {
			rows.Close()
			return false, err
		}
		pairs = append(pairs, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	// New unmapped rows refuse cutover. Retired snapshot rows stay mapped as
	// reference-only IDs, preserving queued references without resurrecting mail.
	var drift bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mail_messages m WHERE m.account_id=$1 AND NOT EXISTS(SELECT 1 FROM mail_graph_id_map p WHERE p.account_id=$1 AND p.old_id=m.id))`, string(acct)).Scan(&drift); err != nil {
		return false, err
	}
	if drift {
		return false, errors.New("mail: Graph inventory changed; abort and restart mapping")
	}
	references, err := s.graphInventory(ctx, tx, acct)
	if err != nil {
		return false, err
	}
	mapped := map[MessageID]bool{}
	for _, p := range pairs {
		mapped[p.OldID] = true
	}
	for _, p := range pairs {
		if p.OldID != p.NewID && mapped[p.NewID] {
			return false, ErrIdentityCollision
		}
	}
	for _, id := range references {
		if !mapped[id] {
			return false, errors.New("mail: Graph product references changed; abort and restart mapping")
		}
	}
	var unseen bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mirror_scan_seen seen JOIN mirror_scans scan ON scan.id=seen.scan_id WHERE scan.account_id=$1 AND NOT EXISTS(SELECT 1 FROM mail_graph_id_map p WHERE p.account_id=$1 AND p.old_id=seen.message_id))`, string(acct)).Scan(&unseen); err != nil {
		return false, err
	}
	if unseen {
		return false, errors.New("mail: Graph scan references changed; abort and restart mapping")
	}
	var envs []Envelope
	var changes, referenceOnly []IdentityPair
	for _, p := range pairs {
		if p.OldID == p.NewID {
			continue
		}
		env, err := scanEnvelope(tx.QueryRow(ctx, `SELECT `+envelopeColumns+` FROM mail_messages WHERE account_id=$1 AND id=$2`, string(acct), string(p.OldID)))
		if errors.Is(err, pgx.ErrNoRows) {
			referenceOnly = append(referenceOnly, p)
			continue
		}
		if err != nil {
			return false, err
		}
		env.ID = p.NewID // Thread IDs are opaque conversation identities, unchanged.
		envs = append(envs, *env)
		changes = append(changes, p)
	}
	// Callback sees the COMPLETE mapping, including product/scan-only refs.
	remap := func(ctx context.Context, tx IdentityTx, acct AccountID, _ []IdentityPair) error {
		return s.remap(ctx, tx, acct, pairs)
	}
	if err := s.identityUpsertsWithRemapTx(ctx, tx, acct, envs, changes, remap); err != nil {
		return false, err
	}
	if len(changes) == 0 {
		if err := s.remap(ctx, tx, acct, pairs); err != nil {
			return false, err
		}
	}
	for _, p := range referenceOnly {
		// Preserve target records; only the account's old seen evidence moves.
		for _, stmt := range []string{
			`INSERT INTO mirror_scan_seen(scan_id,message_id) SELECT seen.scan_id,$3 FROM mirror_scan_seen seen JOIN mirror_scans scan ON scan.id=seen.scan_id WHERE scan.account_id=$1 AND seen.message_id=$2 ON CONFLICT DO NOTHING`,
			`DELETE FROM mirror_scan_seen WHERE message_id=$2 AND scan_id IN (SELECT id FROM mirror_scans WHERE account_id=$1)`,
			`INSERT INTO mail_identity_aliases(account_id,old_id,new_id) VALUES($1,$2,$3)`,
		} {
			args := []any{string(acct), string(p.OldID), string(p.NewID)}
			if strings.HasPrefix(stmt, "DELETE") {
				args = args[:2]
			}
			if _, err := tx.Exec(ctx, stmt, args...); err != nil {
				return false, err
			}
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE mail_graph_identity SET format=$2 WHERE account_id=$1`, string(acct), GraphImmutableIDs); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
func (s *PgStore) AbortGraphIdentity(ctx context.Context, acct AccountID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := s.lockAccountTx(ctx, tx, acct); err != nil {
		return err
	}
	var format string
	err = tx.QueryRow(ctx, `SELECT format FROM mail_graph_identity WHERE account_id=$1`, string(acct)).Scan(&format)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if format != "mapping" {
		return errors.New("mail: committed Graph identity cutover cannot be downgraded")
	}
	if _, err := tx.Exec(ctx, `DELETE FROM mail_graph_id_map WHERE account_id=$1`, string(acct)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM mail_graph_identity WHERE account_id=$1`, string(acct)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PgStore) mirrorWriteReadyTx(ctx context.Context, tx pgx.Tx, acct AccountID) error {
	var pending bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mail_graph_identity WHERE account_id=$1 AND format='mapping')`, string(acct)).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return ErrIdentityMigrationPending
	}
	return nil
}

// Graph adapter format is checked before any provider operation. Resolvers
// must also use this policy for direct Raw/Attachment/product send clients.
func (e *Engine) checkGraphFormat(ctx context.Context, acct AccountID, ad Adapter) error {
	if ad.Provider() != ProviderGraph {
		return nil
	}
	policy, ok := e.store.(interface {
		GraphIdentityFormat(context.Context, AccountID) (string, error)
	})
	if !ok {
		return nil
	}
	format, err := policy.GraphIdentityFormat(ctx, acct)
	if err != nil {
		return err
	}
	dialect, ok := ad.(interface{ GraphIdentityFormat() string })
	if !ok {
		if format == GraphLegacyIDs {
			return nil
		}
		return ErrIdentityMigrationPending
	}
	if dialect.GraphIdentityFormat() != format {
		return ErrIdentityMigrationPending
	}
	return nil
}

type identityResolver interface {
	ResolveIdentity(context.Context, AccountID, MessageID) (MessageID, error)
}

func (e *Engine) resolveIdentity(ctx context.Context, acct AccountID, id MessageID) (MessageID, error) {
	if aliases, ok := e.store.(identityResolver); ok {
		return aliases.ResolveIdentity(ctx, acct, id)
	}
	return id, nil
}
