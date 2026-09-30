package neutronauth

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SQLWebAuthnStore provides persistent single-use ceremonies and atomic
// credential revision commits across processes. Store calls have a5s budget.
// Protect these tables with the application's database principal; they contain
// authentication state and must never be writable by untrusted SQL users.
type SQLWebAuthnStore struct{ pool *pgxpool.Pool }

func NewSQLWebAuthnStore(ctx context.Context, pool *pgxpool.Pool) (*SQLWebAuthnStore, error) {
	if pool == nil {
		return nil, errors.New("neutronauth: WebAuthn SQL pool is nil")
	}
	for _, ddl := range []string{`CREATE TABLE IF NOT EXISTS neutron_auth_webauthn_credentials (credential_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,revision TEXT NOT NULL,data TEXT NOT NULL)`, `CREATE TABLE IF NOT EXISTS neutron_auth_webauthn_ceremonies (id TEXT PRIMARY KEY,state TEXT NOT NULL,expires_at BIGINT NOT NULL)`} {
		if _, e := pool.Exec(ctx, ddl); e != nil {
			return nil, e
		}
	}
	return &SQLWebAuthnStore{pool: pool}, nil
}
func webAuthnStoreContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
func (s *SQLWebAuthnStore) StoreChallenge(key, state string, ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("neutronauth: ceremony TTL must be positive")
	}
	ctx, cancel := webAuthnStoreContext()
	defer cancel()
	_, e := s.pool.Exec(ctx, `INSERT INTO neutron_auth_webauthn_ceremonies(id,state,expires_at) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET state=EXCLUDED.state,expires_at=EXCLUDED.expires_at`, key, state, time.Now().Add(ttl).UnixMilli())
	return e
}
func (s *SQLWebAuthnStore) GetChallenge(key string) (string, error) {
	ctx, cancel := webAuthnStoreContext()
	defer cancel()
	var state string
	var expires int64
	e := s.pool.QueryRow(ctx, `DELETE FROM neutron_auth_webauthn_ceremonies WHERE id=$1 RETURNING state,expires_at`, key).Scan(&state, &expires)
	if e != nil {
		return "", e
	}
	if expires <= time.Now().UnixMilli() {
		return "", errors.New("neutronauth: ceremony expired")
	}
	return state, nil
}
func (s *SQLWebAuthnStore) SaveCredential(c WebAuthnCredential) error {
	data, e := json.Marshal(c)
	if e != nil {
		return e
	}
	ctx, cancel := webAuthnStoreContext()
	defer cancel()
	_, e = s.pool.Exec(ctx, `INSERT INTO neutron_auth_webauthn_credentials(credential_id,user_id,revision,data) VALUES($1,$2,$3,$4)`, c.CredentialID, c.UserID, c.Revision, string(data))
	return e
}
func (s *SQLWebAuthnStore) GetCredentialsByUser(user string) ([]WebAuthnCredential, error) {
	ctx, cancel := webAuthnStoreContext()
	defer cancel()
	rows, e := s.pool.Query(ctx, `SELECT data FROM neutron_auth_webauthn_credentials WHERE user_id=$1`, user)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	result := []WebAuthnCredential{}
	for rows.Next() {
		var raw string
		var c WebAuthnCredential
		if e = rows.Scan(&raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal([]byte(raw), &c); e != nil {
			return nil, e
		}
		result = append(result, c)
	}
	return result, rows.Err()
}
func (s *SQLWebAuthnStore) CompareAndSwapCredential(id, expected string, c WebAuthnCredential) error {
	if id != c.CredentialID || expected == "" || c.Revision == "" || c.Revision == expected {
		return ErrCredentialConflict
	}
	data, e := json.Marshal(c)
	if e != nil {
		return e
	}
	ctx, cancel := webAuthnStoreContext()
	defer cancel()
	tag, e := s.pool.Exec(ctx, `UPDATE neutron_auth_webauthn_credentials SET revision=$1,data=$2 WHERE credential_id=$3 AND revision=$4 AND user_id=$5`, c.Revision, string(data), id, expected, c.UserID)
	if e != nil {
		return e
	}
	if tag.RowsAffected() != 1 {
		return ErrCredentialConflict
	}
	return nil
}

// UpdateSignCount remains a legacy administrative operation. It reads a
// revision then performs the same conditional commit; no blind counter update.
func (s *SQLWebAuthnStore) UpdateSignCount(id string, count uint32) error {
	ctx, cancel := webAuthnStoreContext()
	defer cancel()
	var raw string
	e := s.pool.QueryRow(ctx, `SELECT data FROM neutron_auth_webauthn_credentials WHERE credential_id=$1`, id).Scan(&raw)
	if errors.Is(e, pgx.ErrNoRows) {
		return ErrCredentialConflict
	}
	if e != nil {
		return e
	}
	var c WebAuthnCredential
	if e = json.Unmarshal([]byte(raw), &c); e != nil {
		return e
	}
	expected := c.Revision
	c.Revision = generateSessionID()
	c.SignCount = count
	if c.VerifiedCredential != nil {
		c.VerifiedCredential.Authenticator.SignCount = count
	}
	return s.CompareAndSwapCredential(id, expected, c)
}

// PurgeExpiredCeremonies removes abandoned expired challenges. Run periodically
// for physical retention; expired challenges already fail authentication.
func (s *SQLWebAuthnStore) PurgeExpiredCeremonies(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM neutron_auth_webauthn_ceremonies WHERE expires_at<=$1`, time.Now().UnixMilli())
	return tag.RowsAffected(), err
}
