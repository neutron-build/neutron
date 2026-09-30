package neutronauth

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SQLSessionStore persists revision-fenced sessions in an ordinary SQL table.
// It uses conditional single-statement DML, not a process-local lock.
// NewSQLSessionStore initializes its own table; call once at application setup.
type SQLSessionStore struct{ pool *pgxpool.Pool }

func NewSQLSessionStore(ctx context.Context, pool *pgxpool.Pool) (*SQLSessionStore, error) {
	if pool == nil {
		return nil, errors.New("neutronauth: session SQL pool is nil")
	}
	_, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS neutron_auth_sessions (id TEXT PRIMARY KEY, revision TEXT NOT NULL, data TEXT NOT NULL, expires_at BIGINT)`)
	if err != nil {
		return nil, err
	}
	return &SQLSessionStore{pool: pool}, nil
}
func (s *SQLSessionStore) LoadSession(ctx context.Context, id string) (*SessionRecord, error) {
	var r SessionRecord
	var data string
	var expires *int64
	err := s.pool.QueryRow(ctx, `SELECT revision,data,expires_at FROM neutron_auth_sessions WHERE id=$1`, id).Scan(&r.Version, &data, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expires != nil {
		r.ExpiresAt = time.UnixMilli(*expires)
		if !r.ExpiresAt.After(time.Now()) {
			return nil, nil
		}
	}
	if err = json.Unmarshal([]byte(data), &r.Data); err != nil {
		return nil, err
	}
	return &r, nil
}
func (s *SQLSessionStore) Get(ctx context.Context, id string) (map[string]any, error) {
	r, e := s.LoadSession(ctx, id)
	if r == nil {
		return nil, e
	}
	return r.Data, e
}
func sessionExpiry(ttl time.Duration) any {
	if ttl <= 0 {
		return nil
	}
	return time.Now().Add(ttl).UnixMilli()
}

// Set is for administrative seeding/replacement. Requests use CommitSession.
func (s *SQLSessionStore) Set(ctx context.Context, id string, data map[string]any, ttl time.Duration) error {
	b, e := json.Marshal(data)
	if e != nil {
		return e
	}
	_, e = s.pool.Exec(ctx, `INSERT INTO neutron_auth_sessions(id,revision,data,expires_at) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET revision=EXCLUDED.revision,data=EXCLUDED.data,expires_at=EXCLUDED.expires_at`, id, generateSessionID(), string(b), sessionExpiry(ttl))
	return e
}
func (s *SQLSessionStore) Delete(ctx context.Context, id string) error {
	_, e := s.pool.Exec(ctx, `DELETE FROM neutron_auth_sessions WHERE id=$1`, id)
	return e
}

// CommitSession uses one conditional DML statement. A stale loaded revision
// never falls back to INSERT. Empty expected supports creation at the same ID
// only; absent-ID rotation/revocation is deliberately refused.
func (s *SQLSessionStore) CommitSession(ctx context.Context, id, expected string, next *SessionReplacement, ttl time.Duration) (string, error) {
	var err error
	var data []byte
	if next != nil {
		data, err = json.Marshal(next.Data)
		if err != nil {
			return "", err
		}
	}
	if expected == "" {
		if next == nil || next.ID != id {
			return "", ErrSessionConflict
		}
		version := generateSessionID()
		_, err = s.pool.Exec(ctx, `INSERT INTO neutron_auth_sessions(id,revision,data,expires_at) VALUES($1,$2,$3,$4)`, id, version, string(data), sessionExpiry(ttl))
		if err != nil {
			return "", sessionSQLError(err)
		}
		return version, nil
	}
	if next == nil {
		tag, err := s.pool.Exec(ctx, `DELETE FROM neutron_auth_sessions WHERE id=$1 AND revision=$2 AND (expires_at IS NULL OR expires_at>$3)`, id, expected, time.Now().UnixMilli())
		if err != nil {
			return "", sessionSQLError(err)
		}
		if tag.RowsAffected() != 1 {
			return "", ErrSessionConflict
		}
		return "", nil
	}
	version := generateSessionID()
	tag, err := s.pool.Exec(ctx, `UPDATE neutron_auth_sessions SET id=$3,revision=$4,data=$5,expires_at=$6 WHERE id=$1 AND revision=$2 AND (expires_at IS NULL OR expires_at>$7)`, id, expected, next.ID, version, string(data), sessionExpiry(ttl), time.Now().UnixMilli())
	if err != nil {
		return "", sessionSQLError(err)
	}
	if tag.RowsAffected() != 1 {
		return "", ErrSessionConflict
	}
	return version, nil
}
func sessionSQLError(err error) error {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && (pgerr.Code == "23505" || pgerr.Code == "40001") {
		return ErrSessionConflict
	}
	return err
}

// PurgeExpired removes expired persisted rows. Authentication already refuses
// these rows; applications should schedule maintenance for physical retention.
func (s *SQLSessionStore) PurgeExpired(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM neutron_auth_sessions WHERE expires_at<=$1`, time.Now().UnixMilli())
	return tag.RowsAffected(), err
}
