package mail

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func configureIdentityFixture(t *testing.T, s *PgStore, acct AccountID) {
	t.Helper()
	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE TABLE identity_test_owner(account_id TEXT PRIMARY KEY,active BOOLEAN NOT NULL)`,
		`CREATE TABLE identity_test_filing(account_id TEXT,message_id TEXT,bucket TEXT,read_at TEXT,snooze TEXT,revision BIGINT,PRIMARY KEY(account_id,message_id))`,
		`CREATE TABLE identity_test_receipt(account_id TEXT,message_id TEXT,receipt TEXT,PRIMARY KEY(account_id,message_id))`,
	} {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, name := range []string{"identity_test_owner", "identity_test_filing", "identity_test_receipt"} {
			_, _ = s.pool.Exec(ctx, "DROP TABLE IF EXISTS "+name)
		}
	})
	if _, err := s.pool.Exec(ctx, `INSERT INTO identity_test_owner VALUES($1,true)`, string(acct)); err != nil {
		t.Fatal(err)
	}
	s.beforeAccountWrite = func(ctx context.Context, tx IdentityTx, acct AccountID) error {
		var active bool
		if err := tx.QueryRow(ctx, `SELECT active FROM identity_test_owner WHERE account_id=$1 FOR UPDATE`, string(acct)).Scan(&active); err != nil {
			return err
		}
		if !active {
			return errors.New("inactive owner/account")
		}
		return nil
	}
	s.remap = func(ctx context.Context, tx IdentityTx, acct AccountID, pairs []IdentityPair) error {
		for _, p := range pairs {
			for _, name := range []string{"identity_test_filing", "identity_test_receipt"} {
				if _, err := tx.Exec(ctx, "UPDATE "+name+" SET message_id=$3 WHERE account_id=$1 AND message_id=$2", string(acct), string(p.OldID), string(p.NewID)); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func seedPromotionFixture(t *testing.T, s *PgStore, acct AccountID) (Envelope, IdentityPair) {
	t.Helper()
	ctx := context.Background()
	old := PositionalMessageID("INBOX", 1, 7)
	newID := HeaderMessageID("promote@example.com")
	env := Envelope{ID: old, ThreadID: "retained-thread", MailboxIDs: []MailboxID{"INBOX", "Archive"}, Subject: "old"}
	if err := s.PutEnvelopes(ctx, acct, []Envelope{env}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutBody(ctx, acct, &Body{MessageID: old, Text: "retained body", Parts: []BodyPart{{PartID: "1", Filename: "proof.txt"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO identity_test_filing VALUES($1,$2,'feed','exact-read','exact-snooze',7)`, string(acct), string(old)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO identity_test_receipt VALUES($1,$2,'push-receipt')`, string(acct), string(old)); err != nil {
		t.Fatal(err)
	}
	env.ID = newID
	env.Subject = "new"
	env.MailboxIDs = []MailboxID{"INBOX"}
	return env, IdentityPair{old, newID}
}

func TestIntegrationIdentityPromotionRollbackAndRestart(t *testing.T) {
	// Trigger faults exercise the actual production SQL after every durable phase,
	// not a memory double. Failed commits retain old state and the retry cursor.
	for _, phase := range []struct{ table, event string }{
		{"mail_messages", "INSERT"}, {"mail_message_mailboxes", "INSERT"},
		{"mail_bodies", "INSERT"}, {"identity_test_filing", "UPDATE"},
		{"identity_test_receipt", "UPDATE"}, {"mail_identity_aliases", "INSERT"},
		{"mail_messages", "DELETE"}, {"mail_sync_state", "INSERT"},
	} {
		t.Run(phase.table+phase.event, func(t *testing.T) {
			s := testStore(t)
			ctx := context.Background()
			acct := seedAccount(t, s)
			configureIdentityFixture(t, s, acct)
			env, pair := seedPromotionFixture(t, s, acct)
			if _, err := s.pool.Exec(ctx, `CREATE FUNCTION identity_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected phase failure'; END $$`); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = s.pool.Exec(ctx, `DROP FUNCTION IF EXISTS identity_test_fail() CASCADE`) })
			if _, err := s.pool.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER identity_test_fail AFTER %s ON %s FOR EACH ROW EXECUTE FUNCTION identity_test_fail()`, phase.event, phase.table)); err != nil {
				t.Fatal(err)
			}
			err := s.ApplyIdentityPage(ctx, acct, "INBOX", []Envelope{env}, []IdentityPair{pair}, nil, "terminal")
			if err == nil {
				t.Fatal("fault did not abort page")
			}
			if _, err := s.Envelope(ctx, acct, pair.OldID); err != nil {
				t.Fatal("old envelope lost", err)
			}
			if _, err := s.Envelope(ctx, acct, pair.NewID); !errors.Is(err, ErrNoStore) {
				t.Fatal("partial replacement survived", err)
			}
			if cur, _ := s.Cursor(ctx, acct, "INBOX"); cur != "" {
				t.Fatal("cursor advanced", cur)
			}
			if _, err := s.pool.Exec(ctx, `DROP FUNCTION identity_test_fail() CASCADE`); err != nil {
				t.Fatal(err)
			}
			// A fresh pool retries the prior write set, simulating a stopped process.
			reopened, err := OpenWithIdentityPolicy(ctx, osGetenvTestURL(t), s.beforeAccountWrite, s.remap)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if err := reopened.ApplyIdentityPage(ctx, acct, "INBOX", []Envelope{env}, []IdentityPair{pair}, nil, "terminal"); err != nil {
				t.Fatal(err)
			}
			assertPromotedFixture(t, reopened, acct, pair)
			if err := reopened.ApplyIdentityPage(ctx, acct, "INBOX", []Envelope{env}, []IdentityPair{pair}, nil, "terminal"); err != nil {
				t.Fatal("idempotent retry", err)
			}
			assertPromotedFixture(t, reopened, acct, pair)
		})
	}
}
func assertPromotedFixture(t *testing.T, s *PgStore, acct AccountID, p IdentityPair) {
	t.Helper()
	ctx := context.Background()
	body, err := s.Body(ctx, acct, p.NewID)
	if err != nil || body.Text != "retained body" {
		t.Fatal("body not preserved", body, err)
	}
	boxes, err := s.MessageMailboxes(ctx, acct, p.NewID)
	if err != nil || len(boxes) != 2 {
		t.Fatal("memberships lost", boxes, err)
	}
	var bucket, read, snooze string
	var revision int64
	if err := s.pool.QueryRow(ctx, `SELECT bucket,read_at,snooze,revision FROM identity_test_filing WHERE account_id=$1 AND message_id=$2`, string(acct), string(p.NewID)).Scan(&bucket, &read, &snooze, &revision); err != nil {
		t.Fatal(err)
	}
	if bucket != "feed" || read != "exact-read" || snooze != "exact-snooze" || revision != 7 {
		t.Fatal("filing lost")
	}
	var receipt string
	if err := s.pool.QueryRow(ctx, `SELECT receipt FROM identity_test_receipt WHERE account_id=$1 AND message_id=$2`, string(acct), string(p.NewID)).Scan(&receipt); err != nil || receipt != "push-receipt" {
		t.Fatal("receipt lost", err)
	}
	resolved, err := s.ResolveIdentity(ctx, acct, p.OldID)
	if err != nil || resolved != p.NewID {
		t.Fatal("alias failed", resolved, err)
	}
	var oldCount int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM mail_messages WHERE account_id=$1 AND id=$2`, string(acct), string(p.OldID)).Scan(&oldCount); err != nil || oldCount != 0 {
		t.Fatal("old row not retired", err)
	}
}

func TestIntegrationIdentityCollisionAndAdmissionRefuse(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)
	configureIdentityFixture(t, s, acct)
	env, pair := seedPromotionFixture(t, s, acct)
	if err := s.PutEnvelopes(ctx, acct, []Envelope{{ID: pair.NewID, Subject: "conflicting target"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyIdentityPage(ctx, acct, "INBOX", []Envelope{env}, []IdentityPair{pair}, nil, "bad"); !errors.Is(err, ErrIdentityCollision) {
		t.Fatal("collision not refused", err)
	}
	got, err := s.Envelope(ctx, acct, pair.NewID)
	if err != nil || got.Subject != "conflicting target" {
		t.Fatal("collision overwrote target", err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE identity_test_owner SET active=false`); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteMessages(ctx, acct, []MessageID{pair.OldID}); err == nil {
		t.Fatal("inactive owner retired identity")
	}
	if _, err := s.Envelope(ctx, acct, pair.OldID); err != nil {
		t.Fatal("old lost", err)
	}
	// Missing admission cannot silently bypass the product owner lock.
	if _, err := OpenWithIdentityPolicy(ctx, "unused", nil, s.remap); !errors.Is(err, ErrIdentityTransactionRequired) {
		t.Fatal(err)
	}
}

func TestIntegrationIdentityScanFinalCallbackRollback(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)
	configureIdentityFixture(t, s, acct)
	env, pair := seedPromotionFixture(t, s, acct)
	scan, err := s.BeginScan(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	// Another mailbox's in-progress seen evidence must also follow the identity.
	other, err := s.BeginScan(ctx, acct, "Archive")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyScanPage(ctx, other.ID, nil, []MessageID{pair.OldID}, nil, "other"); err != nil {
		t.Fatal(err)
	}
	remap := s.remap
	s.remap = func(ctx context.Context, tx IdentityTx, acct AccountID, pairs []IdentityPair) error {
		if err := remap(ctx, tx, acct, pairs); err != nil {
			return err
		}
		return errors.New("after product writes")
	}
	if _, err := s.ApplyIdentityScanPage(ctx, scan.ID, []Envelope{env}, []IdentityPair{pair}, []MessageID{pair.NewID}, nil, "terminal", true); err == nil {
		t.Fatal("callback failure committed")
	}
	resumed, err := s.RunningScan(ctx, acct, "INBOX")
	if err != nil || resumed.Continuation != "" {
		t.Fatal("scan advanced", err)
	}
	s.remap = remap
	if _, err := s.ApplyIdentityScanPage(ctx, scan.ID, []Envelope{env}, []IdentityPair{pair}, []MessageID{pair.NewID}, nil, "terminal", true); err != nil {
		t.Fatal(err)
	}
	assertPromotedFixture(t, s, acct, pair)
	var seen bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mirror_scan_seen WHERE scan_id=$1 AND message_id=$2)`, string(other.ID), string(pair.NewID)).Scan(&seen); err != nil || !seen {
		t.Fatal("other scan evidence lost", err)
	}
	if _, err := s.RunningScan(ctx, acct, "INBOX"); !errors.Is(err, ErrNoStore) {
		t.Fatal("terminal scan remains", err)
	}
}

var _ IdentityTx = (pgx.Tx)(nil)

func TestIntegrationIdentityAdmissionPrecedesAccountLockOnSameTransaction(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)
	configureIdentityFixture(t, s, acct)
	env, pair := seedPromotionFixture(t, s, acct)
	admission, remap := s.beforeAccountWrite, s.remap
	var admittedXID int64
	s.beforeAccountWrite = func(ctx context.Context, tx IdentityTx, acct AccountID) error {
		var held bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND classid=(($1::bigint >> 32) & 4294967295)::oid AND objid=($1::bigint & 4294967295)::oid)`, AccountLockKey(acct)).Scan(&held); err != nil {
			return err
		}
		if held {
			return errors.New("account lock preceded owner admission")
		}
		if err := admission(ctx, tx, acct); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&admittedXID)
	}
	s.remap = func(ctx context.Context, tx IdentityTx, acct AccountID, pairs []IdentityPair) error {
		var xid int64
		if err := tx.QueryRow(ctx, `SELECT txid_current()`).Scan(&xid); err != nil {
			return err
		}
		if xid != admittedXID {
			return errors.New("owner admission used another transaction")
		}
		probe, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer probe.Rollback(ctx)
		_, err = probe.Exec(ctx, `SELECT account_id FROM identity_test_owner WHERE account_id=$1 FOR UPDATE NOWAIT`, string(acct))
		if err == nil {
			return errors.New("owner row lock was released before remap")
		}
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "55P03" {
			return err
		}
		return remap(ctx, tx, acct, pairs)
	}
	if err := s.ApplyIdentityPage(ctx, acct, "INBOX", []Envelope{env}, []IdentityPair{pair}, nil, "done"); err != nil {
		t.Fatal(err)
	}
	assertPromotedFixture(t, s, acct, pair)
}

func TestIntegrationIdentityDeferredCommitFailureRollsBack(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)
	configureIdentityFixture(t, s, acct)
	env, pair := seedPromotionFixture(t, s, acct)
	if _, err := s.pool.Exec(ctx, `CREATE FUNCTION identity_commit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected commit failure'; END $$`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.pool.Exec(ctx, `DROP FUNCTION IF EXISTS identity_commit_fail() CASCADE`) })
	if _, err := s.pool.Exec(ctx, `CREATE CONSTRAINT TRIGGER identity_commit_fail AFTER INSERT ON mail_identity_aliases DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION identity_commit_fail()`); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyIdentityPage(ctx, acct, "INBOX", []Envelope{env}, []IdentityPair{pair}, nil, "done"); err == nil {
		t.Fatal("deferred commit failure accepted")
	}
	old, err := s.Envelope(ctx, acct, pair.OldID)
	if err != nil || old.ID != pair.OldID {
		t.Fatal("commit failure lost old row", err)
	}
	if cur, _ := s.Cursor(ctx, acct, "INBOX"); cur != "" {
		t.Fatal("commit failure advanced cursor")
	}
	var key MessageID
	if err := s.pool.QueryRow(ctx, `SELECT message_id FROM identity_test_filing WHERE account_id=$1`, string(acct)).Scan(&key); err != nil || key != pair.OldID {
		t.Fatal("commit failure persisted remap", err)
	}
}

func TestIntegrationEnginePromotionDeltaAndStagedRouting(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(fmt.Sprint(staged), func(t *testing.T) {
			s := testStore(t)
			ctx := context.Background()
			acct := seedAccount(t, s)
			configureIdentityFixture(t, s, acct)
			env, pair := seedPromotionFixture(t, s, acct)
			env.ID = pair.OldID
			env.MessageIDHeader = "promote@example.com"
			env.ThreadID = "new-incoming-thread"
			ad := &scriptedAdapter{pages: []*Changes{{Changes: []Change{{Kind: ChangeUpdated, ID: pair.OldID, Envelope: &env}}, Next: "terminal", EnumerationStart: staged, Complete: staged}}}
			report, err := NewEngine(s, nil).SyncMailbox(ctx, acct, "INBOX", ad)
			if err != nil || report.Upgraded != 1 {
				t.Fatal(report, err)
			}
			assertPromotedFixture(t, s, acct, pair)
			got, err := s.Envelope(ctx, acct, pair.NewID)
			if err != nil || got.ThreadID != "retained-thread" {
				t.Fatal("promotion changed board thread identity", got, err)
			}
			cur, err := s.Cursor(ctx, acct, "INBOX")
			if err != nil || cur != "terminal" {
				t.Fatal("cursor not in page commit", cur, err)
			}
		})
	}
}

func TestIntegrationIdentityMissingSourceStillRemapsAndRetainsReplayThread(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)
	configureIdentityFixture(t, s, acct)
	pair := IdentityPair{PositionalMessageID("INBOX", 1, 9), HeaderMessageID("missing@example.com")}
	if _, err := s.pool.Exec(ctx, `INSERT INTO identity_test_receipt VALUES($1,$2,'retained')`, string(acct), string(pair.OldID)); err != nil {
		t.Fatal(err)
	}
	env := Envelope{ID: pair.NewID, ThreadID: "original-thread", MailboxIDs: []MailboxID{"INBOX"}}
	if err := s.ApplyIdentityPage(ctx, acct, "INBOX", []Envelope{env}, []IdentityPair{pair}, nil, "one"); err != nil {
		t.Fatal(err)
	}
	env.ThreadID = "incoming-change"
	s.remap = func(context.Context, IdentityTx, AccountID, []IdentityPair) error {
		return errors.New("replay called remap twice")
	}
	if err := s.ApplyIdentityPage(ctx, acct, "INBOX", []Envelope{env}, []IdentityPair{pair}, nil, "two"); err != nil {
		t.Fatal(err)
	}
	got, err := s.Envelope(ctx, acct, pair.NewID)
	if err != nil || got.ThreadID != "original-thread" {
		t.Fatal("replay changed thread", got, err)
	}
	var id MessageID
	if err := s.pool.QueryRow(ctx, `SELECT message_id FROM identity_test_receipt WHERE account_id=$1`, string(acct)).Scan(&id); err != nil || id != pair.NewID {
		t.Fatal("expired source reference lost", err)
	}
}
