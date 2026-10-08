package mail

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Real-PostgreSQL coverage for staged reconciliation (audit SYNC-03) and
// the migration runner (audit OPS-02). Same skip contract as
// store_integration_test.go: set NEUTRON_MAIL_TEST_DATABASE_URL.

func seedScanMailbox(t *testing.T, s *PgStore) AccountID {
	t.Helper()
	ctx := context.Background()
	acct := seedAccount(t, s)
	if err := s.PutMailboxes(ctx, acct, []Mailbox{{ID: "INBOX"}, {ID: "Archive"}}); err != nil {
		t.Fatalf("put mailboxes: %v", err)
	}
	envs := []Envelope{
		{ID: HeaderMessageID("<k1@example.com>"), MailboxIDs: []MailboxID{"INBOX"}, Subject: "one"},
		{ID: HeaderMessageID("<k2@example.com>"), MailboxIDs: []MailboxID{"INBOX"}, Subject: "two"},
	}
	if err := s.PutEnvelopes(ctx, acct, envs); err != nil {
		t.Fatalf("seed envelopes: %v", err)
	}
	if err := s.PutCursor(ctx, acct, "INBOX", "stale-cursor"); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}
	return acct
}

func countMessages(t *testing.T, s *PgStore, acct AccountID) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM mail_messages WHERE account_id = $1`, string(acct)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func hasRunningScan(t *testing.T, s *PgStore, acct AccountID, box MailboxID) bool {
	t.Helper()
	_, err := s.RunningScan(context.Background(), acct, box)
	if errors.Is(err, ErrNoStore) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

// The kill-between-pages proof on the real store: every interruption
// leaves the mirror untouched and the scan durable across a full pool
// close/reopen (a process restart), and only FinishScan prunes.
func TestIntegrationStagedScanSurvivesKillBetweenPages(t *testing.T) {
	url := osGetenvTestURL(t)
	s := testStore(t)
	ctx := context.Background()
	acct := seedScanMailbox(t, s)

	mk := func(subject string, id string) Envelope {
		return Envelope{ID: HeaderMessageID(id), MailboxIDs: []MailboxID{"INBOX"}, Subject: subject}
	}
	page1 := mk("one", "<k1@example.com>")
	page2 := mk("three", "<k3@example.com>")

	scan, err := s.BeginScan(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyScanPage(ctx, scan.ID, []Envelope{page1},
		[]MessageID{page1.ID}, nil, "page-1"); err != nil {
		t.Fatal(err)
	}

	// Kill: close the whole pool — a process restart, not a gentle pause.
	s.Close()
	s2, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// The scan survived, its continuation survived, and NOTHING was
	// pruned: both seeded messages are still readable.
	resumed, err := s2.RunningScan(ctx, acct, "INBOX")
	if err != nil {
		t.Fatalf("scan did not survive the restart: %v", err)
	}
	if resumed.ID != scan.ID || resumed.Continuation != "page-1" {
		t.Fatalf("resumed scan = %+v, want id %s continuation page-1", resumed, scan.ID)
	}
	if n := countMessages(t, s2, acct); n != 2 {
		t.Fatalf("restart pruned: %d messages, want 2", n)
	}
	if _, err := s2.Envelope(ctx, acct, HeaderMessageID("<k2@example.com>")); err != nil {
		t.Fatalf("message two was pruned before completion: %v", err)
	}

	// Completion: page 2 stages, FinishScan prunes exactly the absence.
	if err := s2.ApplyScanPage(ctx, resumed.ID, []Envelope{page2},
		[]MessageID{page1.ID, page2.ID}, nil, "terminal"); err != nil {
		t.Fatal(err)
	}
	pruned, err := s2.FinishScan(ctx, acct, "INBOX", resumed.ID, "terminal")
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 1 {
		t.Errorf("pruned = %d, want 1 (message two)", pruned)
	}
	if n := countMessages(t, s2, acct); n != 2 {
		t.Errorf("after completion: %d messages, want 2 (one and three)", n)
	}
	if _, err := s2.Envelope(ctx, acct, HeaderMessageID("<k2@example.com>")); !errors.Is(err, ErrNoStore) {
		t.Error("message absent from the complete listing was not pruned at completion")
	}
	if cur, err := s2.Cursor(ctx, acct, "INBOX"); err != nil || cur != "terminal" {
		t.Errorf("terminal cursor = %q err %v, want terminal published", cur, err)
	}
	if hasRunningScan(t, s2, acct, "INBOX") {
		t.Error("scan rows survived completion")
	}
}

// The prune respects multi-mailbox membership on the real store: a
// message still filed elsewhere keeps its other membership and survives.
func TestIntegrationFinishScanKeepsOtherMailboxMembership(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedScanMailbox(t, s)

	shared := HeaderMessageID("<k1@example.com>")
	if err := s.PutEnvelopes(ctx, acct, []Envelope{
		{ID: shared, MailboxIDs: []MailboxID{"INBOX", "Archive"}, Subject: "shared"},
	}); err != nil {
		t.Fatal(err)
	}

	scan, err := s.BeginScan(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	// The INBOX enumeration omits the shared message (it left INBOX) and
	// reports a new one.
	fresh := HeaderMessageID("<k3@example.com>")
	if err := s.ApplyScanPage(ctx, scan.ID,
		[]Envelope{{ID: fresh, MailboxIDs: []MailboxID{"INBOX"}, Subject: "three"}},
		[]MessageID{fresh}, nil, "done"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinishScan(ctx, acct, "INBOX", scan.ID, "done"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Envelope(ctx, acct, shared); err != nil {
		t.Fatalf("message still filed in Archive was deleted: %v", err)
	}
	boxes, err := s.MessageMailboxes(ctx, acct, shared)
	if err != nil || len(boxes) != 1 || boxes[0] != "Archive" {
		t.Errorf("memberships = %v err %v, want [Archive]", boxes, err)
	}
}

// SYNC-04's foreign keys: a body or membership for a message the mirror
// does not hold is refused at write time, and deleting a message cascades.
func TestIntegrationMirrorForeignKeysRefuseOrphans(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)
	id := HeaderMessageID("<fk@example.com>")
	if err := s.PutEnvelopes(ctx, acct, []Envelope{
		{ID: id, MailboxIDs: []MailboxID{"INBOX"}, Subject: "fk"},
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.PutBody(ctx, acct, &Body{MessageID: HeaderMessageID("<ghost@example.com>"), Text: "x"}); err == nil {
		t.Error("store accepted a body for a message it does not hold")
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO mail_message_mailboxes (account_id, message_id, mailbox_id) VALUES ($1, $2, 'INBOX')`,
		string(acct), string(HeaderMessageID("<ghost2@example.com>"))); err == nil {
		t.Error("database accepted a membership for a missing message")
	}

	if err := s.PutBody(ctx, acct, &Body{MessageID: id, Text: "real"}); err != nil {
		t.Fatalf("legitimate body refused: %v", err)
	}
	if err := s.DeleteMessages(ctx, acct, []MessageID{id}); err != nil {
		t.Fatal(err)
	}
	var bodies, members int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM mail_bodies WHERE account_id=$1`, string(acct)).Scan(&bodies); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM mail_message_mailboxes WHERE account_id=$1`, string(acct)).Scan(&members); err != nil {
		t.Fatal(err)
	}
	if bodies != 0 || members != 0 {
		t.Errorf("after message deletion: bodies=%d members=%d, want cascade to 0", bodies, members)
	}
}

// OPS-02 convergence: a database converged by the OLD boot path (schema
// statements, no ledger, live data) must migrate through the versioned
// runner without losing anything, and the constraints must land.
func TestIntegrationMigrationConvergesPreLedgerDatabase(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedScanMailbox(t, s)

	// Rewind to the pre-runner world: keep the tables and data, forget
	// the ledger and everything after baseline.
	if _, err := s.pool.Exec(ctx, `DROP TABLE mail_migrations`); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`DROP TABLE IF EXISTS mail_graph_id_map`,
		`DROP TABLE IF EXISTS mail_graph_identity`,
		`DROP TABLE IF EXISTS mail_identity_aliases`,
		`ALTER TABLE mail_messages DROP COLUMN attachment_presence`,
		`DROP TABLE IF EXISTS mirror_scans`,
		`DROP TABLE IF EXISTS mirror_scan_seen`,
		`ALTER TABLE mail_bodies DROP CONSTRAINT IF EXISTS mail_bodies_message_fk`,
		`ALTER TABLE mail_message_mailboxes DROP CONSTRAINT IF EXISTS mail_membership_message_fk`,
	} {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("pre-ledger database did not converge: %v", err)
	}

	var versions []int64
	rows, err := s.pool.Query(ctx, `SELECT version FROM mail_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, v)
	}
	if len(versions) != len(EngineMigrations) {
		t.Fatalf("ledger versions = %v, want %d entries", versions, len(EngineMigrations))
	}
	if n := countMessages(t, s, acct); n != 2 {
		t.Errorf("convergence lost data: %d messages, want 2", n)
	}

	// A foreign schema must neither inflate nor satisfy these assertions.
	if _, err := s.pool.Exec(ctx, `CREATE SCHEMA mail_constraint_shadow`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.pool.Exec(ctx, `DROP SCHEMA IF EXISTS mail_constraint_shadow CASCADE`) })
	for _, stmt := range []string{
		`CREATE TABLE mail_constraint_shadow.mail_messages(account_id TEXT,id TEXT,PRIMARY KEY(account_id,id))`,
		`CREATE TABLE mail_constraint_shadow.mail_bodies(account_id TEXT,message_id TEXT,CONSTRAINT mail_bodies_message_fk FOREIGN KEY(account_id,message_id) REFERENCES mail_constraint_shadow.mail_messages(account_id,id) ON DELETE CASCADE)`,
		`CREATE TABLE mail_constraint_shadow.mail_message_mailboxes(account_id TEXT,message_id TEXT,CONSTRAINT mail_membership_message_fk FOREIGN KEY(account_id,message_id) REFERENCES mail_constraint_shadow.mail_messages(account_id,id) ON DELETE CASCADE)`,
	} {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	query := `SELECT COUNT(*) FROM pg_constraint
 WHERE ((conname='mail_bodies_message_fk' AND conrelid='mail_bodies'::regclass)
 OR (conname='mail_membership_message_fk' AND conrelid='mail_message_mailboxes'::regclass))
 AND confrelid='mail_messages'::regclass AND contype='f' AND confdeltype='c'
 AND conkey=ARRAY[1,2]::smallint[] AND confkey=ARRAY[1,2]::smallint[] AND convalidated`
	var constraints int
	if err := s.pool.QueryRow(ctx, query).Scan(&constraints); err != nil {
		t.Fatal(err)
	}
	if constraints != 2 {
		t.Fatalf("target validated FKs=%d, want 2 regardless of foreign schema", constraints)
	}
	// Drop a target FK inside a rollback-only transaction: foreign copies cannot
	// conceal the missing target or make a deficient mirror appear valid.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ALTER TABLE mail_bodies DROP CONSTRAINT mail_bodies_message_fk`); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, query).Scan(&constraints); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if constraints != 1 {
		tx.Rollback(ctx)
		t.Fatalf("foreign schema masked missing target FK: %d", constraints)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// A second run is a no-op, and a modified applied migration refuses
	// to boot.
	before := countMessages(t, s, acct)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("idempotent re-run failed: %v", err)
	}
	if after := countMessages(t, s, acct); after != before {
		t.Errorf("re-run changed data: %d -> %d", before, after)
	}
	tampered := append([]string{}, EngineMigrations[0].Statements...)
	EngineMigrations[0].Statements = []string{`CREATE TABLE IF NOT EXISTS mail_accounts (id TEXT PRIMARY KEY)`}
	defer func() { EngineMigrations[0].Statements = tampered }()
	if err := s.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "modified after application") {
		t.Errorf("tampered applied migration err = %v, want checksum refusal", err)
	}
}

// Two runners on one database: both finish, the ledger holds each version
// exactly once, and the data survives.
func TestIntegrationConcurrentMigrationsSerialize(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	// Start from an empty (dropped) schema but a live store: Drop removed
	// the ledger, so both runners will attempt the full list.
	if err := s.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	// Re-open a second store on the same URL so the two runners use
	// independent pools.
	s2, err := Open(ctx, osGetenvTestURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, st := range []*PgStore{s, s2} {
		wg.Add(1)
		go func(i int, st *PgStore) {
			defer wg.Done()
			errs[i] = st.Migrate(ctx)
		}(i, st)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("runner %d failed: %v", i, err)
		}
	}

	rows, err := s.pool.Query(ctx, `SELECT version, COUNT(*) FROM mail_migrations GROUP BY version ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[int64]int{}
	for rows.Next() {
		var v, n int64
		if err := rows.Scan(&v, &n); err != nil {
			t.Fatal(err)
		}
		got[v] = int(n)
	}
	if len(got) != len(EngineMigrations) {
		t.Fatalf("ledger = %v, want every version once", got)
	}
	for _, m := range EngineMigrations {
		if got[m.Version] != 1 {
			t.Errorf("version %d recorded %d times, want exactly 1", m.Version, got[m.Version])
		}
	}
}

// The advisory-lock proof: while another session holds the migration
// lock, a runner WAITS rather than racing past it.
func TestIntegrationMigrationRunnerWaitsForAdvisoryLock(t *testing.T) {
	s := testStore(t)
	if !s.advisoryReady() {
		t.Skip("backend without advisory locks")
	}
	ctx := context.Background()
	if err := s.Drop(ctx); err != nil {
		t.Fatal(err)
	}

	blocker, err := pgx.Connect(ctx, osGetenvTestURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close(context.Background())
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_lock($1)`, engineLockKey); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- s.Migrate(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("migration completed while the advisory lock was held elsewhere: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_unlock($1)`, engineLockKey); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("migration after lock release failed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("migration never completed after the lock was released")
	}
}

// The SYNC-04 race, run for real: retention-shaped deletions on one pool
// concurrent with sync-shaped writes on another. The account advisory
// lock serializes them; the finish line is zero orphans and intact FKs.
func TestIntegrationConcurrentRetentionAndSyncLeaveNoOrphans(t *testing.T) {
	url := osGetenvTestURL(t)
	s := testStore(t)
	ctx := context.Background()
	acct := seedScanMailbox(t, s)

	writer, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			id := HeaderMessageID(fmt.Sprintf("<race-%d@example.com>", i))
			env := Envelope{ID: id, MailboxIDs: []MailboxID{"INBOX"}, Subject: fmt.Sprintf("race %d", i)}
			if err := writer.PutEnvelopes(ctx, acct, []Envelope{env}); err != nil {
				t.Errorf("writer: %v", err)
				return
			}
			if err := writer.PutBody(ctx, acct, &Body{MessageID: id, Text: "body"}); err != nil && !strings.Contains(err.Error(), "foreign key") {
				t.Errorf("writer body: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			if err := s.DeleteMessages(ctx, acct, []MessageID{
				HeaderMessageID(fmt.Sprintf("<race-%d@example.com>", i)),
				HeaderMessageID(fmt.Sprintf("<k%d@example.com>", i%3)),
			}); err != nil {
				t.Errorf("deleter: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	var orphanBodies, orphanMembers int
	if err := s.pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM mail_bodies b WHERE NOT EXISTS
		     (SELECT 1 FROM mail_messages m WHERE m.account_id=b.account_id AND m.id=b.message_id)),
		  (SELECT COUNT(*) FROM mail_message_mailboxes mm WHERE NOT EXISTS
		     (SELECT 1 FROM mail_messages m WHERE m.account_id=mm.account_id AND m.id=mm.message_id))`).
		Scan(&orphanBodies, &orphanMembers); err != nil {
		t.Fatal(err)
	}
	if orphanBodies != 0 || orphanMembers != 0 {
		t.Errorf("orphans after the race: bodies=%d members=%d, want 0/0", orphanBodies, orphanMembers)
	}
}

func osGetenvTestURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("NEUTRON_MAIL_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set NEUTRON_MAIL_TEST_DATABASE_URL to run store integration tests")
	}
	return url
}

// Scan generations on real PostgreSQL (audit 5 SYNC-05/SYNC-04):
// BeginScanGeneration tags the rows, ApplyScanPage applies destroyed
// evidence in the same transaction as the page, FinishScan records the
// generation's completion marker, and PruneScanDone bounds the markers
// to the live generation.
func TestIntegrationScanGenerationsAndDestroyedEvidence(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedScanMailbox(t, s)

	scan, err := s.BeginScanGeneration(ctx, acct, "INBOX", 7)
	if err != nil {
		t.Fatal(err)
	}
	if scan.Generation != 7 {
		t.Fatalf("scan generation = %d, want 7", scan.Generation)
	}

	// Page one stages the whole mailbox as seen...
	seen := []MessageID{HeaderMessageID("<k1@example.com>"), HeaderMessageID("<k2@example.com>")}
	if err := s.ApplyScanPage(ctx, scan.ID, nil, seen, nil, "page-1"); err != nil {
		t.Fatal(err)
	}
	// ...then the provider destroys k1 before completion: the destroyed ID
	// leaves BOTH the staged seen set and the live membership.
	destroyed := []MessageID{HeaderMessageID("<k1@example.com>")}
	if err := s.ApplyScanPage(ctx, scan.ID, nil, nil, destroyed, "terminal"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinishScan(ctx, acct, "INBOX", scan.ID, "terminal"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Envelope(ctx, acct, HeaderMessageID("<k1@example.com>")); !errors.Is(err, ErrNoStore) {
		t.Error("the destroyed message survived the scan's negative evidence")
	}
	if _, err := s.Envelope(ctx, acct, HeaderMessageID("<k2@example.com>")); err != nil {
		t.Errorf("the live message did not survive: %v", err)
	}

	done, err := s.ScanDone(ctx, acct, "INBOX", 7)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("the generation's completion marker was not recorded")
	}
	if err := s.PruneScanDone(ctx, acct, 8); err != nil {
		t.Fatal(err)
	}
	done, err = s.ScanDone(ctx, acct, "INBOX", 7)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Error("PruneScanDone kept another generation's markers")
	}
}

// Failure after final-page staging, pruning, and cursor publication must
// roll back all of them. After a successful retry, reopening the pool must
// see a completed generation and must never enumerate the mailbox again.
func TestIntegrationFinalScanPageAtomicRollbackAndRestart(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedScanMailbox(t, s)
	one := HeaderMessageID("<k1@example.com>")
	two := HeaderMessageID("<k2@example.com>")
	three := HeaderMessageID("<k3@example.com>")
	scan, err := s.BeginScanGeneration(ctx, acct, "INBOX", 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyScanPage(ctx, scan.ID, nil, []MessageID{one}, nil, "last-page"); err != nil {
		t.Fatal(err)
	}

	// The completion marker is written after staging and pruning. Failing
	// there proves that even those earlier statements share its transaction.
	if _, err := s.pool.Exec(ctx, `ALTER TABLE mail_scan_done ADD CONSTRAINT reject_test_generation CHECK (generation <> 7)`); err != nil {
		t.Fatal(err)
	}
	finalEnvs := []Envelope{{ID: three, MailboxIDs: []MailboxID{"INBOX"}, Subject: "three"}}
	if _, err := s.ApplyFinalScanPage(ctx, scan.ID, finalEnvs, []MessageID{three}, []MessageID{one}, "incremental-terminal"); err == nil {
		t.Fatal("terminal transaction unexpectedly ignored the failing completion marker")
	}
	running, err := s.RunningScan(ctx, acct, "INBOX")
	if err != nil || running.ID != scan.ID || running.Generation != 7 || running.Continuation != "last-page" {
		t.Fatalf("rollback changed the scan: %+v, err %v", running, err)
	}
	if cursor, err := s.Cursor(ctx, acct, "INBOX"); err != nil || cursor != "stale-cursor" {
		t.Fatalf("rollback published cursor %q, err %v", cursor, err)
	}
	for _, id := range []MessageID{one, two} {
		if _, err := s.Envelope(ctx, acct, id); err != nil {
			t.Fatalf("rollback lost original message %s: %v", id, err)
		}
	}
	if _, err := s.Envelope(ctx, acct, three); !errors.Is(err, ErrNoStore) {
		t.Fatalf("rollback retained terminal-page envelope: %v", err)
	}
	var seen int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM mirror_scan_seen WHERE scan_id=$1 AND message_id=$2`, string(scan.ID), string(one)).Scan(&seen); err != nil || seen != 1 {
		t.Fatalf("rollback lost prior positive evidence: count %d, err %v", seen, err)
	}
	if done, err := s.ScanDone(ctx, acct, "INBOX", 7); err != nil || done {
		t.Fatalf("rollback recorded completion: %v, err %v", done, err)
	}

	if _, err := s.pool.Exec(ctx, `ALTER TABLE mail_scan_done DROP CONSTRAINT reject_test_generation`); err != nil {
		t.Fatal(err)
	}
	pruned, err := s.ApplyFinalScanPage(ctx, scan.ID, finalEnvs, []MessageID{three}, []MessageID{one}, "incremental-terminal")
	if err != nil || pruned != 1 {
		t.Fatalf("final-page retry pruned %d, err %v, want message two pruned", pruned, err)
	}
	s.Close()
	reopened, err := Open(ctx, osGetenvTestURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertCompletedScan(t, reopened, acct, 7, "incremental-terminal")
	if n := countMessages(t, reopened, acct); n != 1 {
		t.Fatalf("completed scan holds %d messages, want only message three", n)
	}
	if _, err := reopened.Envelope(ctx, acct, three); err != nil {
		t.Fatalf("final envelope did not survive reopening: %v", err)
	}
	var stagedRows int
	if err := reopened.pool.QueryRow(ctx, `SELECT COUNT(*) FROM mirror_scan_seen WHERE scan_id=$1`, string(scan.ID)).Scan(&stagedRows); err != nil || stagedRows != 0 {
		t.Fatalf("completed scan left %d staged rows, err %v", stagedRows, err)
	}

	eng := NewEngine(reopened, discardLogger())
	ad := &scriptedAdapter{boxes: []Mailbox{{ID: "INBOX"}}}
	if err := eng.RequestRescanVersion(ctx, acct, ad, 7); err != nil {
		t.Fatal(err)
	}
	assertCompletedScan(t, reopened, acct, 7, "incremental-terminal")
	if _, err := eng.SyncMailbox(ctx, acct, "INBOX", ad); err != nil {
		t.Fatal(err)
	}
	if len(ad.seenCursors) != 1 || ad.seenCursors[0] != "incremental-terminal" {
		t.Fatalf("completed scan re-enumerated after restart: %v", ad.seenCursors)
	}
}

func TestIntegrationFinalScanPageRejectsReplacedScan(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedScanMailbox(t, s)
	old, err := s.BeginScanGeneration(ctx, acct, "INBOX", 7)
	if err != nil {
		t.Fatal(err)
	}
	current, err := s.BeginScanGeneration(ctx, acct, "INBOX", 8)
	if err != nil {
		t.Fatal(err)
	}
	id := HeaderMessageID("<stale-page@example.com>")
	if _, err := s.ApplyFinalScanPage(ctx, old.ID, []Envelope{{ID: id, MailboxIDs: []MailboxID{"INBOX"}}}, []MessageID{id}, nil, "obsolete-terminal"); !errors.Is(err, ErrNoStore) {
		t.Fatalf("stale final-page error = %v, want ErrNoStore", err)
	}
	running, err := s.RunningScan(ctx, acct, "INBOX")
	if err != nil || running.ID != current.ID || running.Generation != 8 || running.Continuation != "" {
		t.Fatalf("stale page changed replacement scan: %+v, err %v", running, err)
	}
	if n := countMessages(t, s, acct); n != 2 {
		t.Fatalf("stale final page changed live messages: count %d, want 2", n)
	}
}
