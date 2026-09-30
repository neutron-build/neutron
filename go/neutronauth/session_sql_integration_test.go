package neutronauth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func authLivePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("NEUTRON_AUTH_TEST_DATABASE_URL")
	if url == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_AUTH_TEST_DATABASE_URL required")
		}
		t.Skip("live auth database not configured")
	}
	pool, e := pgxpool.New(context.Background(), url)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(pool.Close)
	if e = pool.Ping(context.Background()); e != nil {
		t.Fatal(e)
	}
	return pool
}
func TestSQLSessionAtomicRevocationRotationAndCAS(t *testing.T) {
	pool := authLivePool(t)
	ctx := context.Background()
	store, e := NewSQLSessionStore(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	prefix := fmt.Sprintf("finish_auth_%d", time.Now().UnixNano())
	ids := []string{prefix + "_old", prefix + "_new", prefix + "_collision", prefix + "_fresh"}
	t.Cleanup(func() {
		for _, id := range ids {
			store.Delete(context.Background(), id)
		}
	})
	if e = store.Set(ctx, ids[0], map[string]any{"user": "alice"}, time.Hour); e != nil {
		t.Fatal(e)
	}
	record, e := store.LoadSession(ctx, ids[0])
	if e != nil {
		t.Fatal(e)
	}
	if e = store.Set(ctx, ids[2], map[string]any{"user": "bob"}, time.Hour); e != nil {
		t.Fatal(e)
	}
	if _, e = store.CommitSession(ctx, ids[0], record.Version, &SessionReplacement{ID: ids[2], Data: record.Data}, time.Hour); !errors.Is(e, ErrSessionConflict) {
		t.Fatalf("collision: %v", e)
	}
	if r, e := store.LoadSession(ctx, ids[0]); e != nil || r == nil || r.Version != record.Version {
		t.Fatalf("collision did not rollback source: %+v %v", r, e)
	}
	if _, e = store.CommitSession(ctx, ids[0], record.Version, &SessionReplacement{ID: ids[1], Data: record.Data}, time.Hour); e != nil {
		t.Fatal(e)
	}
	if r, e := store.LoadSession(ctx, ids[0]); e != nil || r != nil {
		t.Fatalf("rotation left old record: %+v %v", r, e)
	}
	if _, e = store.CommitSession(ctx, ids[0], record.Version, &SessionReplacement{ID: ids[0], Data: record.Data}, time.Hour); !errors.Is(e, ErrSessionConflict) {
		t.Fatal("stale session recreated old ID", e)
	}
	current, e := store.LoadSession(ctx, ids[1])
	if e != nil {
		t.Fatal(e)
	}
	// Two independent connections contend on the same loaded revision:
	// exactly one may publish. The adapter never replaces this with
	// an in-process store lock.
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			_, e := store.CommitSession(ctx, ids[1], current.Version, &SessionReplacement{ID: ids[1], Data: map[string]any{"winner": n}}, time.Hour)
			results <- e
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	conflicts := 0
	for e := range results {
		if e == nil {
			success++
		} else if errors.Is(e, ErrSessionConflict) {
			conflicts++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatalf("CAS results success%d conflict%d", success, conflicts)
	}
	current, e = store.LoadSession(ctx, ids[1])
	if e != nil {
		t.Fatal(e)
	}
	if _, e = store.CommitSession(ctx, ids[1], current.Version, nil, time.Hour); e != nil {
		t.Fatal(e)
	}
	if _, e = store.CommitSession(ctx, ids[1], current.Version, &SessionReplacement{ID: ids[1], Data: current.Data}, time.Hour); !errors.Is(e, ErrSessionConflict) {
		t.Fatal("revoked ID restored", e)
	}
	if _, e = store.CommitSession(ctx, ids[3], "", &SessionReplacement{ID: ids[3], Data: map[string]any{"created": true}}, time.Hour); e != nil {
		t.Fatal("atomic create", e)
	}
	if _, e = store.CommitSession(ctx, ids[3], "", &SessionReplacement{ID: ids[0], Data: map[string]any{"wrong": true}}, time.Hour); !errors.Is(e, ErrSessionConflict) {
		t.Fatal("empty token overwrote existing identity", e)
	}
}
func TestSQLWebAuthnSingleUseAndCredentialCAS(t *testing.T) {
	pool := authLivePool(t)
	ctx := context.Background()
	store, e := NewSQLWebAuthnStore(ctx, pool)
	if e != nil {
		t.Fatal(e)
	}
	user := fmt.Sprintf("finish_auth_web_%d", time.Now().UnixNano())
	key := "reg:" + user
	defer pool.Exec(ctx, `DELETE FROM neutron_auth_webauthn_credentials WHERE user_id=$1`, user)
	defer pool.Exec(ctx, `DELETE FROM neutron_auth_webauthn_ceremonies WHERE id=$1`, key)
	if e = store.StoreChallenge(key, "entire serialized ceremony", time.Minute); e != nil {
		t.Fatal(e)
	}
	results := make(chan error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			state, e := store.GetChallenge(key)
			if e == nil && state != "entire serialized ceremony" {
				e = fmt.Errorf("wrong state%q", state)
			}
			results <- e
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for e := range results {
		if e == nil {
			success++
		} else if !errors.Is(e, pgx.ErrNoRows) {
			t.Fatalf("unexpected challenge failure: %v", e)
		}
	}
	if success != 1 {
		t.Fatalf("single-use successes%d", success)
	}
	c := WebAuthnCredential{CredentialID: user, UserID: user, Revision: generateSessionID()}
	if e = store.SaveCredential(c); e != nil {
		t.Fatal(e)
	}
	records, e := store.GetCredentialsByUser(user)
	if e != nil || len(records) != 1 {
		t.Fatalf("persisted %+v %v", records, e)
	}
	old := c.Revision
	c.Revision = generateSessionID()
	c.SignCount = 2
	if e = store.CompareAndSwapCredential(c.CredentialID, old, c); e != nil {
		t.Fatal(e)
	}
	c.Revision = generateSessionID()
	if e = store.CompareAndSwapCredential(c.CredentialID, old, c); !errors.Is(e, ErrCredentialConflict) {
		t.Fatalf("credential stale commit%v", e)
	}
	rows, e := store.GetCredentialsByUser(user)
	if e != nil || len(rows) != 1 || rows[0].SignCount != 2 {
		t.Fatalf("counter persistence %+v %v", rows, e)
	}
	if e = store.StoreChallenge(key, "expired", time.Nanosecond); e != nil {
		t.Fatal(e)
	}
	time.Sleep(time.Millisecond)
	if _, e = store.GetChallenge(key); e == nil {
		t.Fatal("expired SQL ceremony accepted")
	}
	// Only names from this run are removed, never shared tables or other tests.
	if !strings.HasPrefix(user, "finish_auth_") {
		t.Fatal("fixture namespace")
	}
}

func TestSQLWebAuthnFullVerifiedCeremony(t *testing.T) {
	pool := authLivePool(t)
	ctx := context.Background()
	store, err := NewSQLWebAuthnStore(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	user := fmt.Sprintf("finish_auth_verified_%d", time.Now().UnixNano())
	defer pool.Exec(ctx, `DELETE FROM neutron_auth_webauthn_credentials WHERE user_id=$1`, user)
	for _, prefix := range []string{"reg:", "auth:"} {
		defer pool.Exec(ctx, `DELETE FROM neutron_auth_webauthn_ceremonies WHERE id=$1`, prefix+user)
	}
	service := NewWebAuthnService(WebAuthnConfig{RPID: "example.test", RPName: "Example", RPOrigin: "https://example.test"}, store)
	opts, err := service.BeginRegistration(user, user, user)
	if err != nil {
		t.Fatal(err)
	}
	response, key := registrationFixtureWithID(t, opts.Challenge, "https://example.test", "example.test", 0x45, []byte(user))
	credential, err := service.FinishRegistration(user, response)
	if err != nil {
		t.Fatal(err)
	}
	if credential.VerifiedCredential == nil || credential.Revision == "" {
		t.Fatal("verified registration metadata missing")
	}
	if _, err = service.FinishRegistration(user, response); err == nil {
		t.Fatal("registration replay accepted")
	}
	auth, err := service.BeginAuthentication(user)
	if err != nil {
		t.Fatal(err)
	}
	assertion := assertionFixture(t, key, auth.Challenge, "https://example.test", "example.test", credential.CredentialID, 1, 0x05)
	assertion.Response.UserHandle = base64.RawURLEncoding.EncodeToString([]byte(user))
	verified, err := service.FinishAuthentication(user, assertion)
	if err != nil {
		t.Fatal(err)
	}
	if verified.SignCount != 1 || verified.Revision == credential.Revision {
		t.Fatal("counter/revision not committed")
	}
	if _, err = service.FinishAuthentication(user, assertion); err == nil {
		t.Fatal("assertion replay accepted")
	}
	records, err := store.GetCredentialsByUser(user)
	if err != nil || len(records) != 1 || records[0].VerifiedCredential == nil || records[0].SignCount != 1 {
		t.Fatalf("verified persistent record %+v %v", records, err)
	}
}
