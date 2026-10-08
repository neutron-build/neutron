package mail

import (
	"context"
	"errors"
	"testing"
)

type graphTranslationFixture struct {
	key   string
	fail  bool
	calls int
}

func (f *graphTranslationFixture) GraphMailboxKey(context.Context) (string, error) { return f.key, nil }
func (f *graphTranslationFixture) TranslateGraphIDs(_ context.Context, ids []MessageID) ([]IdentityPair, error) {
	f.calls++
	if f.fail {
		return nil, errors.New("provider interrupted")
	}
	var out []IdentityPair
	for _, id := range ids {
		out = append(out, IdentityPair{id, NativeMessageID(ProviderGraph, "immutable-"+string(id))})
	}
	return out, nil
}
func TestIntegrationGraphMappingRestartRollbackAndAliases(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := AccountID("graph-owner")
	if err := s.PutAccount(ctx, &Account{ID: acct, Provider: ProviderGraph, Email: "test@example.com"}); err != nil {
		t.Fatal(err)
	}
	configureIdentityFixture(t, s, acct)
	s.graphInventory = func(context.Context, IdentityTx, AccountID) ([]MessageID, error) { return nil, nil }
	var envs []Envelope
	for _, id := range []string{"CaseSensitiveA", "caseSensitiveB"} {
		envs = append(envs, Envelope{ID: NativeMessageID(ProviderGraph, id), ThreadID: "conversation", MailboxIDs: []MailboxID{"INBOX", "Archive"}})
	}
	if err := s.PutEnvelopes(ctx, acct, envs); err != nil {
		t.Fatal(err)
	}
	for _, env := range envs {
		if err := s.PutBody(ctx, acct, &Body{MessageID: env.ID, Text: "body"}); err != nil {
			t.Fatal(err)
		}
	}
	provider := &graphTranslationFixture{key: "verified-mailbox"}
	// Wrong mailbox is refused before migration begins.
	if _, err := s.MigrateGraphIdentity(ctx, acct, "wrong-mailbox", provider, 1); err == nil {
		t.Fatal("wrong mailbox accepted")
	}
	if mode, err := s.GraphIdentityFormat(ctx, acct); err != nil || mode != GraphLegacyIDs {
		t.Fatal(mode, err)
	}
	done, err := s.MigrateGraphIdentity(ctx, acct, provider.key, provider, 1)
	if err != nil || done {
		t.Fatal("partial batch cut over", done, err)
	}
	if _, err := s.GraphIdentityFormat(ctx, acct); !errors.Is(err, ErrIdentityMigrationPending) {
		t.Fatal("mapping not fenced", err)
	}
	if err := s.PutEnvelopes(ctx, acct, envs); !errors.Is(err, ErrIdentityMigrationPending) {
		t.Fatal("sync admitted during mapping", err)
	}
	provider.fail = true
	if _, err := s.MigrateGraphIdentity(ctx, acct, provider.key, provider, 1); err == nil {
		t.Fatal("provider failure swallowed")
	}
	// Pools can reopen without discarding the completed first translation batch.
	reopened, err := OpenWithGraphIdentityPolicy(ctx, osGetenvTestURL(t), s.beforeAccountWrite, s.remap, s.graphInventory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	provider.fail = false
	remap := reopened.remap
	reopened.remap = func(context.Context, IdentityTx, AccountID, []IdentityPair) error {
		return errors.New("product remap refused")
	}
	if _, err := reopened.MigrateGraphIdentity(ctx, acct, provider.key, provider, 1); err == nil {
		t.Fatal("cutover callback failure committed")
	}
	for _, env := range envs {
		got, err := s.Envelope(ctx, acct, env.ID)
		if err != nil || got.ID != env.ID {
			t.Fatal("rollback lost old format", err)
		}
	}
	if _, err := s.GraphIdentityFormat(ctx, acct); !errors.Is(err, ErrIdentityMigrationPending) {
		t.Fatal("format changed on failure", err)
	}
	reopened.remap = remap
	done, err = reopened.MigrateGraphIdentity(ctx, acct, provider.key, provider, 1)
	if err != nil || !done {
		t.Fatal("resume failed", done, err)
	}
	if mode, err := s.GraphIdentityFormat(ctx, acct); err != nil || mode != GraphImmutableIDs {
		t.Fatal(mode, err)
	}
	for _, env := range envs {
		id, err := s.ResolveIdentity(ctx, acct, env.ID)
		if err != nil || id == env.ID {
			t.Fatal("alias missing", err)
		}
		got, err := s.Envelope(ctx, acct, env.ID)
		if err != nil || got.ID != id || got.ThreadID != "conversation" || len(got.MailboxIDs) != 2 {
			t.Fatal("identity state lost", got, err)
		}
		body, err := s.Body(ctx, acct, env.ID)
		if err != nil || body.MessageID != id || body.Text != "body" {
			t.Fatal("old body reference broken", err)
		}
		// Other accounts cannot resolve this account's aliases.
		other, err := s.ResolveIdentity(ctx, "other-account", env.ID)
		if err != nil || other != env.ID {
			t.Fatal("alias crossed accounts", err)
		}
	}
	if err := s.PutEnvelopes(ctx, acct, envs); !errors.Is(err, ErrIdentityMigrationPending) {
		t.Fatal("retired IDs recreated", err)
	}
	if err := s.AbortGraphIdentity(ctx, acct); err == nil {
		t.Fatal("committed cutover downgraded")
	}
	done, err = s.MigrateGraphIdentity(ctx, acct, provider.key, provider, 1)
	if err != nil || !done {
		t.Fatal("committed retry failed", err)
	}
}

func TestGraphTranslationProofRejectsPartialDuplicateAndForeignIDs(t *testing.T) {
	a, b := NativeMessageID(ProviderGraph, "A"), NativeMessageID(ProviderGraph, "B")
	target := NativeMessageID(ProviderGraph, "I")
	for _, pairs := range [][]IdentityPair{
		{{a, target}}, {{a, target}, {b, target}}, {{a, target}, {a, NativeMessageID(ProviderGraph, "J")}},
		{{a, target}, {b, NativeMessageID(ProviderGmail, "foreign")}},
	} {
		if err := validateGraphTranslations([]MessageID{a, b}, pairs); err == nil {
			t.Fatal("invalid proof accepted", pairs)
		}
	}
}

func TestIntegrationGraphAbortMappingAndCollision(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := AccountID("graph-owner")
	if err := s.PutAccount(ctx, &Account{ID: acct, Provider: ProviderGraph, Email: "test@example.com"}); err != nil {
		t.Fatal(err)
	}
	configureIdentityFixture(t, s, acct)
	s.graphInventory = func(context.Context, IdentityTx, AccountID) ([]MessageID, error) { return nil, nil }
	a, b := NativeMessageID(ProviderGraph, "A"), NativeMessageID(ProviderGraph, "B")
	if err := s.PutEnvelopes(ctx, acct, []Envelope{{ID: a}, {ID: b}}); err != nil {
		t.Fatal(err)
	}
	provider := &graphTranslationFixture{key: "verified"}
	if done, err := s.MigrateGraphIdentity(ctx, acct, provider.key, provider, 1); err != nil || done {
		t.Fatal(done, err)
	}
	// Two different legacy IDs cannot stage the same immutable target across calls.
	if err := s.stageGraphTranslations(ctx, acct, provider.key, []IdentityPair{{b, NativeMessageID(ProviderGraph, "immutable-"+string(a))}}); !errors.Is(err, ErrIdentityCollision) {
		t.Fatal("collision accepted", err)
	}
	if err := s.AbortGraphIdentity(ctx, acct); err != nil {
		t.Fatal(err)
	}
	if mode, err := s.GraphIdentityFormat(ctx, acct); err != nil || mode != GraphLegacyIDs {
		t.Fatal(mode, err)
	}
	for _, id := range []MessageID{a, b} {
		if _, err := s.Envelope(ctx, acct, id); err != nil {
			t.Fatal("abort lost old rows", err)
		}
	}
}

func TestIntegrationGraphProductOnlyAndScanOnlyReferences(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := AccountID("graph-owner")
	if err := s.PutAccount(ctx, &Account{ID: acct, Provider: ProviderGraph, Email: "test@example.com"}); err != nil {
		t.Fatal(err)
	}
	configureIdentityFixture(t, s, acct)
	productID := NativeMessageID(ProviderGraph, "expired-product-parent")
	scanID := NativeMessageID(ProviderGraph, "expired-scan-message")
	s.graphInventory = func(context.Context, IdentityTx, AccountID) ([]MessageID, error) { return []MessageID{productID}, nil }
	scan, err := s.BeginScan(ctx, acct, "INBOX")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyScanPage(ctx, scan.ID, nil, []MessageID{scanID}, nil, "continuation"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO identity_test_receipt VALUES($1,$2,'retained-only')`, string(acct), string(productID)); err != nil {
		t.Fatal(err)
	}
	original := s.remap
	calls := 0
	s.remap = func(ctx context.Context, tx IdentityTx, acct AccountID, pairs []IdentityPair) error {
		calls++
		if len(pairs) != 2 {
			return errors.New("partial product/scan-only mapping")
		}
		return original(ctx, tx, acct, pairs)
	}
	provider := &graphTranslationFixture{key: "verified"}
	done, err := s.MigrateGraphIdentity(ctx, acct, provider.key, provider, 100)
	if err != nil || !done || calls != 1 {
		t.Fatal(done, err, calls)
	}
	for _, old := range []MessageID{productID, scanID} {
		resolved, err := s.ResolveIdentity(ctx, acct, old)
		if err != nil || resolved == old {
			t.Fatal("reference-only alias lost", old, err)
		}
		if _, err := s.Envelope(ctx, acct, resolved); !errors.Is(err, ErrNoStore) {
			t.Fatal("reference remap resurrected expired mail", err)
		}
	}
	var seen MessageID
	if err := s.pool.QueryRow(ctx, `SELECT message_id FROM mirror_scan_seen WHERE scan_id=$1`, string(scan.ID)).Scan(&seen); err != nil || seen == scanID {
		t.Fatal("scan-only evidence not translated", err)
	}
}
