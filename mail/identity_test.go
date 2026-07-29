package mail

import (
	"testing"
	"time"
)

// The invariant the whole design rests on: an identity must survive a move
// between mailboxes and a UIDVALIDITY reset. These two tests are the reason
// the canonical model is JMAP-shaped rather than IMAP-shaped.

func TestHeaderIdentitySurvivesMoveAndUIDValidityReset(t *testing.T) {
	const hdr = "<CAF=abc123@mail.example.com>"

	inInbox := HeaderMessageID(hdr)
	afterMove := HeaderMessageID(hdr)
	afterReset := HeaderMessageID(hdr)

	if inInbox != afterMove {
		t.Errorf("identity changed on move: %s -> %s", inInbox, afterMove)
	}
	if inInbox != afterReset {
		t.Errorf("identity changed on UIDVALIDITY reset: %s -> %s", inInbox, afterReset)
	}
}

func TestPositionalIdentityIsInvalidatedByUIDValidityReset(t *testing.T) {
	// This is not a defect being tested — it is the reason positional
	// identity is last resort. If this ever stops holding, the comment in
	// PositionalMessageID is wrong.
	before := PositionalMessageID("INBOX", 1, 42)
	afterReset := PositionalMessageID("INBOX", 2, 42)
	if before == afterReset {
		t.Fatal("positional identity survived a UIDVALIDITY change; it cannot")
	}

	afterMove := PositionalMessageID("Archive", 1, 42)
	if before == afterMove {
		t.Fatal("positional identity survived a mailbox move; it cannot")
	}
}

func TestNativeIdentityIsScopedByProvider(t *testing.T) {
	// Two providers can hand out the same opaque string; scoping by
	// provider keeps them from colliding in one store.
	gmail := NativeMessageID(ProviderGmail, "18c9f0a")
	graph := NativeMessageID(ProviderGraph, "18c9f0a")
	if gmail == graph {
		t.Fatal("native identities collided across providers")
	}
}

func TestIdentitySourcesCannotCollide(t *testing.T) {
	// All three schemes share one namespace in the store, so their prefixes
	// must keep them apart even when the underlying value is identical.
	const same = "identical"
	n := NativeMessageID(ProviderIMAP, same)
	h := HeaderMessageID(same)
	p := PositionalMessageID(MailboxID(same), 0, 0)

	if n == h || h == p || n == p {
		t.Fatalf("identity sources collided: native=%s header=%s positional=%s", n, h, p)
	}
}

func TestUpgradeIdentityPromotesPositionalToHeader(t *testing.T) {
	pos := PositionalMessageID("INBOX", 1, 42)
	if !IsPositional(pos) {
		t.Fatal("expected a positional identity")
	}

	upgraded, ok := UpgradeIdentity(pos, "<real@example.com>")
	if !ok {
		t.Fatal("expected upgrade to report a change")
	}
	if IsPositional(upgraded) {
		t.Error("upgraded identity is still positional")
	}
	if want := HeaderMessageID("<real@example.com>"); upgraded != want {
		t.Errorf("upgraded = %s, want %s", upgraded, want)
	}
}

func TestUpgradeIdentityLeavesNativeAlone(t *testing.T) {
	native := NativeMessageID(ProviderJMAP, "Mabcdef")
	got, ok := UpgradeIdentity(native, "<something@example.com>")
	if ok {
		t.Error("native identity should never be downgraded to a header identity")
	}
	if got != native {
		t.Errorf("native identity mutated: %s -> %s", native, got)
	}
}

func TestUpgradeIdentityIgnoresAbsentHeader(t *testing.T) {
	pos := PositionalMessageID("INBOX", 1, 42)
	for _, hdr := range []string{"", "   ", "<>", "<   >"} {
		got, ok := UpgradeIdentity(pos, hdr)
		if ok {
			t.Errorf("header %q produced a bogus upgrade to %s", hdr, got)
		}
	}
}

func TestUpgradeIdentityIsIdempotent(t *testing.T) {
	id := HeaderMessageID("<a@b.com>")
	got, ok := UpgradeIdentity(id, "<a@b.com>")
	if ok {
		t.Error("re-upgrading an already-upgraded identity reported a change")
	}
	if got != id {
		t.Errorf("identity churned: %s -> %s", id, got)
	}
}

func TestNormalizeMessageIDHeader(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"<abc@example.com>", "abc@example.com"},
		{"  <abc@example.com>  ", "abc@example.com"},
		{"abc@example.com", "abc@example.com"},
		{"", ""},
		{"<>", ""},
		{"   ", ""},
	}
	for _, tt := range tests {
		if got := NormalizeMessageIDHeader(tt.in); got != tt.want {
			t.Errorf("NormalizeMessageIDHeader(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestMessageIDHeaderCaseIsSignificant(t *testing.T) {
	// The addr-spec local part is case-sensitive per RFC 5322 and real
	// generators emit case-significant ids. Folding case here would merge
	// two genuinely distinct messages.
	upper := HeaderMessageID("<ABC@example.com>")
	lower := HeaderMessageID("<abc@example.com>")
	if upper == lower {
		t.Fatal("message id comparison folded case; distinct messages would merge")
	}
}

func TestFingerprintMatchesAcrossAccounts(t *testing.T) {
	// The same message delivered to two connected accounts: different
	// per-account identity, one content identity, so a unified inbox shows
	// it once.
	a := &Envelope{ID: NativeMessageID(ProviderGmail, "1"), MessageIDHeader: "<shared@example.com>"}
	b := &Envelope{ID: NativeMessageID(ProviderGraph, "2"), MessageIDHeader: "<shared@example.com>"}

	if a.ID == b.ID {
		t.Fatal("per-account identities should differ")
	}
	if ComputeFingerprint(a) != ComputeFingerprint(b) {
		t.Error("fingerprints differ; a unified inbox would show the message twice")
	}
}

func TestFingerprintFallsBackWithoutHeader(t *testing.T) {
	sent := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	base := func() *Envelope {
		return &Envelope{
			From:    []Address{{Email: "sender@example.com"}},
			Subject: "Quarterly numbers",
			SentAt:  sent,
			Size:    4096,
		}
	}

	if ComputeFingerprint(base()) != ComputeFingerprint(base()) {
		t.Error("fallback fingerprint is not deterministic")
	}

	differs := base()
	differs.Subject = "Different subject"
	if ComputeFingerprint(base()) == ComputeFingerprint(differs) {
		t.Error("fallback fingerprint ignored the subject")
	}

	// Timezone must not affect the fingerprint — the same instant expressed
	// in two zones is the same message.
	shifted := base()
	shifted.SentAt = sent.In(time.FixedZone("UTC+5", 5*3600))
	if ComputeFingerprint(base()) != ComputeFingerprint(shifted) {
		t.Error("fingerprint changed with timezone representation")
	}
}

func TestFingerprintPrefersHeaderOverContent(t *testing.T) {
	withHeader := &Envelope{
		MessageIDHeader: "<x@example.com>",
		Subject:         "one",
		Size:            1,
	}
	sameHeaderDifferentContent := &Envelope{
		MessageIDHeader: "<x@example.com>",
		Subject:         "totally different",
		Size:            999,
	}
	if ComputeFingerprint(withHeader) != ComputeFingerprint(sameHeaderDifferentContent) {
		t.Error("header-based fingerprint should not depend on content fields")
	}
}

func TestParseReferences(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"space separated", "<a@x.com> <b@x.com>", []string{"a@x.com", "b@x.com"}},
		{"comma separated", "<a@x.com>, <b@x.com>", []string{"a@x.com", "b@x.com"}},
		{"no separator", "<a@x.com><b@x.com>", []string{"a@x.com", "b@x.com"}},
		{"newline folded", "<a@x.com>\r\n\t<b@x.com>", []string{"a@x.com", "b@x.com"}},
		{"unbracketed single", "a@x.com", []string{"a@x.com"}},
		{"empty", "", nil},
		// A truncated header is salvaged rather than dropped: threading on a
		// recovered identifier beats losing the thread outright, and folding
		// damage is common enough in real mail to be worth the recovery.
		{"unterminated", "<a@x.com", []string{"a@x.com"}},
		{"only brackets", "<>", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseReferences(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("ParseReferences(%q) = %v, want %v", tt.in, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("ParseReferences(%q)[%d] = %q, want %q", tt.in, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestThreadKeyPrefersReferencesRoot(t *testing.T) {
	env := &Envelope{
		References:      []string{"root@x.com", "middle@x.com"},
		InReplyTo:       []string{"middle@x.com"},
		MessageIDHeader: "<leaf@x.com>",
	}
	if got := ThreadKey(env); got != "root@x.com" {
		t.Errorf("ThreadKey = %q, want the References root", got)
	}
}

func TestThreadKeyFallsBackThroughInReplyToThenSelf(t *testing.T) {
	inReplyOnly := &Envelope{InReplyTo: []string{"parent@x.com"}, MessageIDHeader: "<leaf@x.com>"}
	if got := ThreadKey(inReplyOnly); got != "parent@x.com" {
		t.Errorf("ThreadKey = %q, want the In-Reply-To value", got)
	}

	orphan := &Envelope{MessageIDHeader: "<alone@x.com>"}
	if got := ThreadKey(orphan); got != "alone@x.com" {
		t.Errorf("ThreadKey = %q, want the message's own id", got)
	}

	headerless := &Envelope{ID: NativeMessageID(ProviderIMAP, "9")}
	if got := ThreadKey(headerless); got != string(headerless.ID) {
		t.Errorf("ThreadKey = %q, want the message identity", got)
	}
}

func TestValidate(t *testing.T) {
	valid := []MessageID{
		NativeMessageID(ProviderGmail, "abc"),
		HeaderMessageID("<a@b.com>"),
		PositionalMessageID("INBOX", 1, 1),
	}
	for _, id := range valid {
		if err := id.Validate(); err != nil {
			t.Errorf("Validate(%s) = %v, want nil", id, err)
		}
	}

	invalid := []MessageID{"", "nocolon", "z:unknown-source", "n:"}
	for _, id := range invalid {
		if err := id.Validate(); err == nil {
			t.Errorf("Validate(%q) = nil, want an error", id)
		}
	}
}

func TestShortTruncatesLongIdentities(t *testing.T) {
	long := HeaderMessageID("<some-quite-long-identifier@example.com>")
	if s := long.Short(); len([]rune(s)) > 13 {
		t.Errorf("Short() = %q, longer than expected", s)
	}
	short := MessageID("n:x")
	if short.Short() != "n:x" {
		t.Errorf("Short() mangled a short identity: %q", short.Short())
	}
}

func TestBodyPartAttachmentClassification(t *testing.T) {
	tests := []struct {
		name string
		part BodyPart
		want bool
	}{
		{"explicit attachment", BodyPart{Disposition: "attachment", Filename: "a.pdf"}, true},
		{"explicit inline", BodyPart{Disposition: "inline", Filename: "logo.png"}, false},
		{"filename, no disposition", BodyPart{Filename: "report.xlsx"}, true},
		{"inline image by content id", BodyPart{Filename: "logo.png", ContentID: "logo@x"}, false},
		{"plain body part", BodyPart{Type: "text/plain"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.part.IsAttachment(); got != tt.want {
				t.Errorf("IsAttachment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBodyAttachmentsFiltersInlineParts(t *testing.T) {
	b := &Body{Parts: []BodyPart{
		{PartID: "1", Type: "text/plain"},
		{PartID: "2", Type: "image/png", Filename: "logo.png", ContentID: "logo@x"},
		{PartID: "3", Type: "application/pdf", Filename: "invoice.pdf", Disposition: "attachment"},
	}}
	got := b.Attachments()
	if len(got) != 1 || got[0].PartID != "3" {
		t.Errorf("Attachments() = %+v, want only the pdf", got)
	}
}
