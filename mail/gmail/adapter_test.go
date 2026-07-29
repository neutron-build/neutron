package gmail

import (
	"errors"
	"testing"

	"github.com/neutron-build/neutron/mail"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
)

func TestRoleFromReservedLabelIDs(t *testing.T) {
	// Reserved label IDs are stable and locale-independent; display names
	// are neither, which is why the mapping keys on the ID.
	tests := map[string]mail.Role{
		"INBOX":     mail.RoleInbox,
		"SENT":      mail.RoleSent,
		"DRAFT":     mail.RoleDrafts,
		"TRASH":     mail.RoleTrash,
		"SPAM":      mail.RoleJunk,
		"Label_42":  mail.RoleNone,
		"IMPORTANT": mail.RoleNone,
	}
	for in, want := range tests {
		if got := roleFrom(in); got != want {
			t.Errorf("roleFrom(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestReadStateIsTheAbsenceOfUnread(t *testing.T) {
	// Gmail models read state as the absence of a label. Getting this
	// backwards marks every message in the mirror unread.
	unread := toEnvelope(&gmail.Message{Id: "a", LabelIds: []string{"INBOX", "UNREAD"}})
	if unread.Keywords.Seen {
		t.Error("a message carrying UNREAD was marked Seen")
	}

	read := toEnvelope(&gmail.Message{Id: "b", LabelIds: []string{"INBOX"}})
	if !read.Keywords.Seen {
		t.Error("a message without UNREAD was not marked Seen")
	}
}

func TestStarredMapsToFlagged(t *testing.T) {
	env := toEnvelope(&gmail.Message{Id: "a", LabelIds: []string{"STARRED"}})
	if !env.Keywords.Flagged {
		t.Error("STARRED did not map to Flagged")
	}
}

func TestLabelsBecomeMailboxMemberships(t *testing.T) {
	// A Gmail message can carry several labels at once, which is the
	// many-to-many case the store's membership table exists for.
	env := toEnvelope(&gmail.Message{Id: "a", LabelIds: []string{"INBOX", "Label_7", "UNREAD"}})
	if len(env.MailboxIDs) != 3 {
		t.Errorf("MailboxIDs = %v, want all three labels", env.MailboxIDs)
	}
}

func TestToEnvelopeReadsHeaders(t *testing.T) {
	m := &gmail.Message{
		Id:           "abc",
		ThreadId:     "thr",
		SizeEstimate: 2048,
		Snippet:      "preview text",
		InternalDate: 1_700_000_000_000,
		Payload: &gmail.MessagePart{
			Headers: []*gmail.MessagePartHeader{
				{Name: "Subject", Value: "Quarterly numbers"},
				{Name: "From", Value: `"Alice Smith" <alice@example.com>`},
				{Name: "To", Value: "bob@example.com, carol@example.com"},
				{Name: "Message-ID", Value: "<xyz@example.com>"},
				{Name: "References", Value: "<root@example.com> <mid@example.com>"},
			},
		},
	}

	env := toEnvelope(m)

	if env.Subject != "Quarterly numbers" {
		t.Errorf("Subject = %q", env.Subject)
	}
	if env.ThreadID != "thr" {
		t.Errorf("ThreadID = %q, want thr", env.ThreadID)
	}
	if len(env.From) != 1 || env.From[0].Email != "alice@example.com" {
		t.Errorf("From = %+v", env.From)
	}
	if env.From[0].Name != "Alice Smith" {
		t.Errorf("From name = %q, want the unquoted display name", env.From[0].Name)
	}
	if len(env.To) != 2 {
		t.Errorf("To = %+v, want two recipients", env.To)
	}
	if len(env.References) != 2 || env.References[0] != "root@example.com" {
		t.Errorf("References = %v", env.References)
	}
	if env.ReceivedAt.IsZero() {
		t.Error("InternalDate did not become ReceivedAt")
	}
	if env.Fingerprint == "" {
		t.Error("fingerprint was not computed")
	}
}

func TestParseAddrs(t *testing.T) {
	tests := []struct {
		in        string
		wantCount int
		wantFirst string
	}{
		{`"Alice" <alice@example.com>`, 1, "alice@example.com"},
		{`alice@example.com`, 1, "alice@example.com"},
		{`a@x.com, b@x.com`, 2, "a@x.com"},
		{`Alice <a@x.com>, Bob <b@x.com>`, 2, "a@x.com"},
		{``, 0, ""},
	}
	for _, tt := range tests {
		got := parseAddrs(tt.in)
		if len(got) != tt.wantCount {
			t.Errorf("parseAddrs(%q) = %+v, want %d addresses", tt.in, got, tt.wantCount)
			continue
		}
		if tt.wantCount > 0 && got[0].Email != tt.wantFirst {
			t.Errorf("parseAddrs(%q)[0] = %q, want %q", tt.in, got[0].Email, tt.wantFirst)
		}
	}
}

func TestClassifyDistinguishesQuotaFromRevocation(t *testing.T) {
	// Google returns 403 for both a revoked grant and an exhausted quota.
	// Conflating them means either retrying a dead credential forever or
	// telling a user to reconnect an account that was merely throttled.
	quota := &googleapi.Error{
		Code:   403,
		Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}},
	}
	if err := classify(quota); !errors.Is(err, mail.ErrRateLimited) {
		t.Errorf("quota 403 = %v, want ErrRateLimited", err)
	}

	revoked := &googleapi.Error{
		Code:   403,
		Errors: []googleapi.ErrorItem{{Reason: "forbidden"}},
	}
	if err := classify(revoked); !errors.Is(err, mail.ErrReauthRequired) {
		t.Errorf("revoked 403 = %v, want ErrReauthRequired", err)
	}
}

func TestClassifyMapsStatusCodes(t *testing.T) {
	tests := []struct {
		code int
		want error
	}{
		{401, mail.ErrReauthRequired},
		{404, mail.ErrNotFound},
		{429, mail.ErrRateLimited},
	}
	for _, tt := range tests {
		err := classify(&googleapi.Error{Code: tt.code})
		if !errors.Is(err, tt.want) {
			t.Errorf("classify(%d) = %v, want %v", tt.code, err, tt.want)
		}
	}
	if classify(nil) != nil {
		t.Error("classify(nil) should stay nil")
	}
}

func TestGmailLabelMapping(t *testing.T) {
	if got := gmailLabel("flagged"); got != "STARRED" {
		t.Errorf("gmailLabel(flagged) = %q, want STARRED", got)
	}
	if got := gmailLabel("draft"); got != "DRAFT" {
		t.Errorf("gmailLabel(draft) = %q, want DRAFT", got)
	}
}

func TestPayloadHasAttachmentWalksTheTree(t *testing.T) {
	nested := &gmail.MessagePart{
		MimeType: "multipart/mixed",
		Parts: []*gmail.MessagePart{
			{MimeType: "text/plain"},
			{MimeType: "multipart/related", Parts: []*gmail.MessagePart{
				{MimeType: "application/pdf", Filename: "invoice.pdf"},
			}},
		},
	}
	if !payloadHasAttachment(nested) {
		t.Error("an attachment nested two levels deep was missed")
	}

	plain := &gmail.MessagePart{MimeType: "text/plain"}
	if payloadHasAttachment(plain) {
		t.Error("a plain body part was reported as an attachment")
	}
}

func TestNativeIDStripsThePrefix(t *testing.T) {
	id := mail.NativeMessageID(mail.ProviderGmail, "18c9f0a")
	if got := nativeID(id); got != "18c9f0a" {
		t.Errorf("nativeID = %q, want 18c9f0a", got)
	}
}
