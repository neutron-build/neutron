package mail

import (
	"context"
	"testing"
)

func TestIntegrationAttachmentEvidencePublishesWithBody(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acct := seedAccount(t, s)
	id := NativeMessageID(ProviderGmail, "mime")
	env := Envelope{ID: id, AttachmentPresence: AttachmentUnknown}
	if err := s.PutEnvelopes(ctx, acct, []Envelope{env}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Envelope(ctx, acct, id)
	if err != nil || got.AttachmentPresence != AttachmentUnknown {
		t.Fatal(got, err)
	}
	body := &Body{MessageID: id, Text: "body", Parts: []BodyPart{{PartID: "1", Filename: "attachment.txt"}}}
	if err := s.PutBody(ctx, acct, body); err != nil {
		t.Fatal(err)
	}
	if err := s.PutEnvelopes(ctx, acct, []Envelope{env}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Envelope(ctx, acct, id)
	if err != nil || !got.HasAttachment || got.AttachmentPresence != AttachmentPresent {
		t.Fatal("metadata erased positive evidence", got, err)
	}
	body.Parts = nil
	if err := s.PutBody(ctx, acct, body); err != nil {
		t.Fatal(err)
	}
	if err := s.PutEnvelopes(ctx, acct, []Envelope{env}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Envelope(ctx, acct, id)
	if err != nil || got.HasAttachment || got.AttachmentPresence != AttachmentAbsent {
		t.Fatal("metadata erased complete absence evidence", got, err)
	}
}
