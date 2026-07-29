package mail

import (
	"strings"
	"testing"
)

func TestReplyCarriesTheThreadingChain(t *testing.T) {
	// Getting this wrong does not fail loudly — the reply simply starts a
	// new conversation in the recipient's client instead of nesting.
	parent := &Envelope{
		Subject:         "Quarterly numbers",
		MessageIDHeader: "<parent@example.com>",
		References:      []string{"root@example.com"},
		From:            []Address{{Name: "Alice", Email: "alice@example.com"}},
	}

	reply := ReplyTo(parent, Address{Email: "bob@example.com"}, "Looks good")

	if reply.InReplyTo != "parent@example.com" {
		t.Errorf("InReplyTo = %q, want the parent's Message-ID", reply.InReplyTo)
	}
	if len(reply.References) != 2 {
		t.Fatalf("References = %v, want the parent chain plus its id", reply.References)
	}
	if reply.References[0] != "root@example.com" || reply.References[1] != "parent@example.com" {
		t.Errorf("References = %v, want [root, parent] in order", reply.References)
	}
	if reply.Subject != "Re: Quarterly numbers" {
		t.Errorf("Subject = %q", reply.Subject)
	}
	if len(reply.To) != 1 || reply.To[0].Email != "alice@example.com" {
		t.Errorf("To = %+v, want the original sender", reply.To)
	}
}

func TestReplyDoesNotDoubleThePrefix(t *testing.T) {
	for _, subject := range []string{"Re: Already", "re: already", "RE: ALREADY"} {
		reply := ReplyTo(&Envelope{Subject: subject}, Address{Email: "a@b.com"}, "x")
		if strings.Count(strings.ToLower(reply.Subject), "re:") != 1 {
			t.Errorf("ReplyTo(%q).Subject = %q, want a single prefix", subject, reply.Subject)
		}
	}
}

func TestReplyPrefersTheReplyToHeader(t *testing.T) {
	// A sender that sets Reply-To wants answers elsewhere, and ignoring it
	// sends the reply to the wrong person.
	parent := &Envelope{
		From:    []Address{{Email: "noreply@example.com"}},
		ReplyTo: []Address{{Email: "support@example.com"}},
	}
	reply := ReplyTo(parent, Address{Email: "me@x.com"}, "hello")

	if len(reply.To) != 1 || reply.To[0].Email != "support@example.com" {
		t.Errorf("To = %+v, want the Reply-To address", reply.To)
	}
}

func TestRenderProducesValidHeaders(t *testing.T) {
	msg := &Outgoing{
		From:    Address{Name: "Alice", Email: "alice@example.com"},
		To:      []Address{{Email: "bob@example.com"}},
		Cc:      []Address{{Name: "Carol", Email: "carol@example.com"}},
		Subject: "Hello",
		Text:    "body text",
	}

	raw, err := msg.render("<test@example.com>")
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)

	for _, want := range []string{
		"From: Alice <alice@example.com>",
		"To: bob@example.com",
		"Cc: Carol <carol@example.com>",
		"Subject: Hello",
		"Message-ID: <test@example.com>",
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=utf-8",
		"body text",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("rendered message is missing %q\n---\n%s", want, s)
		}
	}

	// Headers end with CRLF, not LF: some servers reject bare LF.
	if strings.Contains(strings.ReplaceAll(s, "\r\n", ""), "\n") {
		t.Error("message contains a bare LF; line endings must be CRLF")
	}
}

func TestBccIsNotRenderedIntoHeaders(t *testing.T) {
	// A Bcc recipient appearing in the headers defeats the entire point and
	// leaks who else received the message.
	msg := &Outgoing{
		From: Address{Email: "a@x.com"},
		To:   []Address{{Email: "b@x.com"}},
		Bcc:  []Address{{Email: "secret@x.com"}},
		Text: "hi",
	}

	raw, err := msg.render("<id@x.com>")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret@x.com") {
		t.Error("a Bcc recipient leaked into the rendered headers")
	}
}

func TestMultipartPutsPlainTextFirst(t *testing.T) {
	// A client renders the last part it understands, so plain text has to
	// come before HTML or text-only clients show nothing useful.
	msg := &Outgoing{
		From: Address{Email: "a@x.com"},
		To:   []Address{{Email: "b@x.com"}},
		Text: "PLAINPART",
		HTML: "<p>HTMLPART</p>",
	}

	raw, err := msg.render("<id@x.com>")
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)

	if !strings.Contains(s, "multipart/alternative") {
		t.Fatal("a message with both bodies was not sent as multipart/alternative")
	}
	plainAt := strings.Index(s, "PLAINPART")
	htmlAt := strings.Index(s, "HTMLPART")
	if plainAt < 0 || htmlAt < 0 {
		t.Fatalf("both parts should be present:\n%s", s)
	}
	if plainAt > htmlAt {
		t.Error("HTML came before plain text; text-only clients would show nothing")
	}
}

func TestNonASCIIHeadersAreEncoded(t *testing.T) {
	msg := &Outgoing{
		From:    Address{Name: "Ünicode Sender", Email: "u@x.com"},
		To:      []Address{{Email: "b@x.com"}},
		Subject: "Grüße",
		Text:    "hi",
	}

	raw, err := msg.render("<id@x.com>")
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)

	// Raw UTF-8 in a header is not legal; it must be RFC 2047 encoded.
	if strings.Contains(s, "Grüße") {
		t.Error("a non-ASCII subject was not encoded")
	}
	if !strings.Contains(s, "=?utf-8?") {
		t.Error("no RFC 2047 encoded-word found in the headers")
	}
}

func TestMessageIDsAreUnique(t *testing.T) {
	// Two identical messages sent twice are distinct messages. Deriving the
	// id from content would make the mirror merge them.
	seen := map[string]bool{}
	for range 100 {
		id := newMessageID("alice@example.com")
		if seen[id] {
			t.Fatalf("duplicate Message-ID: %s", id)
		}
		seen[id] = true

		if !strings.HasSuffix(id, "@example.com>") {
			t.Errorf("Message-ID %q does not carry the sender's domain", id)
		}
		if !strings.HasPrefix(id, "<") {
			t.Errorf("Message-ID %q is not bracketed", id)
		}
	}
}

func TestSendRejectsIncompleteMessages(t *testing.T) {
	s := NewSender(SMTPConfig{Host: "localhost"})

	noSender := &Outgoing{To: []Address{{Email: "b@x.com"}}, Text: "hi"}
	if _, err := s.Send(t.Context(), noSender); err == nil {
		t.Error("a message with no sender was accepted")
	}

	noRecipients := &Outgoing{From: Address{Email: "a@x.com"}, Text: "hi"}
	if _, err := s.Send(t.Context(), noRecipients); err == nil {
		t.Error("a message with no recipients was accepted")
	}
}

func TestDomainOf(t *testing.T) {
	tests := map[string]string{
		"alice@example.com": "example.com",
		"no-at-sign":        "localhost",
		"trailing@":         "localhost",
	}
	for in, want := range tests {
		if got := domainOf(in); got != want {
			t.Errorf("domainOf(%q) = %q, want %q", in, got, want)
		}
	}
}
