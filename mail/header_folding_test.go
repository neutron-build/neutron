package mail

import (
	"bytes"
	"mime"
	netmail "net/mail"
	"strings"
	"testing"
)

func TestRenderedHeadersFoldWithoutChangingComposition(t *testing.T) {
	for _, subject := range []string{strings.TrimSpace(strings.Repeat("ASCII words ", 100)), strings.TrimSpace(strings.Repeat("世界 ", 100))} {
		msg := &Outgoing{From: Address{Email: "from@example.com"}, To: []Address{{Name: "Doe, Jane", Email: "one@example.com"}}, Subject: subject, Text: "body", InReplyTo: "parent@example.com"}
		for i := 0; i < 40; i++ {
			msg.To = append(msg.To, Address{Name: "Recipient", Email: "recipient@example.com"})
			msg.References = append(msg.References, "parent@example.com")
		}
		raw, err := msg.Render()
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range bytes.Split(raw, []byte("\r\n")) {
			if len(line) > 998 {
				t.Fatal("oversized physical line", len(line))
			}
		}
		parsed, err := netmail.ReadMessage(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := new(mime.WordDecoder).DecodeHeader(parsed.Header.Get("Subject"))
		if err != nil || decoded != subject {
			t.Fatal("subject changed", decoded, err)
		}
		recipients, err := parsed.Header.AddressList("To")
		if err != nil || len(recipients) != len(msg.To) || recipients[0].Name != "Doe, Jane" {
			t.Fatal("recipient folding changed addresses", err)
		}
		if parsed.Header.Get("Bcc") != "" {
			t.Fatal("SMTP Bcc exposed")
		}
	}
}
func TestRenderedHeaderHardBoundaryAndInjectionRefuse(t *testing.T) {
	for _, subject := range []string{strings.Repeat("x", 999), "injected\r\nBcc: attacker@example.com"} {
		msg := &Outgoing{From: Address{Email: "from@example.com"}, To: []Address{{Email: "to@example.com"}}, Subject: subject, Text: "body"}
		if _, err := msg.Render(); err == nil {
			t.Fatal("invalid subject accepted")
		}
	}
	for _, n := range []int{989, 990} {
		msg := &Outgoing{From: Address{Email: "from@example.com"}, Subject: strings.Repeat("x", n), Text: "body"}
		_, err := msg.Render()
		if (err == nil) != (n == 989) {
			t.Fatal("Subject line 998 boundary", n, err)
		}
	}
}
