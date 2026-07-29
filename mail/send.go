package mail

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"mime"
	"net/smtp"
	"strings"
	"time"
)

// Outgoing is a message to send.
//
// Composition is deliberately separate from the adapters. Sending is SMTP
// submission on every provider that supports it, so putting it behind the
// Adapter interface would mean four implementations of one protocol.
type Outgoing struct {
	From    Address
	To      []Address
	Cc      []Address
	Bcc     []Address
	Subject string
	Text    string
	HTML    string

	// InReplyTo is the Message-ID of the message being answered. Setting it
	// is what makes a reply thread in the recipient's client rather than
	// starting a new conversation.
	InReplyTo string

	// References is the parent's References chain plus its Message-ID.
	// ReplyTo populates this correctly; setting it by hand is rarely right.
	References []string
}

// SMTPConfig describes the submission server.
type SMTPConfig struct {
	Host string
	Port int

	Username string
	Password string

	// Plaintext disables STARTTLS. For local test servers only; the
	// password would otherwise cross the wire in the clear.
	Plaintext bool
}

// Sender submits messages over SMTP.
type Sender struct {
	cfg SMTPConfig
}

func NewSender(cfg SMTPConfig) *Sender {
	if cfg.Port == 0 {
		cfg.Port = 587
	}
	return &Sender{cfg: cfg}
}

// ReplyTo builds a reply to an existing message.
//
// It carries the threading chain forward — In-Reply-To gets the parent's
// Message-ID and References gets the parent's chain plus that ID — which is
// what every mail client uses to nest the reply under the original. Getting
// this wrong does not fail loudly; it just silently starts a new thread in
// the recipient's inbox.
func ReplyTo(parent *Envelope, from Address, text string) *Outgoing {
	subject := parent.Subject
	if !strings.HasPrefix(strings.ToLower(subject), "re:") {
		subject = "Re: " + subject
	}

	// Reply to the Reply-To header when the sender set one, otherwise to
	// the From address.
	to := parent.ReplyTo
	if len(to) == 0 {
		to = parent.From
	}

	parentID := NormalizeMessageIDHeader(parent.MessageIDHeader)
	refs := append([]string{}, parent.References...)
	if parentID != "" {
		refs = append(refs, parentID)
	}

	return &Outgoing{
		From:       from,
		To:         to,
		Subject:    subject,
		Text:       text,
		InReplyTo:  parentID,
		References: refs,
	}
}

// Send submits the message.
func (s *Sender) Send(ctx context.Context, msg *Outgoing) (messageID string, err error) {
	if msg.From.Email == "" {
		return "", fmt.Errorf("mail: outgoing message has no sender")
	}
	if len(msg.To)+len(msg.Cc)+len(msg.Bcc) == 0 {
		return "", fmt.Errorf("mail: outgoing message has no recipients")
	}

	messageID = newMessageID(msg.From.Email)
	body, err := msg.render(messageID)
	if err != nil {
		return "", err
	}

	// Bcc recipients receive the message but must not appear in the
	// headers; render() omits them and they are added only to the envelope.
	var rcpts []string
	for _, group := range [][]Address{msg.To, msg.Cc, msg.Bcc} {
		for _, a := range group {
			rcpts = append(rcpts, a.Email)
		}
	}

	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	var auth smtp.Auth
	if s.cfg.Username != "" {
		auth = smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.Host)
	}

	if err := smtp.SendMail(addr, auth, msg.From.Email, rcpts, body); err != nil {
		return "", fmt.Errorf("mail: send: %w", err)
	}
	return messageID, nil
}

// render builds the RFC 5322 message.
func (msg *Outgoing) render(messageID string) ([]byte, error) {
	var b strings.Builder

	b.WriteString("From: " + formatAddress(msg.From) + "\r\n")
	if len(msg.To) > 0 {
		b.WriteString("To: " + formatAddressList(msg.To) + "\r\n")
	}
	if len(msg.Cc) > 0 {
		b.WriteString("Cc: " + formatAddressList(msg.Cc) + "\r\n")
	}
	b.WriteString("Subject: " + encodeHeader(msg.Subject) + "\r\n")
	b.WriteString("Date: " + time.Now().Format(time.RFC1123Z) + "\r\n")
	b.WriteString("Message-ID: " + messageID + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")

	if msg.InReplyTo != "" {
		b.WriteString("In-Reply-To: <" + msg.InReplyTo + ">\r\n")
	}
	if len(msg.References) > 0 {
		var refs []string
		for _, r := range msg.References {
			if n := NormalizeMessageIDHeader(r); n != "" {
				refs = append(refs, "<"+n+">")
			}
		}
		if len(refs) > 0 {
			b.WriteString("References: " + strings.Join(refs, " ") + "\r\n")
		}
	}

	switch {
	case msg.HTML != "" && msg.Text != "":
		boundary, err := newBoundary()
		if err != nil {
			return nil, err
		}
		b.WriteString("Content-Type: multipart/alternative; boundary=\"" + boundary + "\"\r\n\r\n")
		// Least-rich part first: a client picks the last part it can
		// render, so plain text must precede HTML.
		b.WriteString("--" + boundary + "\r\n")
		b.WriteString("Content-Type: text/plain; charset=utf-8\r\n\r\n")
		b.WriteString(msg.Text + "\r\n")
		b.WriteString("--" + boundary + "\r\n")
		b.WriteString("Content-Type: text/html; charset=utf-8\r\n\r\n")
		b.WriteString(msg.HTML + "\r\n")
		b.WriteString("--" + boundary + "--\r\n")

	case msg.HTML != "":
		b.WriteString("Content-Type: text/html; charset=utf-8\r\n\r\n")
		b.WriteString(msg.HTML + "\r\n")

	default:
		b.WriteString("Content-Type: text/plain; charset=utf-8\r\n\r\n")
		b.WriteString(msg.Text + "\r\n")
	}

	return []byte(b.String()), nil
}

// newMessageID mints an RFC 5322 Message-ID.
//
// The local part is random rather than derived from content: two identical
// messages sent twice are distinct messages, and giving them one identity
// would make the mirror merge them.
func newMessageID(from string) string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// Falling back to a timestamp keeps sending working; a collision
		// costs a merged thread, not a lost message.
		return fmt.Sprintf("<%d@%s>", time.Now().UnixNano(), domainOf(from))
	}
	return fmt.Sprintf("<%s@%s>",
		base64.RawURLEncoding.EncodeToString(buf), domainOf(from))
}

func newBoundary() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("mail: boundary: %w", err)
	}
	return "neutron-" + base64.RawURLEncoding.EncodeToString(buf), nil
}

func domainOf(email string) string {
	if _, domain, ok := strings.Cut(email, "@"); ok && domain != "" {
		return domain
	}
	return "localhost"
}

func formatAddress(a Address) string {
	if a.Name == "" {
		return a.Email
	}
	return mime.QEncoding.Encode("utf-8", a.Name) + " <" + a.Email + ">"
}

func formatAddressList(addrs []Address) string {
	parts := make([]string, 0, len(addrs))
	for _, a := range addrs {
		parts = append(parts, formatAddress(a))
	}
	return strings.Join(parts, ", ")
}

// encodeHeader applies RFC 2047 encoding when a header value is not ASCII.
func encodeHeader(s string) string {
	return mime.QEncoding.Encode("utf-8", s)
}
