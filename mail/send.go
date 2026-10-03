package mail

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
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

	// Attachments are carried as multipart/mixed parts after the body.
	// Callers sanitize nothing; render() strips anything header-hostile.
	Attachments []Attachment

	// InReplyTo is the Message-ID of the message being answered. Setting it
	// is what makes a reply thread in the recipient's client rather than
	// starting a new conversation.
	InReplyTo string

	// References is the parent's References chain plus its Message-ID.
	// ReplyTo populates this correctly; setting it by hand is rarely right.
	References []string
}

// Attachment is one binary file carried by an outgoing message.
type Attachment struct {
	Filename    string
	ContentType string // empty falls back to application/octet-stream
	Data        []byte
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

// Send submits the message and returns its Message-ID along with the exact
// RFC 5322 bytes that crossed the wire, so a caller can archive the sent copy
// verbatim through an adapter that implements Appender.
//
// Submission is context-aware end to end (audit neutron-12): the dial uses
// DialContext, every stage inherits the caller's deadline, and a canceled or
// expired context closes the connection so a stalled SMTP server cannot
// outlive the application's send timeout.
func (s *Sender) Send(ctx context.Context, msg *Outgoing) (messageID string, raw []byte, err error) {
	if msg.From.Email == "" {
		return "", nil, fmt.Errorf("mail: outgoing message has no sender")
	}
	if len(msg.To)+len(msg.Cc)+len(msg.Bcc) == 0 {
		return "", nil, fmt.Errorf("mail: outgoing message has no recipients")
	}

	messageID = newMessageID(msg.From.Email)
	body, err := msg.render(messageID, false)
	if err != nil {
		return "", nil, err
	}

	// Bcc recipients receive the message but must not appear in the
	// headers; render() omits them and they are added only to the envelope.
	var rcpts []string
	for _, group := range [][]Address{msg.To, msg.Cc, msg.Bcc} {
		for _, a := range group {
			rcpts = append(rcpts, a.Email)
		}
	}

	if err := s.submit(ctx, msg.From.Email, rcpts, body); err != nil {
		return "", nil, fmt.Errorf("mail: send: %w", err)
	}
	return messageID, body, nil
}

// submit performs the SMTP transaction with cancellation at every stage.
// Transport policy is fail-closed (audit SEND-01):
//
//   - Port 465 without Plaintext dials implicit TLS before the greeting.
//   - Every other non-Plaintext connection MUST upgrade via STARTTLS; a
//     server that offers neither STARTTLS nor a TLS port gets no mail.
//   - A configured username means authentication is required; a server
//     that stops offering AUTH is a failure, not a silent skip.
//   - Plaintext submission is restricted to loopback peers (local test
//     servers); the config comment already promised this for passwords.
//   - A failure of the final QUIT is not a delivery failure: once the
//     server acknowledges DATA, the message is accepted whether or not it
//     says goodbye politely.
func (s *Sender) submit(ctx context.Context, from string, rcpts []string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	addr := net.JoinHostPort(s.cfg.Host, fmt.Sprintf("%d", s.cfg.Port))

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	tlsCfg := &tls.Config{ServerName: s.cfg.Host, MinVersion: tls.VersionTLS12}

	var conn net.Conn
	var err error
	implicitTLS := !s.cfg.Plaintext && s.cfg.Port == 465
	if implicitTLS {
		conn, err = (&tls.Dialer{NetDialer: dialer, Config: tlsCfg}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("smtp dial: %w", err)
	}
	defer conn.Close()
	if s.cfg.Plaintext {
		peer, ok := conn.RemoteAddr().(*net.TCPAddr)
		if !ok || !peer.IP.IsLoopback() {
			return fmt.Errorf("plaintext SMTP is restricted to loopback peers")
		}
	}

	// Cancellation-driven close: a read or write blocked on a stalled
	// server is released by closing the socket, and every stage inherits
	// the caller's deadline.
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("set deadline: %w", err)
		}
	}

	c, err := smtp.NewClient(conn, s.cfg.Host)
	if err != nil {
		return err
	}
	defer c.Close()

	if err := c.Hello("localhost"); err != nil {
		return err
	}
	if !s.cfg.Plaintext && !implicitTLS {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return fmt.Errorf("SMTP server %s does not offer required STARTTLS", s.cfg.Host)
		}
		if err := c.StartTLS(tlsCfg); err != nil {
			return err
		}
	}
	if s.cfg.Username != "" {
		if ok, _ := c.Extension("AUTH"); !ok {
			return fmt.Errorf("SMTP server %s does not offer required authentication", s.cfg.Host)
		}
		if err := c.Auth(smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.Host)); err != nil {
			return err
		}
	}
	if err := c.Mail(from); err != nil {
		return err
	}
	for _, rcpt := range rcpts {
		if err := c.Rcpt(rcpt); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err // the DATA acknowledgment is what makes it accepted
	}
	// Bound the goodbye so a server that stalls on QUIT cannot hold the
	// connection past the point of acceptance.
	quitDeadline := time.Now().Add(2 * time.Second)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(quitDeadline) {
		quitDeadline = deadline
	}
	_ = conn.SetDeadline(quitDeadline)
	_ = c.Quit()
	return nil
}

// Render builds the complete RFC 5322 bytes for this message with a freshly
// minted Message-ID. Callers that submit raw MIME to a provider API (Gmail's
// raw upload, JMAP) get the same multipart/alternative handling as SMTP,
// including an HTML part when set. Bcc recipients are omitted: they travel
// in the SMTP envelope, never in headers other recipients can read.
func (msg *Outgoing) Render() ([]byte, error) {
	return msg.render(newMessageID(msg.From.Email), false)
}

// RenderWithBcc is Render with the Bcc header included, for transports with
// no envelope of their own: a raw upload delivers to whoever the headers
// name, so Bcc recipients receive nothing without the header. The transport
// is expected to strip the header from what it relays, but the sender's own
// saved copy will list the recipients — SMTP submission must keep using
// Send, where Bcc rides the envelope.
func (msg *Outgoing) RenderWithBcc() ([]byte, error) {
	return msg.render(newMessageID(msg.From.Email), true)
}

// render builds the RFC 5322 message.
func (msg *Outgoing) render(messageID string, includeBcc bool) ([]byte, error) {
	for _, group := range [][]Address{msg.To, msg.Cc, msg.Bcc} {
		for _, a := range group {
			if err := validateAddress(a); err != nil {
				return nil, err
			}
		}
	}
	if err := validateAddress(msg.From); err != nil {
		return nil, err
	}

	var b strings.Builder

	b.WriteString("From: " + formatAddress(msg.From) + "\r\n")
	if len(msg.To) > 0 {
		b.WriteString("To: " + formatAddressList(msg.To) + "\r\n")
	}
	if len(msg.Cc) > 0 {
		b.WriteString("Cc: " + formatAddressList(msg.Cc) + "\r\n")
	}
	if includeBcc && len(msg.Bcc) > 0 {
		b.WriteString("Bcc: " + formatAddressList(msg.Bcc) + "\r\n")
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

	// The body part is rendered first so it can nest unchanged inside a
	// multipart/mixed wrapper when attachments exist.
	bodyPart, err := msg.renderBodyPart()
	if err != nil {
		return nil, err
	}
	if len(msg.Attachments) == 0 {
		b.WriteString(bodyPart)
		return []byte(b.String()), nil
	}

	mixedBoundary, err := newBoundary()
	if err != nil {
		return nil, err
	}
	b.WriteString("Content-Type: multipart/mixed; boundary=\"" + mixedBoundary + "\"\r\n\r\n")
	b.WriteString("This is a multi-part message in MIME format.\r\n")
	b.WriteString("--" + mixedBoundary + "\r\n")
	b.WriteString(bodyPart)
	for _, att := range msg.Attachments {
		b.WriteString("--" + mixedBoundary + "\r\n")
		b.WriteString("Content-Type: " + sanitizeMIMEValue(att.ContentType, "application/octet-stream") + "\r\n")
		b.WriteString("Content-Disposition: " + formatDisposition(att.Filename) + "\r\n")
		b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
		enc := base64.StdEncoding.EncodeToString(att.Data)
		for i := 0; i < len(enc); i += 76 {
			end := i + 76
			if end > len(enc) {
				end = len(enc)
			}
			b.WriteString(enc[i:end])
			b.WriteString("\r\n")
		}
	}
	b.WriteString("--" + mixedBoundary + "--\r\n")

	return []byte(b.String()), nil
}

// formatDisposition builds the Content-Disposition value for an attachment.
// Non-ASCII filenames travel as RFC 2047/2231 encoded parameters via
// mime.FormatMediaType rather than raw UTF-8 bytes inside ad-hoc quotes.
func formatDisposition(filename string) string {
	name := sanitizeFilename(filename)
	if formatted := mime.FormatMediaType("attachment", map[string]string{"filename": name}); formatted != "" {
		return formatted
	}
	return `attachment; filename="` + name + `"`
}

// encodedTextPart renders one textual MIME leaf as quoted-printable.
//
// Raw UTF-8 text with no transfer encoding is implicitly 7bit, and long
// unwrapped lines (HTML especially) exceed transport line limits; some
// strict servers mangle or reject such messages (audit SEND-02).
// quoted-printable keeps ASCII text readable, bounds every encoded line,
// and is universally decodable.
func encodedTextPart(mediaType, text string) (string, error) {
	if mediaType != "text/plain" && mediaType != "text/html" {
		return "", fmt.Errorf("unsupported text MIME type %q", mediaType)
	}
	var b strings.Builder
	b.WriteString("Content-Type: " + mediaType + "; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: quoted-printable\r\n\r\n")
	enc := quotedprintable.NewWriter(&b)
	if _, err := enc.Write([]byte(text)); err != nil {
		return "", err
	}
	if err := enc.Close(); err != nil {
		return "", err
	}
	b.WriteString("\r\n")
	return b.String(), nil
}

// renderBodyPart renders the textual body (plain, html, or alternative) as a
// self-contained MIME part: its own Content-Type header plus content, each
// leaf transfer-encoded per encodedTextPart.
func (msg *Outgoing) renderBodyPart() (string, error) {
	switch {
	case msg.HTML != "" && msg.Text != "":
		boundary, err := newBoundary()
		if err != nil {
			return "", err
		}
		plain, err := encodedTextPart("text/plain", msg.Text)
		if err != nil {
			return "", err
		}
		html, err := encodedTextPart("text/html", msg.HTML)
		if err != nil {
			return "", err
		}
		var b strings.Builder
		b.WriteString("Content-Type: multipart/alternative; boundary=\"" + boundary + "\"\r\n\r\n")
		// Least-rich part first: a client picks the last part it can
		// render, so plain text must precede HTML.
		b.WriteString("--" + boundary + "\r\n")
		b.WriteString(plain)
		b.WriteString("--" + boundary + "\r\n")
		b.WriteString(html)
		b.WriteString("--" + boundary + "--\r\n")
		return b.String(), nil

	case msg.HTML != "":
		return encodedTextPart("text/html", msg.HTML)

	default:
		return encodedTextPart("text/plain", msg.Text)
	}
}

// sanitizeMIMEValue strips header-hostile bytes and falls back to def when
// empty. ContentType values must be a clean type/subtype token; anything
// that does not survive cleaning intact falls back rather than shipping a
// mangled value.
func sanitizeMIMEValue(value, def string) string {
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == '\r' || r == '\n' || r == '"' {
			return -1
		}
		return r
	}, strings.TrimSpace(value))
	if !mimeTokenRe.MatchString(value) {
		return def
	}
	return value
}

var mimeTokenRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9!#$&^_.+-]*/[a-zA-Z0-9][a-zA-Z0-9!#$&^_.+-]*$`)

// sanitizeFilename reduces an attachment name to something safe to quote in
// a Content-Disposition header: no control characters, quotes, slashes, or
// backslashes, basename only, capped length, with a fallback name.
func sanitizeFilename(name string) string {
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == '"' || r == '\'' || r == '\\' || r == '/' || r == ':' {
			return -1
		}
		return r
	}, name)
	if i := strings.LastIndexByte(name, '.'); i > 0 && i == len(name)-1 {
		name = name[:i]
	}
	if len(name) > 200 {
		// Keep the tail (the extension end of a long name), but back the
		// cut up to a rune boundary: slicing into the middle of a
		// multi-byte character produced invalid UTF-8 filenames (audit
		// 4 F17).
		start := len(name) - 200
		for start < len(name) && !utf8.RuneStart(name[start]) {
			start++
		}
		name = name[start:]
	}
	if strings.TrimSpace(name) == "" {
		name = "attachment"
	}
	return name
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

// formatAddress renders one mailbox for a header field via net/mail, whose
// formatter quotes RFC 5322 phrases correctly. Hand-building the phrase
// with a Q-encoder left ordinary ASCII names containing commas — "Doe,
// Jane" — unquoted, splitting one mailbox into two for every parser (audit
// neutron-13).
func formatAddress(a Address) string {
	return (&mail.Address{Name: a.Name, Address: a.Email}).String()
}

// validateAddress rejects control characters at the composition boundary:
// a CR or LF inside a display name or address would start a new header line
// once rendered, and NUL is never legal in RFC 5322 text.
func validateAddress(a Address) error {
	for _, s := range []string{a.Name, a.Email} {
		if strings.ContainsAny(s, "\r\n\x00") {
			return fmt.Errorf("mail: address %q contains control characters", a.Name+" <"+a.Email+">")
		}
	}
	return nil
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
