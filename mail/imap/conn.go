package imap

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/neutron-build/neutron/mail"
)

// Conn is one authenticated IMAP session.
//
// Not safe for concurrent use: IMAP multiplexes by command tag, but a mirror
// issues commands in sequence and the added machinery would buy nothing.
type Conn struct {
	mu   sync.Mutex
	raw  net.Conn
	dec  *decoder
	tag  int
	caps map[string]bool

	// selected tracks the currently selected mailbox, so SELECT is skipped
	// when the mailbox has not changed. On large mailboxes SELECT is not
	// cheap — the server re-reports the full flag and count state.
	selected string

	uidValidity uint32
	highestMod  uint64
}

// Config describes how to reach a server.
type Config struct {
	Host string
	Port int

	Username string

	// Password is a password or, for Gmail and other providers that have
	// retired basic authentication, an app password. App passwords still
	// work with 2-step verification and require no OAuth verification,
	// which makes them the pragmatic path for a self-hosted deployment.
	Password string

	// AccessToken selects XOAUTH2 instead of LOGIN when set.
	AccessToken string

	// TLSConfig overrides the default. Leave nil outside tests.
	TLSConfig *tls.Config

	Timeout time.Duration
}

// Dial opens a TLS connection and authenticates.
func Dial(ctx context.Context, cfg Config) (*Conn, error) {
	if cfg.Port == 0 {
		cfg.Port = 993
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	tlsCfg := cfg.TLSConfig
	if tlsCfg == nil {
		tlsCfg = &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}
	}

	dialer := &net.Dialer{Timeout: cfg.Timeout}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	raw, err := tls.DialWithDialer(dialer, "tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("imap: dial %s: %w", addr, err)
	}

	c := &Conn{raw: raw, dec: newDecoder(raw), caps: map[string]bool{}}

	// The server greets before any command; a greeting of BYE means it is
	// refusing the connection outright.
	greeting, err := c.dec.readResponse()
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("imap: greeting: %w", err)
	}
	if len(greeting) >= 2 && greeting[1].atomEq("BYE") {
		raw.Close()
		return nil, fmt.Errorf("imap: server refused connection: %v", greeting)
	}

	if err := c.capability(ctx); err != nil {
		raw.Close()
		return nil, err
	}
	if err := c.authenticate(ctx, cfg); err != nil {
		raw.Close()
		return nil, err
	}
	// Capabilities commonly change after login — CONDSTORE and QRESYNC are
	// frequently advertised only to an authenticated session.
	if err := c.capability(ctx); err != nil {
		raw.Close()
		return nil, err
	}
	return c, nil
}

func (c *Conn) Close() error {
	if c.raw == nil {
		return nil
	}
	_, _ = c.exec(context.Background(), "LOGOUT")
	return c.raw.Close()
}

// Supports reports whether the server advertised a capability.
func (c *Conn) Supports(cap string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.caps[strings.ToUpper(cap)]
}

func (c *Conn) nextTag() string {
	c.tag++
	return fmt.Sprintf("A%04d", c.tag)
}

// exec sends a command and collects untagged responses until the tagged
// completion arrives.
func (c *Conn) exec(ctx context.Context, format string, args ...any) ([][]token, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.raw.SetDeadline(deadline)
		defer c.raw.SetDeadline(time.Time{})
	}

	tag := c.nextTag()
	cmd := tag + " " + fmt.Sprintf(format, args...) + "\r\n"
	if _, err := c.raw.Write([]byte(cmd)); err != nil {
		return nil, fmt.Errorf("imap: write: %w", err)
	}

	var untagged [][]token
	for {
		toks, err := c.dec.readResponse()
		if err != nil {
			return nil, fmt.Errorf("imap: read: %w", err)
		}
		if len(toks) == 0 {
			continue
		}

		// A tagged line closes the command; anything else is data.
		if toks[0].kind == tokenAtom && toks[0].text == tag {
			if len(toks) < 2 {
				return untagged, fmt.Errorf("imap: malformed completion: %v", toks)
			}
			switch {
			case toks[1].atomEq("OK"):
				return untagged, nil
			case toks[1].atomEq("NO"), toks[1].atomEq("BAD"):
				return untagged, c.commandError(toks)
			default:
				return untagged, fmt.Errorf("imap: unexpected completion: %v", toks)
			}
		}
		untagged = append(untagged, toks)
	}
}

// commandError turns a NO/BAD completion into a typed error where the reason
// is one the engine acts on.
func (c *Conn) commandError(toks []token) error {
	var sb strings.Builder
	for _, t := range toks[1:] {
		sb.WriteString(t.String())
		sb.WriteByte(' ')
	}
	msg := strings.TrimSpace(sb.String())
	upper := strings.ToUpper(msg)

	switch {
	case strings.Contains(upper, "AUTHENTICATIONFAILED"),
		strings.Contains(upper, "AUTHORIZATIONFAILED"),
		strings.Contains(upper, "INVALID CREDENTIALS"),
		strings.Contains(upper, "EXPIRED"):
		// A rejected credential will not start working on retry, so this
		// must reach the engine as a reauth requirement rather than as a
		// transient failure.
		return fmt.Errorf("imap: %s: %w", msg, mail.ErrReauthRequired)
	case strings.Contains(upper, "LIMIT"), strings.Contains(upper, "THROTTL"):
		return fmt.Errorf("imap: %s: %w", msg, mail.ErrRateLimited)
	default:
		return fmt.Errorf("imap: command failed: %s", msg)
	}
}

func (c *Conn) capability(ctx context.Context) error {
	resp, err := c.exec(ctx, "CAPABILITY")
	if err != nil {
		return err
	}
	c.caps = map[string]bool{}
	for _, line := range resp {
		if len(line) < 2 || !line[1].atomEq("CAPABILITY") {
			continue
		}
		for _, t := range line[2:] {
			c.caps[strings.ToUpper(t.text)] = true
		}
	}
	return nil
}

func (c *Conn) authenticate(ctx context.Context, cfg Config) error {
	if cfg.AccessToken != "" {
		return c.authXOAuth2(ctx, cfg)
	}
	// The password is quoted rather than interpolated: it is attacker-
	// influenced input in the sense that a user may choose any bytes, and
	// an unquoted quote character would desynchronise the command stream.
	_, err := c.exec(ctx, "LOGIN %s %s", quote(cfg.Username), quote(cfg.Password))
	return err
}

func (c *Conn) authXOAuth2(ctx context.Context, cfg Config) error {
	if !c.caps["AUTH=XOAUTH2"] {
		return errors.New("imap: server does not advertise AUTH=XOAUTH2")
	}
	blob := encodeXOAuth2(cfg.Username, cfg.AccessToken)
	_, err := c.exec(ctx, "AUTHENTICATE XOAUTH2 %s", blob)
	if err != nil {
		// An OAuth rejection here is always a credential problem.
		if !errors.Is(err, mail.ErrReauthRequired) {
			return fmt.Errorf("%w: %v", mail.ErrReauthRequired, err)
		}
		return err
	}
	return nil
}

// Select opens a mailbox and records its UIDVALIDITY and HIGHESTMODSEQ.
//
// readOnly issues EXAMINE instead of SELECT, which avoids setting \Seen as a
// side effect of reading — a mirror must never change what it observes.
func (c *Conn) Select(ctx context.Context, mailbox string, readOnly bool) (uidValidity uint32, highestModSeq uint64, err error) {
	cmd := "SELECT"
	if readOnly {
		cmd = "EXAMINE"
	}

	// QRESYNC has to be enabled explicitly before the server will report
	// VANISHED responses.
	if c.caps["QRESYNC"] && c.selected == "" {
		if _, err := c.exec(ctx, "ENABLE QRESYNC"); err != nil {
			return 0, 0, err
		}
	}

	resp, err := c.exec(ctx, "%s %s", cmd, quote(mailbox))
	if err != nil {
		return 0, 0, err
	}

	c.selected = mailbox
	c.uidValidity, c.highestMod = 0, 0
	for _, line := range resp {
		if len(line) < 3 || !line[1].atomEq("OK") {
			continue
		}
		if v, ok := line[2].find("UIDVALIDITY"); ok {
			if n, ok := v.int(); ok {
				c.uidValidity = uint32(n)
			}
		}
		if v, ok := line[2].find("HIGHESTMODSEQ"); ok {
			if n, ok := v.int(); ok {
				c.highestMod = uint64(n)
			}
		}
	}
	return c.uidValidity, c.highestMod, nil
}

// List enumerates mailboxes with their SPECIAL-USE attributes.
func (c *Conn) List(ctx context.Context) ([]ListEntry, error) {
	resp, err := c.exec(ctx, `LIST "" "*"`)
	if err != nil {
		return nil, err
	}

	var out []ListEntry
	for _, line := range resp {
		// * LIST (\HasNoChildren \Sent) "/" "Sent Items"
		if len(line) < 5 || !line[1].atomEq("LIST") {
			continue
		}
		e := ListEntry{
			Delimiter: line[3].text,
			Name:      line[4].text,
		}
		for _, attr := range line[2].list {
			e.Attributes = append(e.Attributes, attr.text)
		}
		out = append(out, e)
	}
	return out, nil
}

// ListEntry is one mailbox as reported by LIST.
type ListEntry struct {
	Name       string
	Delimiter  string
	Attributes []string
}

// Role maps SPECIAL-USE attributes to the canonical role.
//
// Attributes are used rather than names because names are localised — a
// German account's Sent folder is "Gesendet" — and matching on them is how
// mirrors mis-file mail.
func (e ListEntry) Role() mail.Role {
	for _, a := range e.Attributes {
		switch strings.ToLower(a) {
		case `\inbox`:
			return mail.RoleInbox
		case `\sent`:
			return mail.RoleSent
		case `\drafts`:
			return mail.RoleDrafts
		case `\trash`:
			return mail.RoleTrash
		case `\junk`:
			return mail.RoleJunk
		case `\archive`:
			return mail.RoleArchive
		case `\all`:
			return mail.RoleAll
		}
	}
	// INBOX is the one name the protocol reserves and requires, so it is
	// safe to match even though every other name is not.
	if strings.EqualFold(e.Name, "INBOX") {
		return mail.RoleInbox
	}
	return mail.RoleNone
}

// Selectable reports whether the mailbox can hold messages. A \NoSelect
// container exists only to parent other mailboxes.
func (e ListEntry) Selectable() bool {
	for _, a := range e.Attributes {
		if strings.EqualFold(a, `\Noselect`) || strings.EqualFold(a, `\NonExistent`) {
			return false
		}
	}
	return true
}
