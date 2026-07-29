// Package imap implements the mail.Adapter interface over IMAP4rev1.
//
// The client is written here rather than taken from a library because the Go
// IMAP ecosystem has no stable modern client — go-imap v2 is still beta — and
// because the parts that matter most for a mirror are exactly the parts
// libraries handle inconsistently: CONDSTORE modification sequences, QRESYNC
// vanished-message reporting, and UIDVALIDITY invalidation.
package imap

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// IMAP's wire format is four data types, and every response is a tree of
// them. Parsing them once, properly, is what keeps the command layer above
// free of ad-hoc string slicing.
type tokenKind int

const (
	tokenAtom tokenKind = iota
	tokenString
	tokenList
	tokenNil
)

type token struct {
	kind tokenKind
	text string
	list []token
}

func (t token) String() string {
	switch t.kind {
	case tokenNil:
		return "NIL"
	case tokenList:
		parts := make([]string, len(t.list))
		for i, c := range t.list {
			parts[i] = c.String()
		}
		return "(" + strings.Join(parts, " ") + ")"
	default:
		return t.text
	}
}

// isNil reports whether the token is IMAP's NIL, which stands in for an
// absent value everywhere in the protocol.
func (t token) isNil() bool { return t.kind == tokenNil }

// atomEq compares an atom case-insensitively, which is what the protocol
// requires for keywords like FETCH, FLAGS, and UID.
func (t token) atomEq(s string) bool {
	return t.kind == tokenAtom && strings.EqualFold(t.text, s)
}

func (t token) int() (int64, bool) {
	if t.kind != tokenAtom {
		return 0, false
	}
	n, err := strconv.ParseInt(t.text, 10, 64)
	return n, err == nil
}

// find locates the value following a keyword in a parenthesised list, which
// is how FETCH returns its items: (UID 12 FLAGS (\Seen) RFC822.SIZE 400).
func (t token) find(key string) (token, bool) {
	if t.kind != tokenList {
		return token{}, false
	}
	for i := 0; i+1 < len(t.list); i += 2 {
		if t.list[i].atomEq(key) {
			return t.list[i+1], true
		}
	}
	return token{}, false
}

// findPrefix locates the value following the first key with the given
// prefix. FETCH body items carry their section in the name — BODY[],
// BODY[HEADER], BODY[1.2] — so an exact match cannot find them.
func (t token) findPrefix(prefix string) (token, bool) {
	if t.kind != tokenList {
		return token{}, false
	}
	for i := 0; i+1 < len(t.list); i += 2 {
		if t.list[i].kind == tokenAtom &&
			strings.HasPrefix(strings.ToUpper(t.list[i].text), strings.ToUpper(prefix)) {
			return t.list[i+1], true
		}
	}
	return token{}, false
}

// decoder reads IMAP responses from a stream.
//
// It has to own the reader rather than work line by line, because literals
// carry a byte count and their content may contain newlines — a message body
// arrives mid-response and cannot be found by scanning for CRLF.
type decoder struct {
	r *bufio.Reader
}

func newDecoder(r io.Reader) *decoder {
	return &decoder{r: bufio.NewReaderSize(r, 64*1024)}
}

// readLine reads one CRLF-terminated line, stripping the terminator.
func (d *decoder) readLine() (string, error) {
	line, err := d.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// readResponse reads one complete response, following literals.
//
// The returned string is the response with every literal spliced in as a
// quoted value, so callers can treat it as one flat unit.
func (d *decoder) readResponse() ([]token, error) {
	line, err := d.readLine()
	if err != nil {
		return nil, err
	}

	var sb strings.Builder
	for {
		n, ok := literalSize(line)
		if !ok {
			sb.WriteString(line)
			break
		}

		// Everything before the {n} marker, then the literal's bytes.
		sb.WriteString(line[:strings.LastIndex(line, "{")])
		buf := make([]byte, n)
		if _, err := io.ReadFull(d.r, buf); err != nil {
			return nil, fmt.Errorf("imap: short literal: %w", err)
		}
		sb.WriteString(quote(string(buf)))

		line, err = d.readLine()
		if err != nil {
			return nil, err
		}
	}

	return tokenize(sb.String())
}

// literalSize reports the byte count of a trailing {n} literal marker.
func literalSize(line string) (int, bool) {
	if !strings.HasSuffix(line, "}") {
		return 0, false
	}
	open := strings.LastIndex(line, "{")
	if open < 0 {
		return 0, false
	}
	body := line[open+1 : len(line)-1]
	// RFC 7888 non-synchronising literals end in "+"; the size is the same.
	body = strings.TrimSuffix(body, "+")
	n, err := strconv.Atoi(body)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func quote(s string) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for i := 0; i < len(s); i++ {
		if s[i] == '"' || s[i] == '\\' {
			sb.WriteByte('\\')
		}
		sb.WriteByte(s[i])
	}
	sb.WriteByte('"')
	return sb.String()
}

// tokenize parses a flat response string into tokens.
func tokenize(s string) ([]token, error) {
	p := &parser{s: s}
	var out []token
	for {
		p.skipSpace()
		if p.eof() {
			return out, nil
		}
		t, err := p.next()
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
}

type parser struct {
	s string
	i int
}

func (p *parser) eof() bool { return p.i >= len(p.s) }

func (p *parser) skipSpace() {
	for p.i < len(p.s) && p.s[p.i] == ' ' {
		p.i++
	}
}

func (p *parser) next() (token, error) {
	if p.eof() {
		return token{}, io.ErrUnexpectedEOF
	}

	switch p.s[p.i] {
	case '(':
		return p.parseList(')')
	case '[':
		// Response codes are bracketed but structurally identical to
		// lists: [UIDVALIDITY 1] parses the same way as (UIDVALIDITY 1).
		return p.parseList(']')
	case '"':
		return p.parseQuoted()
	default:
		return p.parseAtom(), nil
	}
}

func (p *parser) parseList(closer byte) (token, error) {
	p.i++ // consume opener
	list := token{kind: tokenList}
	for {
		p.skipSpace()
		if p.eof() {
			return token{}, fmt.Errorf("imap: unterminated list")
		}
		if p.s[p.i] == closer {
			p.i++
			return list, nil
		}
		child, err := p.next()
		if err != nil {
			return token{}, err
		}
		list.list = append(list.list, child)
	}
}

func (p *parser) parseQuoted() (token, error) {
	p.i++ // consume opening quote
	var sb strings.Builder
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch c {
		case '\\':
			if p.i+1 >= len(p.s) {
				return token{}, fmt.Errorf("imap: trailing escape in quoted string")
			}
			sb.WriteByte(p.s[p.i+1])
			p.i += 2
		case '"':
			p.i++
			return token{kind: tokenString, text: sb.String()}, nil
		default:
			sb.WriteByte(c)
			p.i++
		}
	}
	return token{}, fmt.Errorf("imap: unterminated quoted string")
}

func (p *parser) parseAtom() token {
	start := p.i
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case ' ', '(', ')', ']':
			goto done
		case '[':
			// A bracket that follows atom characters is part of the item
			// name, not a new list: BODY[HEADER.FIELDS (FROM TO)] is one
			// token. A bracket in leading position is a response code and
			// never reaches here, because next() dispatches on it first.
			p.consumeBracketed()
			continue
		}
		p.i++
	}
done:
	text := p.s[start:p.i]
	if strings.EqualFold(text, "NIL") {
		return token{kind: tokenNil, text: text}
	}
	return token{kind: tokenAtom, text: text}
}

// consumeBracketed advances past a balanced [...] section, tolerating the
// nested parentheses that appear in BODY[HEADER.FIELDS (FROM TO)].
func (p *parser) consumeBracketed() {
	depth := 0
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				p.i++
				return
			}
		}
		p.i++
	}
}
