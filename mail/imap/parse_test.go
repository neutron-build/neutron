package imap

import (
	"strings"
	"testing"
)

func TestTokenizeFlatResponse(t *testing.T) {
	toks, err := tokenize(`* 12 FETCH (UID 4827 RFC822.SIZE 44827)`)
	if err != nil {
		t.Fatal(err)
	}
	if len(toks) != 4 {
		t.Fatalf("got %d tokens (%v), want 4", len(toks), toks)
	}
	if !toks[2].atomEq("FETCH") {
		t.Errorf("token 2 = %v, want FETCH", toks[2])
	}

	uid, ok := toks[3].find("UID")
	if !ok {
		t.Fatal("UID not found in fetch items")
	}
	if n, ok := uid.int(); !ok || n != 4827 {
		t.Errorf("UID = %v, want 4827", uid)
	}
}

func TestTokenizeNestedLists(t *testing.T) {
	toks, err := tokenize(`* 1 FETCH (FLAGS (\Seen \Answered) BODY ((("TEXT" "PLAIN"))))`)
	if err != nil {
		t.Fatal(err)
	}
	items := toks[3]

	flags, ok := items.find("FLAGS")
	if !ok {
		t.Fatal("FLAGS not found")
	}
	if flags.kind != tokenList || len(flags.list) != 2 {
		t.Fatalf("FLAGS = %v, want a two-element list", flags)
	}
	if flags.list[0].text != `\Seen` {
		t.Errorf("first flag = %q, want \\Seen", flags.list[0].text)
	}
}

func TestTokenizeQuotedStringsWithEscapes(t *testing.T) {
	toks, err := tokenize(`* 1 FETCH (SUBJECT "He said \"hello\" and left" X "back\\slash")`)
	if err != nil {
		t.Fatal(err)
	}
	subj, ok := toks[3].find("SUBJECT")
	if !ok {
		t.Fatal("SUBJECT not found")
	}
	if want := `He said "hello" and left`; subj.text != want {
		t.Errorf("SUBJECT = %q, want %q", subj.text, want)
	}
	back, _ := toks[3].find("X")
	if want := `back\slash`; back.text != want {
		t.Errorf("X = %q, want %q", back.text, want)
	}
}

func TestQuotedStringIsNotConfusedWithAnAtom(t *testing.T) {
	// A quoted "NIL" is the literal text, not IMAP's NIL.
	toks, err := tokenize(`(A NIL B "NIL")`)
	if err != nil {
		t.Fatal(err)
	}
	list := toks[0]
	if v, _ := list.find("A"); !v.isNil() {
		t.Error("unquoted NIL should parse as the nil token")
	}
	v, _ := list.find("B")
	if v.isNil() {
		t.Error(`quoted "NIL" should be a string, not the nil token`)
	}
	if v.text != "NIL" {
		t.Errorf("B = %q, want the string NIL", v.text)
	}
}

func TestTokenizeResponseCodeBrackets(t *testing.T) {
	toks, err := tokenize(`* OK [UIDVALIDITY 3857529045] UIDs valid`)
	if err != nil {
		t.Fatal(err)
	}
	if toks[2].kind != tokenList {
		t.Fatalf("bracketed response code = %v, want a list", toks[2])
	}
	v, ok := toks[2].find("UIDVALIDITY")
	if !ok {
		t.Fatal("UIDVALIDITY not found")
	}
	if n, ok := v.int(); !ok || n != 3857529045 {
		t.Errorf("UIDVALIDITY = %v, want 3857529045", v)
	}
}

func TestTokenizeHighestModSeq(t *testing.T) {
	// CONDSTORE's MODSEQ is a 64-bit value; parsing it as 32 bits would
	// silently break incremental sync on long-lived mailboxes.
	toks, err := tokenize(`* OK [HIGHESTMODSEQ 90060128194045007] Highest`)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := toks[2].find("HIGHESTMODSEQ")
	if !ok {
		t.Fatal("HIGHESTMODSEQ not found")
	}
	n, ok := v.int()
	if !ok || n != 90060128194045007 {
		t.Errorf("HIGHESTMODSEQ = %v, want 90060128194045007", v)
	}
}

func TestFindIsCaseInsensitive(t *testing.T) {
	// Servers vary in the case they use for response item names.
	toks, _ := tokenize(`(uid 5 Flags (\Seen))`)
	if _, ok := toks[0].find("UID"); !ok {
		t.Error("lowercase uid was not matched")
	}
	if _, ok := toks[0].find("flags"); !ok {
		t.Error("mixed-case Flags was not matched")
	}
}

func TestLiteralSize(t *testing.T) {
	tests := []struct {
		in   string
		want int
		ok   bool
	}{
		{"* 1 FETCH (BODY[] {310}", 310, true},
		{"* 1 FETCH (BODY[] {0}", 0, true},
		{"A1 OK done", 0, false},
		{"* 1 FETCH (BODY[] {12+}", 12, true}, // RFC 7888 non-synchronising
		{"{notanumber}", 0, false},
	}
	for _, tt := range tests {
		got, ok := literalSize(tt.in)
		if ok != tt.ok || (ok && got != tt.want) {
			t.Errorf("literalSize(%q) = %d,%v; want %d,%v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

func TestReadResponseSplicesLiterals(t *testing.T) {
	// A literal's content may contain CRLF, which is exactly why responses
	// cannot be read line by line.
	raw := "* 1 FETCH (BODY[HEADER] {28}\r\nSubject: hi\r\nFrom: a@b.com\r\n)\r\n"
	d := newDecoder(strings.NewReader(raw))

	toks, err := d.readResponse()
	if err != nil {
		t.Fatal(err)
	}
	// The section is part of the item name, so the lookup is by prefix.
	body, ok := toks[3].findPrefix("BODY[")
	if !ok {
		t.Fatalf("BODY[HEADER] not found in %v", toks[3])
	}
	if !strings.Contains(body.text, "Subject: hi") {
		t.Errorf("literal content = %q, want the spliced header", body.text)
	}
	if !strings.Contains(body.text, "From: a@b.com") {
		t.Error("literal was truncated at the first CRLF")
	}
}

func TestReadResponseHandlesMultipleLiterals(t *testing.T) {
	raw := "* 1 FETCH (A {2}\r\nhi B {3}\r\nbye)\r\n"
	d := newDecoder(strings.NewReader(raw))

	toks, err := d.readResponse()
	if err != nil {
		t.Fatal(err)
	}
	a, _ := toks[3].find("A")
	b, _ := toks[3].find("B")
	if a.text != "hi" {
		t.Errorf("A = %q, want hi", a.text)
	}
	if b.text != "bye" {
		t.Errorf("B = %q, want bye", b.text)
	}
}

func TestReadResponseHandlesLiteralWithQuotesInside(t *testing.T) {
	// Literals are re-quoted internally, so content containing quotes and
	// backslashes has to survive that round trip.
	content := `say "hi" \ here`
	raw := "* 1 FETCH (A {" + itoa(len(content)) + "}\r\n" + content + ")\r\n"
	d := newDecoder(strings.NewReader(raw))

	toks, err := d.readResponse()
	if err != nil {
		t.Fatal(err)
	}
	a, _ := toks[3].find("A")
	if a.text != content {
		t.Errorf("literal = %q, want %q", a.text, content)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func TestUnterminatedConstructsAreErrors(t *testing.T) {
	for _, in := range []string{`(A B`, `"unterminated`, `(A "x`} {
		if _, err := tokenize(in); err == nil {
			t.Errorf("tokenize(%q) succeeded, want an error", in)
		}
	}
}

func TestTokenStringRoundTrip(t *testing.T) {
	toks, err := tokenize(`(UID 1 FLAGS (\Seen) X NIL)`)
	if err != nil {
		t.Fatal(err)
	}
	got := toks[0].String()
	want := `(UID 1 FLAGS (\Seen) X NIL)`
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestBracketedFetchItemNamesStayWhole(t *testing.T) {
	// BODY[...] is one item name. Treating the bracket as a list opener
	// splits the name from its value and loses the fetched content.
	toks, err := tokenize(`* 1 FETCH (BODY[HEADER.FIELDS (FROM TO)] "x" UID 7)`)
	if err != nil {
		t.Fatal(err)
	}
	items := toks[3]

	if _, ok := items.find("UID"); !ok {
		t.Error("UID after a bracketed item name was lost")
	}
	v, ok := items.findPrefix("BODY[")
	if !ok {
		t.Fatalf("bracketed item not found in %v", items)
	}
	if v.text != "x" {
		t.Errorf("value = %q, want x", v.text)
	}
}

func TestLeadingBracketIsStillAResponseCode(t *testing.T) {
	// The bracket fix must not break response codes, which do open a list.
	toks, err := tokenize(`* OK [PERMANENTFLAGS (\Seen \*)] Limited`)
	if err != nil {
		t.Fatal(err)
	}
	if toks[2].kind != tokenList {
		t.Fatalf("response code = %v, want a list", toks[2])
	}
	if _, ok := toks[2].find("PERMANENTFLAGS"); !ok {
		t.Error("PERMANENTFLAGS not found in the response code")
	}
}

func TestEmptyListParses(t *testing.T) {
	toks, err := tokenize(`* 1 FETCH (FLAGS ())`)
	if err != nil {
		t.Fatal(err)
	}
	flags, ok := toks[3].find("FLAGS")
	if !ok {
		t.Fatal("FLAGS not found")
	}
	if flags.kind != tokenList || len(flags.list) != 0 {
		t.Errorf("FLAGS = %v, want an empty list", flags)
	}
}
