package imap

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/mail"
)

// fakeServer speaks just enough IMAP for a Dial + APPEND exchange: greeting,
// CAPABILITY, LOGIN, and a synchronising-literal APPEND whose command line
// and literal bytes are recorded for assertions.
type fakeServer struct {
	ln net.Listener

	refuseAppend   bool
	refuseText     string
	appendedLine   string
	appendedBytes  []byte
	sawLiteralData bool
}

func startFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeServer{ln: ln}
	go f.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeServer) addr() (string, int) {
	host, portStr, _ := net.SplitHostPort(f.ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return host, port
}

func (f *fakeServer) serve() {
	conn, err := f.ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	r := bufio.NewReader(conn)
	send := func(s string) { _, _ = io.WriteString(conn, s+"\r\n") }

	send("* OK fake ready")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return
		}
		tag, cmd := fields[0], strings.ToUpper(fields[1])
		switch cmd {
		case "CAPABILITY":
			send("* CAPABILITY IMAP4rev1")
			send(tag + " OK done")
		case "LOGIN":
			send(tag + " OK logged in")
		case "LOGOUT":
			send("* BYE")
			send(tag + " OK logged out")
			return
		case "APPEND":
			f.appendedLine = line
			if f.refuseAppend {
				reason := f.refuseText
				if reason == "" {
					reason = "[TRYCREATE] no such mailbox"
				}
				send(tag + " NO " + reason)
				continue
			}
			var n int
			if _, err := fmt.Sscanf(line[strings.LastIndex(line, "{"):], "{%d}", &n); err != nil {
				return
			}
			send("+ ready for literal")
			buf := make([]byte, n+2)
			if _, err := io.ReadFull(r, buf); err != nil {
				return
			}
			f.appendedBytes = buf[:n]
			if n > 0 {
				f.sawLiteralData = true
			}
			send(tag + " OK APPEND completed")
		default:
			send(tag + " BAD unsupported")
		}
	}
}

func dialFake(t *testing.T, f *fakeServer) *Conn {
	t.Helper()
	host, port := f.addr()
	conn, err := Dial(t.Context(), Config{Host: host, Port: port, Username: "u", Password: "p", Plaintext: true, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("dial fake: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestConnAppendSendsSynchronisingLiteral(t *testing.T) {
	f := startFakeServer(t)
	conn := dialFake(t, f)

	message := []byte("From: a@x.com\r\nSubject: hi\r\n\r\nbody with\r\nnewline {braces}\r\n")
	if err := conn.Append(t.Context(), "Sent", message); err != nil {
		t.Fatalf("append: %v", err)
	}

	if !strings.Contains(f.appendedLine, `APPEND "Sent" (\Seen) {`) {
		t.Errorf("append command malformed: %q", f.appendedLine)
	}
	if !strings.HasSuffix(f.appendedLine, fmt.Sprintf("{%d}", len(message))) {
		t.Errorf("literal size wrong: %q", f.appendedLine)
	}
	if string(f.appendedBytes) != string(message) {
		t.Errorf("literal bytes changed:\n got %q\nwant %q", f.appendedBytes, message)
	}
}

func TestAdapterAppendRoutesNativeMailbox(t *testing.T) {
	f := startFakeServer(t)
	conn := dialFake(t, f)
	ad := New(conn)

	message := []byte("From: a@x.com\r\n\r\nbody\r\n")
	if err := ad.Append(t.Context(), mail.MailboxID("Sent Items"), message); err != nil {
		t.Fatalf("adapter append: %v", err)
	}
	if !strings.Contains(f.appendedLine, `APPEND "Sent Items" `) {
		t.Errorf("mailbox not quoted into command: %q", f.appendedLine)
	}
}

func TestConnAppendRefusalBeforeLiteral(t *testing.T) {
	f := startFakeServer(t)
	f.refuseAppend = true
	conn := dialFake(t, f)

	err := conn.Append(t.Context(), "Missing", []byte("From: a@x.com\r\n\r\nbody\r\n"))
	if err == nil {
		t.Fatal("refused append reported success")
	}
	if f.sawLiteralData {
		t.Error("literal bytes were sent after a refusal")
	}
}

func TestConnAppendMapsReauthRefusal(t *testing.T) {
	f := startFakeServer(t)
	f.refuseAppend = true
	f.refuseText = "AUTHENTICATIONFAILED"
	conn := dialFake(t, f)

	err := conn.Append(t.Context(), "Sent", []byte("x"))
	if !errors.Is(err, mail.ErrReauthRequired) {
		t.Fatalf("authentication failure not mapped to ErrReauthRequired: %v", err)
	}
}
