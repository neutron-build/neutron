package nucleus

import (
	"testing"
)

func TestQuoteIdent(t *testing.T) {
	got := quoteIdent("my_channel")
	want := `"my_channel"`
	if got != want {
		t.Errorf("quoteIdent = %q, want %q", got, want)
	}
}

func TestNotificationStruct(t *testing.T) {
	n := Notification{Channel: "events", Payload: `{"type":"update"}`}
	if n.Channel != "events" {
		t.Errorf("Channel = %q", n.Channel)
	}
	if n.Payload != `{"type":"update"}` {
		t.Errorf("Payload = %q", n.Payload)
	}
}

func TestQuoteIdentWithDoubleQuotes(t *testing.T) {
	got := quoteIdent(`my"channel`)
	want := `"my""channel"`
	if got != want {
		t.Errorf("quoteIdent = %q, want %q", got, want)
	}
}

func TestQuoteIdentEmpty(t *testing.T) {
	got := quoteIdent("")
	want := `""`
	if got != want {
		t.Errorf("quoteIdent = %q, want %q", got, want)
	}
}

func TestQuoteIdentSpecialChars(t *testing.T) {
	got := quoteIdent("a;b--c")
	want := `"a;b--c"`
	if got != want {
		t.Errorf("quoteIdent = %q, want %q", got, want)
	}
}

func TestNotificationEmptyPayload(t *testing.T) {
	n := Notification{Channel: "ch", Payload: ""}
	if n.Payload != "" {
		t.Error("expected empty payload")
	}
}
