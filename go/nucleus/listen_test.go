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
