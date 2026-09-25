package studio

import (
	"strings"
	"testing"
)

// S05 entry condition S03-F2/S04-F3: a timestamptz wire cell without an
// explicit UTC offset is refused (PostgreSQL would silently interpret it in
// the session timezone) with a message offering the explicit-offset and
// canonical forms. Offset-bearing and special spellings stay accepted.
func TestDecodeTaggedTimestamptzOffsetDiscipline(t *testing.T) {
	accept := []string{
		"2026-09-24T12:34:56Z",
		"2026-09-24 12:34:56+02:00",
		"2026-09-24T12:34:56.000001-07:30",
		"2026-09-24T12:34:56+05",
		"2026-09-24T12:34:56+0530",
		"2026-09-24 12:34:56 +02",
		"2026-09-24 12:34:56 UTC",
		"2026-09-24 12:34:56.5 gmt",
		"infinity",
		"-infinity",
		"epoch",
		"0001-01-01T00:00:00.5Z BC",
	}
	for _, v := range accept {
		got, err := decodeTagged(taggedCell{T: "timestamptz", V: v})
		if err != nil {
			t.Errorf("accept %q: unexpected error %v", v, err)
		}
		if got != v {
			t.Errorf("accept %q: payload changed to %#v", v, got)
		}
	}

	refuse := []string{
		"2026-09-24 12:34:56",                   // the S03 finding's Vancouver case
		"2026-09-24",                            // date-only midnight-in-session guess
		"2026-09-24T12:34:56.000001",            // microsecond precision, no zone
		"",                                      // empty payload
		"  2026-09-24 12:34:56  ",               // whitespace does not smuggle a zone
		"12:34:56+02:00",                        // time without a date is not canonical
		"2026-09-24 12:34:56 America/Vancouver", // named zones are not validated here: refused, use an offset
		"2026-09-24 12:34:56  +02",              // at most one space before the zone
	}
	for _, v := range refuse {
		_, err := decodeTagged(taggedCell{T: "timestamptz", V: v})
		if err == nil {
			t.Errorf("refuse %q: accepted", v)
			continue
		}
		if v == "" {
			continue // empty payload has its own message
		}
		if !strings.Contains(err.Error(), "no UTC offset") || !strings.Contains(err.Error(), "session timezone") {
			t.Errorf("refuse %q: message %q does not explain the hazard or the canonical form", v, err)
		}
	}

	// The timestamp (without timezone) tag keeps its canonical offset-less
	// form — only timestamptz input is disciplined.
	if got, err := decodeTagged(taggedCell{T: "timestamp", V: "2026-01-01T00:00:00"}); err != nil || got != "2026-01-01T00:00:00" {
		t.Errorf("timestamp tag changed: %#v %v", got, err)
	}
}
