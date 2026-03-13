package nucleus

import (
	"testing"
)

func TestStreamEntryStruct(t *testing.T) {
	e := StreamEntry{
		ID:     "1234-0",
		Fields: map[string]any{"temp": 72.5},
	}
	if e.ID != "1234-0" {
		t.Errorf("ID = %q", e.ID)
	}
	if e.Fields["temp"] != 72.5 {
		t.Errorf("Fields = %v", e.Fields)
	}
}
