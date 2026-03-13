package neutronrealtime

import (
	"testing"
)

func TestGenerateConnID(t *testing.T) {
	id1 := generateConnID()
	id2 := generateConnID()
	if id1 == "" {
		t.Error("empty conn ID")
	}
	if id1 == id2 {
		t.Error("conn IDs should be unique")
	}
	if len(id1) != 32 { // 16 bytes hex-encoded
		t.Errorf("conn ID length = %d, want 32", len(id1))
	}
}

func TestWebSocketHandlerNotNil(t *testing.T) {
	hub := NewHub()
	// Just verify it returns a non-nil handler
	handler := WebSocketHandler(hub, nil)
	if handler == nil {
		t.Error("handler should not be nil")
	}
}

func TestWebSocketHandlerWithRoomNotNil(t *testing.T) {
	hub := NewHub()
	handler := WebSocketHandlerWithRoom(hub, "test-room", nil)
	if handler == nil {
		t.Error("handler should not be nil")
	}
}
