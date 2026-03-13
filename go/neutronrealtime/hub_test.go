package neutronrealtime

import (
	"testing"
)

func TestHubRegisterUnregister(t *testing.T) {
	hub := NewHub()
	conn := NewConn("c1", 10)

	hub.Register(conn)
	if hub.ConnectionCount() != 1 {
		t.Errorf("connections = %d, want 1", hub.ConnectionCount())
	}

	hub.Unregister(conn)
	if hub.ConnectionCount() != 0 {
		t.Errorf("connections = %d, want 0", hub.ConnectionCount())
	}
}

func TestHubSubscribeBroadcast(t *testing.T) {
	hub := NewHub()
	c1 := NewConn("c1", 10)
	c2 := NewConn("c2", 10)
	c3 := NewConn("c3", 10)

	hub.Register(c1)
	hub.Register(c2)
	hub.Register(c3)

	hub.Subscribe("room1", c1)
	hub.Subscribe("room1", c2)
	// c3 not in room1

	hub.Broadcast("room1", []byte("hello"))

	// c1 and c2 should receive
	select {
	case msg := <-c1.Send:
		if string(msg) != "hello" {
			t.Errorf("c1 msg = %q", string(msg))
		}
	default:
		t.Error("c1 should have received message")
	}

	select {
	case msg := <-c2.Send:
		if string(msg) != "hello" {
			t.Errorf("c2 msg = %q", string(msg))
		}
	default:
		t.Error("c2 should have received message")
	}

	// c3 should not
	select {
	case <-c3.Send:
		t.Error("c3 should not have received message")
	default:
		// expected
	}
}

func TestHubBroadcastAll(t *testing.T) {
	hub := NewHub()
	c1 := NewConn("c1", 10)
	c2 := NewConn("c2", 10)
	hub.Register(c1)
	hub.Register(c2)

	hub.BroadcastAll([]byte("global"))

	select {
	case msg := <-c1.Send:
		if string(msg) != "global" {
			t.Errorf("c1 = %q", string(msg))
		}
	default:
		t.Error("c1 should have received")
	}
	select {
	case msg := <-c2.Send:
		if string(msg) != "global" {
			t.Errorf("c2 = %q", string(msg))
		}
	default:
		t.Error("c2 should have received")
	}
}

func TestHubRoomCount(t *testing.T) {
	hub := NewHub()
	c1 := NewConn("c1", 10)
	c2 := NewConn("c2", 10)
	hub.Register(c1)
	hub.Register(c2)
	hub.Subscribe("room1", c1)
	hub.Subscribe("room1", c2)

	if hub.RoomCount("room1") != 2 {
		t.Errorf("room count = %d, want 2", hub.RoomCount("room1"))
	}
}

func TestHubUnsubscribe(t *testing.T) {
	hub := NewHub()
	c1 := NewConn("c1", 10)
	hub.Register(c1)
	hub.Subscribe("room1", c1)
	hub.Unsubscribe("room1", c1)

	if hub.RoomCount("room1") != 0 {
		t.Errorf("room count = %d, want 0", hub.RoomCount("room1"))
	}
}

func TestHubUnregisterCleansRooms(t *testing.T) {
	hub := NewHub()
	c1 := NewConn("c1", 10)
	hub.Register(c1)
	hub.Subscribe("room1", c1)
	hub.Subscribe("room2", c1)
	hub.Unregister(c1)

	if hub.RoomCount("room1") != 0 {
		t.Errorf("room1 count = %d, want 0", hub.RoomCount("room1"))
	}
	if hub.RoomCount("room2") != 0 {
		t.Errorf("room2 count = %d, want 0", hub.RoomCount("room2"))
	}
}

func TestConnStruct(t *testing.T) {
	c := NewConn("test-id", 5)
	if c.ID != "test-id" {
		t.Errorf("ID = %q", c.ID)
	}
}
