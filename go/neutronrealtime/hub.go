package neutronrealtime

import (
	"sync"
)

// Conn represents a client connection (WebSocket or other transport).
type Conn struct {
	ID     string
	Send   chan []byte
	rooms  map[string]bool
	mu     sync.Mutex
}

// NewConn creates a new connection.
func NewConn(id string, bufSize int) *Conn {
	return &Conn{
		ID:    id,
		Send:  make(chan []byte, bufSize),
		rooms: make(map[string]bool),
	}
}

// Hub manages WebSocket/realtime connections with room-based broadcasting.
type Hub struct {
	mu          sync.RWMutex
	connections map[string]*Conn
	rooms       map[string]map[string]*Conn // room -> connID -> conn
}

// NewHub creates a new connection hub.
func NewHub() *Hub {
	return &Hub{
		connections: make(map[string]*Conn),
		rooms:       make(map[string]map[string]*Conn),
	}
}

// Register adds a connection to the hub.
func (h *Hub) Register(conn *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connections[conn.ID] = conn
}

// Unregister removes a connection from the hub and all its rooms.
func (h *Hub) Unregister(conn *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	conn.mu.Lock()
	for room := range conn.rooms {
		if members, ok := h.rooms[room]; ok {
			delete(members, conn.ID)
			if len(members) == 0 {
				delete(h.rooms, room)
			}
		}
	}
	conn.mu.Unlock()

	delete(h.connections, conn.ID)
	close(conn.Send)
}

// Subscribe adds a connection to a room.
func (h *Hub) Subscribe(room string, conn *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.rooms[room]; !ok {
		h.rooms[room] = make(map[string]*Conn)
	}
	h.rooms[room][conn.ID] = conn

	conn.mu.Lock()
	conn.rooms[room] = true
	conn.mu.Unlock()
}

// Unsubscribe removes a connection from a room.
func (h *Hub) Unsubscribe(room string, conn *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if members, ok := h.rooms[room]; ok {
		delete(members, conn.ID)
		if len(members) == 0 {
			delete(h.rooms, room)
		}
	}

	conn.mu.Lock()
	delete(conn.rooms, room)
	conn.mu.Unlock()
}

// Broadcast sends a message to all connections in a room.
func (h *Hub) Broadcast(room string, msg []byte) {
	h.mu.RLock()
	members, ok := h.rooms[room]
	if !ok {
		h.mu.RUnlock()
		return
	}
	// Copy to avoid holding lock during sends
	conns := make([]*Conn, 0, len(members))
	for _, c := range members {
		conns = append(conns, c)
	}
	h.mu.RUnlock()

	for _, c := range conns {
		select {
		case c.Send <- msg:
		default:
			// Drop message if buffer full
		}
	}
}

// BroadcastAll sends a message to all connected clients.
func (h *Hub) BroadcastAll(msg []byte) {
	h.mu.RLock()
	conns := make([]*Conn, 0, len(h.connections))
	for _, c := range h.connections {
		conns = append(conns, c)
	}
	h.mu.RUnlock()

	for _, c := range conns {
		select {
		case c.Send <- msg:
		default:
		}
	}
}

// ConnectionCount returns the total number of connections.
func (h *Hub) ConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.connections)
}

// RoomCount returns the number of connections in a room.
func (h *Hub) RoomCount(room string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.rooms[room])
}
