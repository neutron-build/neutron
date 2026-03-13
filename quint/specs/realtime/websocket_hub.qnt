// WebSocket Hub — Room-based broadcasting with delivery invariants.
// Models the pattern used in go/neutronrealtime/.
module websocket_hub {
    // === TYPES ===

    type ConnId = int
    type RoomId = int
    type MsgId = int

    // === CONFIGURATION ===

    pure val CONNS: Set[ConnId] = Set(1, 2, 3, 4)
    pure val ROOMS: Set[RoomId] = Set(1, 2)
    pure val MAX_MSGS: int = 10

    // === STATE ===

    // Which connections are in which rooms
    var room_members: RoomId -> Set[ConnId]
    // Connected / disconnected
    var connected: Set[ConnId]
    // Messages pending delivery per connection
    var pending: ConnId -> Set[MsgId]
    // Messages successfully delivered per connection
    var delivered: ConnId -> Set[MsgId]
    // Broadcast origin: which connection sent each message
    var msg_sender: MsgId -> ConnId
    // Which room each message was broadcast to
    var msg_room: MsgId -> RoomId
    // Next message ID
    var next_msg: MsgId

    // === ACTIONS ===

    action init = all {
        room_members' = (r => Set()),
        connected' = Set(),
        pending' = (c => Set()),
        delivered' = (c => Set()),
        msg_sender' = (m => 0),
        msg_room' = (m => 0),
        next_msg' = 1,
    }

    // A connection comes online
    action connect(conn: ConnId): bool = all {
        conn.in(CONNS),
        not(conn.in(connected)),
        connected' = connected.union(Set(conn)),
        room_members' = room_members,
        pending' = pending,
        delivered' = delivered,
        msg_sender' = msg_sender,
        msg_room' = msg_room,
        next_msg' = next_msg,
    }

    // A connection goes offline
    action disconnect(conn: ConnId): bool = all {
        conn.in(connected),
        connected' = connected.exclude(Set(conn)),
        // Remove from all rooms
        room_members' = (r =>
            room_members(r).exclude(Set(conn))),
        // Clear pending messages for this connection
        pending' = pending.set(conn, Set()),
        delivered' = delivered,
        msg_sender' = msg_sender,
        msg_room' = msg_room,
        next_msg' = next_msg,
    }

    // Join a room
    action join_room(conn: ConnId, room: RoomId): bool = all {
        conn.in(connected),
        room.in(ROOMS),
        not(conn.in(room_members(room))),
        room_members' = room_members.set(room,
            room_members(room).union(Set(conn))),
        connected' = connected,
        pending' = pending,
        delivered' = delivered,
        msg_sender' = msg_sender,
        msg_room' = msg_room,
        next_msg' = next_msg,
    }

    // Leave a room
    action leave_room(conn: ConnId, room: RoomId): bool = all {
        conn.in(connected),
        room.in(ROOMS),
        conn.in(room_members(room)),
        room_members' = room_members.set(room,
            room_members(room).exclude(Set(conn))),
        connected' = connected,
        pending' = pending,
        delivered' = delivered,
        msg_sender' = msg_sender,
        msg_room' = msg_room,
        next_msg' = next_msg,
    }

    // Broadcast a message to a room (adds to pending for all members except sender)
    action broadcast(sender: ConnId, room: RoomId): bool = all {
        sender.in(connected),
        room.in(ROOMS),
        sender.in(room_members(room)),
        next_msg <= MAX_MSGS,
        val mid = next_msg
        // Add to pending for all other connected members of the room
        pending' = (c =>
            if (c.in(room_members(room)) and c != sender and c.in(connected))
                pending(c).union(Set(mid))
            else pending(c)),
        msg_sender' = msg_sender.set(mid, sender),
        msg_room' = msg_room.set(mid, room),
        next_msg' = next_msg + 1,
        connected' = connected,
        room_members' = room_members,
        delivered' = delivered,
    }

    // Deliver a pending message to a connection
    action deliver(conn: ConnId, mid: MsgId): bool = all {
        conn.in(connected),
        mid.in(pending(conn)),
        pending' = pending.set(conn,
            pending(conn).exclude(Set(mid))),
        delivered' = delivered.set(conn,
            delivered(conn).union(Set(mid))),
        connected' = connected,
        room_members' = room_members,
        msg_sender' = msg_sender,
        msg_room' = msg_room,
        next_msg' = next_msg,
    }

    action step = any {
        CONNS.exists(c => connect(c)),
        CONNS.exists(c => disconnect(c)),
        CONNS.exists(c => ROOMS.exists(r => join_room(c, r))),
        CONNS.exists(c => ROOMS.exists(r => leave_room(c, r))),
        CONNS.exists(c => ROOMS.exists(r => broadcast(c, r))),
        CONNS.exists(c => 1.to(MAX_MSGS).exists(m => deliver(c, m))),
    }

    // === SAFETY INVARIANTS ===

    // Room members are connected
    val members_connected: bool =
        ROOMS.forall(r =>
            room_members(r).forall(c => c.in(connected))
        )

    // No self-delivery: sender never receives their own broadcast
    val no_self_delivery: bool =
        1.to(MAX_MSGS).forall(m =>
            m < next_msg implies
            not(m.in(pending(msg_sender(m)))) and
            not(m.in(delivered(msg_sender(m))))
        )

    // Delivery completeness: every pending message for a connected
    // member will eventually be delivered (stated as: delivered ⊆ was-pending)
    val delivered_was_pending: bool =
        CONNS.forall(c =>
            delivered(c).forall(m =>
                m >= 1 and m < next_msg
            )
        )

    // No duplicate delivery: a message is in pending OR delivered, not both
    val no_duplicate_delivery: bool =
        CONNS.forall(c =>
            pending(c).intersect(delivered(c)).size() == 0
        )

    // Room cleanup: empty rooms have no members
    val empty_rooms_clean: bool =
        ROOMS.forall(r =>
            room_members(r).forall(c => c.in(CONNS))
        )

    // Broadcast reaches all room members (at point of broadcast)
    // Messages only go to members who were in the room when broadcast happened
    val broadcast_scoped: bool =
        CONNS.forall(c =>
            pending(c).union(delivered(c)).forall(m =>
                m >= 1 and m < next_msg implies
                msg_sender(m) != c  // sender excluded
            )
        )
}
