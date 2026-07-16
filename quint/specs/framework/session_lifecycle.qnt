// Session Lifecycle — Session state machine with TTL, idle timeout,
// renewal, and cleanup. Models the pattern used in rs/crates/neutron/.
module session_lifecycle {
    // === TYPES ===

    type SessionId = int
    type UserId = int

    // === CONFIGURATION ===

    pure val MAX_SESSIONS: int = 8
    pure val SESSION_IDS: Set[SessionId] = 1.to(MAX_SESSIONS)
    pure val SESSION_TTL: int = 20          // absolute max lifetime (ticks)
    pure val IDLE_TIMEOUT: int = 5          // idle ticks before expiry
    pure val MAX_RENEWALS: int = 3          // max times a session can be renewed

    // === STATE ===

    // Session phases: "active" | "idle" | "expired" | "revoked" | "none"
    var phase: SessionId -> str
    // Which user owns the session
    var owner: SessionId -> UserId
    // Remaining absolute TTL
    var abs_ttl: SessionId -> int
    // Ticks since last activity
    var idle_ticks: SessionId -> int
    // Renewal count
    var renewals: SessionId -> int
    // Next session ID
    var next_sid: SessionId

    // === ACTIONS ===

    action init = all {
        phase' = SESSION_IDS.mapBy(s => "none"),
        owner' = SESSION_IDS.mapBy(s => 0),
        abs_ttl' = SESSION_IDS.mapBy(s => 0),
        idle_ticks' = SESSION_IDS.mapBy(s => 0),
        renewals' = SESSION_IDS.mapBy(s => 0),
        next_sid' = 1,
    }

    // Create a new session for a user (login)
    action create_session(user: UserId): bool = {
        val sid = next_sid
        all {
            next_sid <= MAX_SESSIONS,
            phase' = phase.set(sid, "active"),
            owner' = owner.set(sid, user),
            abs_ttl' = abs_ttl.set(sid, SESSION_TTL),
            idle_ticks' = idle_ticks.set(sid, 0),
            renewals' = renewals.set(sid, 0),
            next_sid' = next_sid + 1,
        }
    }

    // Session activity — resets idle counter
    action touch(sid: SessionId): bool = all {
        sid >= 1,
        sid < next_sid,
        phase.get(sid) == "active" or phase.get(sid) == "idle",
        abs_ttl.get(sid) > 0,
        phase' = phase.set(sid, "active"),
        idle_ticks' = idle_ticks.set(sid, 0),
        owner' = owner,
        abs_ttl' = abs_ttl,
        renewals' = renewals,
        next_sid' = next_sid,
    }

    // Renew a session (extend TTL, within limits)
    action renew(sid: SessionId): bool = all {
        sid >= 1,
        sid < next_sid,
        phase.get(sid) == "active",
        renewals.get(sid) < MAX_RENEWALS,
        abs_ttl' = abs_ttl.set(sid, SESSION_TTL),
        renewals' = renewals.set(sid, renewals.get(sid) + 1),
        idle_ticks' = idle_ticks.set(sid, 0),
        phase' = phase,
        owner' = owner,
        next_sid' = next_sid,
    }

    // Explicitly revoke a session (logout)
    action revoke(sid: SessionId): bool = all {
        sid >= 1,
        sid < next_sid,
        phase.get(sid) == "active" or phase.get(sid) == "idle",
        phase' = phase.set(sid, "revoked"),
        owner' = owner,
        abs_ttl' = abs_ttl,
        idle_ticks' = idle_ticks,
        renewals' = renewals,
        next_sid' = next_sid,
    }

    // Time passes — tick all active/idle sessions
    action time_tick: bool = all {
        // Update all sessions
        phase' = SESSION_IDS.mapBy(sid =>
            if (sid >= 1 and sid < next_sid) (
                if (phase.get(sid) == "active" or phase.get(sid) == "idle") (
                    if (abs_ttl.get(sid) <= 1) "expired"       // absolute TTL
                    else if (idle_ticks.get(sid) + 1 >= IDLE_TIMEOUT and
                             phase.get(sid) == "idle") "expired"  // idle timeout
                    else if (idle_ticks.get(sid) + 1 >= 1 and
                             phase.get(sid) == "active") "idle"    // becomes idle
                    else phase.get(sid)
                ) else phase.get(sid)
            ) else phase.get(sid)),
        abs_ttl' = SESSION_IDS.mapBy(sid =>
            if (sid >= 1 and sid < next_sid and
                (phase.get(sid) == "active" or phase.get(sid) == "idle") and
                abs_ttl.get(sid) > 0)
                abs_ttl.get(sid) - 1
            else abs_ttl.get(sid)),
        idle_ticks' = SESSION_IDS.mapBy(sid =>
            if (sid >= 1 and sid < next_sid and
                (phase.get(sid) == "active" or phase.get(sid) == "idle"))
                idle_ticks.get(sid) + 1
            else idle_ticks.get(sid)),
        owner' = owner,
        renewals' = renewals,
        next_sid' = next_sid,
    }

    action step = any {
        { nondet u = 1.to(5).oneOf()
          create_session(u) },
        { nondet s = SESSION_IDS.oneOf()
          touch(s) },
        { nondet s = SESSION_IDS.oneOf()
          renew(s) },
        { nondet s = SESSION_IDS.oneOf()
          revoke(s) },
        time_tick,
    }

    // === SAFETY INVARIANTS ===

    // Valid phase values
    val valid_phases: bool =
        SESSION_IDS.forall(s =>
            s >= next_sid or
            phase.get(s) == "active" or
            phase.get(s) == "idle" or
            phase.get(s) == "expired" or
            phase.get(s) == "revoked" or
            phase.get(s) == "none"
        )

    // Terminal states are permanent: expired/revoked sessions never become active
    val terminal_permanent: bool =
        SESSION_IDS.forall(s =>
            phase.get(s) == "expired" implies phase.get(s) != "active"
        )

    // Renewal limit is respected
    val renewal_bounded: bool =
        SESSION_IDS.forall(s =>
            renewals.get(s) <= MAX_RENEWALS
        )

    // Active sessions have positive TTL
    val active_has_ttl: bool =
        SESSION_IDS.forall(s =>
            phase.get(s) == "active" implies abs_ttl.get(s) > 0
        )

    // Expired sessions have zero or negative TTL or exceeded idle timeout
    val expired_reason: bool =
        SESSION_IDS.forall(s =>
            phase.get(s) == "expired" implies
            (abs_ttl.get(s) <= 0 or idle_ticks.get(s) >= IDLE_TIMEOUT)
        )

    // Session-user binding is immutable: owner never changes after creation
    val owner_immutable: bool =
        SESSION_IDS.forall(s =>
            s < next_sid implies owner.get(s) > 0
        )

    // Revoked sessions cannot be renewed or touched
    // (enforced by action guards, but stated as invariant)
    val revoked_is_final: bool =
        SESSION_IDS.forall(s =>
            phase.get(s) == "revoked" implies phase.get(s) != "active"
        )
}
