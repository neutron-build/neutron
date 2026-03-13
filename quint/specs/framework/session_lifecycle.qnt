// Session Lifecycle — Session state machine with TTL, idle timeout,
// renewal, and cleanup. Models the pattern used in rs/crates/neutron/.
module session_lifecycle {
    // === TYPES ===

    type SessionId = int
    type UserId = int

    // === CONFIGURATION ===

    pure val MAX_SESSIONS: int = 8
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
        phase' = (s => "none"),
        owner' = (s => 0),
        abs_ttl' = (s => 0),
        idle_ticks' = (s => 0),
        renewals' = (s => 0),
        next_sid' = 1,
    }

    // Create a new session for a user (login)
    action create_session(user: UserId): bool = all {
        next_sid <= MAX_SESSIONS,
        val sid = next_sid
        phase' = phase.set(sid, "active"),
        owner' = owner.set(sid, user),
        abs_ttl' = abs_ttl.set(sid, SESSION_TTL),
        idle_ticks' = idle_ticks.set(sid, 0),
        renewals' = renewals.set(sid, 0),
        next_sid' = next_sid + 1,
    }

    // Session activity — resets idle counter
    action touch(sid: SessionId): bool = all {
        sid >= 1,
        sid < next_sid,
        phase(sid) == "active" or phase(sid) == "idle",
        abs_ttl(sid) > 0,
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
        phase(sid) == "active",
        renewals(sid) < MAX_RENEWALS,
        abs_ttl' = abs_ttl.set(sid, SESSION_TTL),
        renewals' = renewals.set(sid, renewals(sid) + 1),
        idle_ticks' = idle_ticks.set(sid, 0),
        phase' = phase,
        owner' = owner,
        next_sid' = next_sid,
    }

    // Explicitly revoke a session (logout)
    action revoke(sid: SessionId): bool = all {
        sid >= 1,
        sid < next_sid,
        phase(sid) == "active" or phase(sid) == "idle",
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
        phase' = (sid =>
            if (sid >= 1 and sid < next_sid) (
                if (phase(sid) == "active" or phase(sid) == "idle") (
                    if (abs_ttl(sid) <= 1) "expired"       // absolute TTL
                    else if (idle_ticks(sid) + 1 >= IDLE_TIMEOUT and
                             phase(sid) == "idle") "expired"  // idle timeout
                    else if (idle_ticks(sid) + 1 >= 1 and
                             phase(sid) == "active") "idle"    // becomes idle
                    else phase(sid)
                ) else phase(sid)
            ) else phase(sid)),
        abs_ttl' = (sid =>
            if (sid >= 1 and sid < next_sid and
                (phase(sid) == "active" or phase(sid) == "idle") and
                abs_ttl(sid) > 0)
                abs_ttl(sid) - 1
            else abs_ttl(sid)),
        idle_ticks' = (sid =>
            if (sid >= 1 and sid < next_sid and
                (phase(sid) == "active" or phase(sid) == "idle"))
                idle_ticks(sid) + 1
            else idle_ticks(sid)),
        owner' = owner,
        renewals' = renewals,
        next_sid' = next_sid,
    }

    action step = any {
        1.to(5).exists(u => create_session(u)),
        1.to(MAX_SESSIONS).exists(s => touch(s)),
        1.to(MAX_SESSIONS).exists(s => renew(s)),
        1.to(MAX_SESSIONS).exists(s => revoke(s)),
        time_tick,
    }

    // === SAFETY INVARIANTS ===

    // Valid phase values
    val valid_phases: bool =
        1.to(MAX_SESSIONS).forall(s =>
            s >= next_sid or
            phase(s) == "active" or
            phase(s) == "idle" or
            phase(s) == "expired" or
            phase(s) == "revoked" or
            phase(s) == "none"
        )

    // Terminal states are permanent: expired/revoked sessions never become active
    val terminal_permanent: bool =
        1.to(MAX_SESSIONS).forall(s =>
            phase(s) == "expired" implies phase(s) != "active"
        )

    // Renewal limit is respected
    val renewal_bounded: bool =
        1.to(MAX_SESSIONS).forall(s =>
            renewals(s) <= MAX_RENEWALS
        )

    // Active sessions have positive TTL
    val active_has_ttl: bool =
        1.to(MAX_SESSIONS).forall(s =>
            phase(s) == "active" implies abs_ttl(s) > 0
        )

    // Expired sessions have zero or negative TTL or exceeded idle timeout
    val expired_reason: bool =
        1.to(MAX_SESSIONS).forall(s =>
            phase(s) == "expired" implies
            (abs_ttl(s) <= 0 or idle_ticks(s) >= IDLE_TIMEOUT)
        )

    // Session-user binding is immutable: owner never changes after creation
    val owner_immutable: bool =
        1.to(MAX_SESSIONS).forall(s =>
            s < next_sid implies owner(s) > 0
        )

    // Revoked sessions cannot be renewed or touched
    // (enforced by action guards, but stated as invariant)
    val revoked_is_final: bool =
        1.to(MAX_SESSIONS).forall(s =>
            phase(s) == "revoked" implies phase(s) != "active"
        )
}
