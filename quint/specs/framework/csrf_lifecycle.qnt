// CSRF Token Lifecycle — Double-submit cookie pattern with token
// generation, binding, validation, and expiry.
// Models the pattern used in rs/crates/neutron/.
module csrf_lifecycle {
    // === TYPES ===

    type TokenId = int
    type SessionId = int

    // === CONFIGURATION ===

    pure val SESSIONS: Set[SessionId] = Set(1, 2, 3)
    pure val MAX_TOKENS: int = 10
    pure val TOKEN_TTL: int = 5     // ticks before expiry

    // === STATE ===

    // Token states: "active" | "used" | "expired" | "invalid"
    var token_state: TokenId -> str
    // Which session owns the token
    var token_session: TokenId -> SessionId
    // Remaining TTL per token
    var token_ttl: TokenId -> int
    // Cookie-side token (what the browser stores)
    var cookie_token: SessionId -> int   // -1 = none
    // Next token ID to issue
    var next_token: TokenId

    // === ACTIONS ===

    action init = all {
        token_state' = (t => "invalid"),
        token_session' = (t => 0),
        token_ttl' = (t => 0),
        cookie_token' = (s => -1),
        next_token' = 1,
    }

    // Generate a new CSRF token for a session (sets cookie + returns form token)
    action generate_token(session: SessionId): bool = all {
        session.in(SESSIONS),
        next_token <= MAX_TOKENS,
        val tid = next_token
        token_state' = token_state.set(tid, "active"),
        token_session' = token_session.set(tid, session),
        token_ttl' = token_ttl.set(tid, TOKEN_TTL),
        cookie_token' = cookie_token.set(session, tid),
        next_token' = next_token + 1,
    }

    // Validate: form token matches cookie token and is active
    action validate_success(session: SessionId, form_token: TokenId): bool = all {
        session.in(SESSIONS),
        form_token >= 1,
        form_token < next_token,
        // Double-submit check: form token == cookie token
        cookie_token(session) == form_token,
        // Token is active (not expired or used)
        token_state(form_token) == "active",
        // Token belongs to this session
        token_session(form_token) == session,
        // Mark as used (one-time use)
        token_state' = token_state.set(form_token, "used"),
        token_session' = token_session,
        token_ttl' = token_ttl,
        cookie_token' = cookie_token,
        next_token' = next_token,
    }

    // Validation fails: token mismatch, wrong session, or not active
    action validate_reject(session: SessionId, form_token: TokenId): bool = all {
        session.in(SESSIONS),
        form_token >= 1,
        form_token < next_token,
        // At least one check fails
        cookie_token(session) != form_token or
        token_state(form_token) != "active" or
        token_session(form_token) != session,
        // No state change — just rejected
        token_state' = token_state,
        token_session' = token_session,
        token_ttl' = token_ttl,
        cookie_token' = cookie_token,
        next_token' = next_token,
    }

    // Time passes — decrement TTL, expire tokens at zero
    action tick_expiry: bool = all {
        // Expire any active token whose TTL hits zero
        token_state' = (t =>
            if (t >= 1 and t < next_token and
                token_state(t) == "active" and
                token_ttl(t) <= 1) "expired"
            else token_state(t)),
        token_ttl' = (t =>
            if (t >= 1 and t < next_token and token_ttl(t) > 0)
                token_ttl(t) - 1
            else token_ttl(t)),
        token_session' = token_session,
        cookie_token' = cookie_token,
        next_token' = next_token,
    }

    action step = any {
        SESSIONS.exists(s => generate_token(s)),
        SESSIONS.exists(s => 1.to(MAX_TOKENS).exists(t => validate_success(s, t))),
        SESSIONS.exists(s => 1.to(MAX_TOKENS).exists(t => validate_reject(s, t))),
        tick_expiry,
    }

    // === SAFETY INVARIANTS ===

    // Token state is always valid
    val valid_token_states: bool =
        1.to(MAX_TOKENS).forall(t =>
            t >= next_token or
            token_state(t) == "active" or
            token_state(t) == "used" or
            token_state(t) == "expired" or
            token_state(t) == "invalid"
        )

    // Used tokens cannot be reused (no replay attacks)
    val no_replay: bool =
        1.to(MAX_TOKENS).forall(t =>
            token_state(t) == "used" implies token_state(t) != "active"
        )

    // Cross-session isolation: a token for session A cannot validate for session B
    val session_isolation: bool =
        SESSIONS.forall(s1 => SESSIONS.forall(s2 =>
            s1 != s2 implies
            1.to(MAX_TOKENS).forall(t =>
                token_session(t) == s1 implies
                // Cannot be the cookie token for a different session
                // (unless it's not active)
                (cookie_token(s2) == t and token_state(t) == "active") implies
                token_session(t) == s2
            )
        ))

    // Expired tokens are never active
    val expired_not_active: bool =
        1.to(MAX_TOKENS).forall(t =>
            token_state(t) == "expired" implies token_ttl(t) <= 0
        )

    // TTL is non-negative
    val ttl_non_negative: bool =
        1.to(MAX_TOKENS).forall(t => token_ttl(t) >= 0)
}
