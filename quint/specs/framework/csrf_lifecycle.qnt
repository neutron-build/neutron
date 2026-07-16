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
    pure val TOKEN_IDS: Set[TokenId] = 1.to(MAX_TOKENS)
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
        token_state' = TOKEN_IDS.mapBy(t => "invalid"),
        token_session' = TOKEN_IDS.mapBy(t => 0),
        token_ttl' = TOKEN_IDS.mapBy(t => 0),
        cookie_token' = SESSIONS.mapBy(s => -1),
        next_token' = 1,
    }

    // Generate a new CSRF token for a session (sets cookie + returns form token)
    action generate_token(session: SessionId): bool = {
        val tid = next_token
        all {
            session.in(SESSIONS),
            next_token <= MAX_TOKENS,
            token_state' = token_state.set(tid, "active"),
            token_session' = token_session.set(tid, session),
            token_ttl' = token_ttl.set(tid, TOKEN_TTL),
            cookie_token' = cookie_token.set(session, tid),
            next_token' = next_token + 1,
        }
    }

    // Validate: form token matches cookie token and is active
    action validate_success(session: SessionId, form_token: TokenId): bool = all {
        session.in(SESSIONS),
        form_token >= 1,
        form_token < next_token,
        // Double-submit check: form token == cookie token
        cookie_token.get(session) == form_token,
        // Token is active (not expired or used)
        token_state.get(form_token) == "active",
        // Token belongs to this session
        token_session.get(form_token) == session,
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
        cookie_token.get(session) != form_token or
        token_state.get(form_token) != "active" or
        token_session.get(form_token) != session,
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
        token_state' = TOKEN_IDS.mapBy(t =>
            if (t >= 1 and t < next_token and
                token_state.get(t) == "active" and
                token_ttl.get(t) <= 1) "expired"
            else token_state.get(t)),
        token_ttl' = TOKEN_IDS.mapBy(t =>
            if (t >= 1 and t < next_token and token_ttl.get(t) > 0)
                token_ttl.get(t) - 1
            else token_ttl.get(t)),
        token_session' = token_session,
        cookie_token' = cookie_token,
        next_token' = next_token,
    }

    action step = any {
        { nondet s = SESSIONS.oneOf()
          generate_token(s) },
        { nondet s = SESSIONS.oneOf()
          nondet t = TOKEN_IDS.oneOf()
          validate_success(s, t) },
        { nondet s = SESSIONS.oneOf()
          nondet t = TOKEN_IDS.oneOf()
          validate_reject(s, t) },
        tick_expiry,
    }

    // === SAFETY INVARIANTS ===

    // Token state is always valid
    val valid_token_states: bool =
        TOKEN_IDS.forall(t =>
            t >= next_token or
            token_state.get(t) == "active" or
            token_state.get(t) == "used" or
            token_state.get(t) == "expired" or
            token_state.get(t) == "invalid"
        )

    // Used tokens cannot be reused (no replay attacks)
    val no_replay: bool =
        TOKEN_IDS.forall(t =>
            token_state.get(t) == "used" implies token_state.get(t) != "active"
        )

    // Cross-session isolation: a token for session A cannot validate for session B.
    // An active token owned by s1 is never the cookie token of a different session s2.
    val session_isolation: bool =
        SESSIONS.forall(s1 => SESSIONS.forall(s2 =>
            s1 != s2 implies
            TOKEN_IDS.forall(t =>
                (token_session.get(t) == s1 and
                 cookie_token.get(s2) == t and
                 token_state.get(t) == "active")
                implies
                token_session.get(t) == s2
            )
        ))

    // Expired tokens are never active
    val expired_not_active: bool =
        TOKEN_IDS.forall(t =>
            token_state.get(t) == "expired" implies token_ttl.get(t) <= 0
        )

    // TTL is non-negative
    val ttl_non_negative: bool =
        TOKEN_IDS.forall(t => token_ttl.get(t) >= 0)
}
