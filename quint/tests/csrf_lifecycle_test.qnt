// CSRF lifecycle invariant tests.
module csrf_lifecycle_test {
    import csrf_lifecycle.*

    // Test: initial state is valid
    run init_valid_test = {
        init.then(all { valid_token_states, ttl_non_negative })
    }

    // Test: generate and validate a token
    run generate_validate_test = {
        init
            .then(generate_token(1))
            .then(validate_success(1, 1))
            .then(all { valid_token_states, no_replay })
    }

    // Test: used token cannot be reused
    run no_replay_test = {
        init
            .then(generate_token(1))
            .then(validate_success(1, 1))
            // Token 1 is now "used" — validate_success should fail,
            // but validate_reject should succeed
            .then(validate_reject(1, 1))
            .then(no_replay)
    }

    // Test: cross-session token rejection
    run cross_session_test = {
        init
            .then(generate_token(1))  // token 1 belongs to session 1
            // Session 2 tries to use session 1's token
            .then(validate_reject(2, 1))
            .then(session_isolation)
    }

    // Test: token expiry
    run expiry_test = {
        init
            .then(generate_token(1))
            // Tick past TTL
            .then(tick_expiry)
            .then(tick_expiry)
            .then(tick_expiry)
            .then(tick_expiry)
            .then(tick_expiry)
            .then(all { valid_token_states, expired_not_active })
    }

    // Test: multiple sessions generate independent tokens
    run multi_session_test = {
        init
            .then(generate_token(1))
            .then(generate_token(2))
            .then(validate_success(1, 1))
            .then(validate_success(2, 2))
            .then(all { valid_token_states, session_isolation })
    }
}
