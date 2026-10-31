// Session lifecycle invariant tests.
module session_lifecycle_test {
    import session_lifecycle.* from "../specs/framework/session_lifecycle"

    // Test: initial state is valid
    run init_valid_test = {
        init.expect(valid_phases)
    }

    // Test: create a session
    run create_test = {
        init
            .then(create_session(1))
            .expect(all { valid_phases, active_has_ttl })
    }

    // Test: touch keeps session active
    run touch_test = {
        init
            .then(create_session(1))
            .then(time_tick)
            .then(touch(1))
            .expect(all { valid_phases, active_has_ttl })
    }

    // Test: explicit revocation
    run revoke_test = {
        init
            .then(create_session(1))
            .then(revoke(1))
            .expect(all { valid_phases, revoked_is_final })
    }

    // Test: renewal extends TTL
    run renew_test = {
        init
            .then(create_session(1))
            .then(time_tick)
            .then(time_tick)
            .then(touch(1))
            .then(renew(1))
            .expect(all { valid_phases, renewal_bounded, active_has_ttl })
    }

    // Test: renewal limit is respected
    run renewal_limit_test = {
        init
            .then(create_session(1))
            .then(renew(1))
            .then(renew(1))
            .then(renew(1))
            // 3 renewals = MAX_RENEWALS, fourth should fail
            .expect(renewal_bounded)
    }

    // Test: multiple independent sessions
    run multi_session_test = {
        init
            .then(create_session(1))
            .then(create_session(2))
            .then(revoke(1))
            // Session 2 should still be active
            .then(touch(2))
            .expect(valid_phases)
    }
}
