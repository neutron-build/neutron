# Framework Protocol Specifications

Quint specifications for stateful protocols in the Neutron framework ecosystem.

## Specs

| Spec | Source | Invariants | Tests |
|------|--------|-----------|-------|
| `circuit_breaker.qnt` | rs/ middleware | valid_state, closed_under_threshold, open_has_bounded_ticks, half_open_bounded | circuit_breaker_test.qnt |
| `rate_limiter.qnt` | rs/ rate_limit.rs | rate_enforced, fair_capacity, no_undercount, offset_bounded | rate_limiter_test.qnt |
| `csrf_lifecycle.qnt` | rs/ CSRF middleware | no_replay, session_isolation, expired_not_active | csrf_lifecycle_test.qnt |
| `session_lifecycle.qnt` | rs/ session management | terminal_permanent, renewal_bounded, active_has_ttl | session_lifecycle_test.qnt |

## How to Add a Spec

1. Write the `.qnt` file in this directory
2. Add invariants that define safety properties
3. Run `quint typecheck specs/framework/<name>.qnt`
4. Run `quint run --invariant <invariant_name> specs/framework/<name>.qnt`
5. Add tests in `tests/`
6. Add conformance tests in `conformance/` if applicable
