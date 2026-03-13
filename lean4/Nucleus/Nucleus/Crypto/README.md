# Cryptographic Algorithm Proofs

Machine-checked proofs of cryptographic algorithm properties used across the Neutron ecosystem.

## Built Modules

| File | Source | Properties |
|------|--------|-----------|
| `Hmac.lean` | rs/crates/neutron/src/jwt.rs | HMAC model, key preparation, constant-time comparison |
| `Pkce.lean` | rs/crates/neutron-oauth/src/pkce.rs | Challenge derivation, verification roundtrip |
| `ConstantTime.lean` | rs/crates/neutron/src/jwt.rs | Timing independence, length mismatch |
| `HmacSpec.lean` | — | Output length, determinism, PKCE roundtrip, collision resistance |
| `HmacProofs.lean` | — | Key prep length, XOR block length, PRF security |

## Planned

- **SCRAM-SHA-256 (RFC 5802)** — zig/src/layer0/pgwire/auth.zig
- **ECDSA P-256** — rs/crates/neutron-webauthn/

## Approach

1. Model the algorithm in Lean 4 (manually or via Aeneas translation)
2. State properties as theorems
3. Prove using `simp`, `omega`, `decide`, and custom `Nucleus.Helpers.Tactics`
4. Mark `sorry` for proofs requiring deeper mathlib integration (bitwise lemmas, etc.)
