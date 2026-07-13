# M3 — Binary Wire Protocol Test Plan

Test plan for Nucleus's native binary wire protocol (`src/binary_wire/`).
The M3 CI workflow (`.github/workflows/m3_binary_protocol_tests.yml`) verifies
this document exists and that the test module structure below is present.

## Test modules (`src/binary_wire/tests/`)

| Module | Focus | Tests |
|---|---|---|
| `binary_tests.rs` | Frame encode/decode, type codecs, round-trips | 43 |
| `cross_protocol.rs` | Binary vs pgwire result parity for the same query | 28 |
| `error_tests.rs` | Malformed frames, oversized payloads, protocol errors | 16 |
| `property_tests.rs` | Randomized encode→decode invariants | 11 |
| `isolation_tests.rs` | Per-connection state isolation | 9 |
| `concurrency_tests.rs` | Concurrent clients on the binary listener | 6 |

## Contract

1. **Round-trip fidelity** — every value type encodes and decodes without loss;
   `property_tests` fuzzes this over randomized inputs.
2. **Cross-protocol parity** — a query answered over the binary protocol
   returns the same rows/types as over pgwire (`cross_protocol`).
3. **Graceful failure** — malformed or oversized frames produce a protocol
   error, never a panic or a wedged connection (`error_tests`).
4. **Isolation** — one connection's session state never leaks into another
   (`isolation_tests`), and concurrent clients don't corrupt each other
   (`concurrency_tests`).

## Status

The binary protocol tests run under `cargo test --lib binary_wire --features
server`. Some remain `#[ignore]` pending Phase 1 activation of the listener by
default; the CI step that runs them is `continue-on-error` until then. The
codecs, cross-protocol parity, and error-handling suites are active.

## Running locally

```
cd nucleus
cargo test --lib binary_wire --features server -- --nocapture
cargo test --lib binary_wire::tests::cross_protocol --features server
```
