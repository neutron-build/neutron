//! Regression corpus for the binary wire-protocol decoder robustness probe.
//!
//! These are hand-picked adversarial inputs that exercise the header/length
//! boundary and the length-prefixed sub-fields of the payload + handshake
//! decoders. Each must produce a graceful error / "need more bytes" and NEVER
//! panic. Mirrors the live fuzzer (`src/bin/probe_pgwire.rs`) but as a fixed,
//! fast, deterministic guard.
//!
//! Run: `cargo test --features "server" --test probe_pgwire_regression`
#![cfg(feature = "server")]

use nucleus::binary_wire::decoder::{DecodedFrame, Decoder};
use nucleus::binary_wire::handshake::{AuthChallenge, AuthResponse};

/// Drain a byte stream through the framing decoder, dispatching each frame's
/// payload to every payload parser. Must not panic.
fn drive_stream(bytes: &[u8]) {
    let mut dec = Decoder::new();
    dec.feed(bytes);
    let mut guard = 0usize;
    loop {
        guard += 1;
        assert!(guard < 100_000, "decoder failed to make progress (possible hang)");
        match dec.parse_frame() {
            Ok(Some(DecodedFrame { payload, .. })) => {
                let _ = Decoder::parse_query(&payload);
                let _ = Decoder::parse_prepared_stmt(&payload);
                let _ = Decoder::parse_bind(&payload);
                let _ = Decoder::parse_execute(&payload);
                let _ = Decoder::parse_command_complete(&payload);
                let _ = Decoder::parse_data_row(&payload);
                let _ = Decoder::parse_error(&payload);
                let _ = Decoder::parse_handshake(&payload);
                let _ = Decoder::parse_authentication(&payload);
                let _ = Decoder::parse_ready(&payload);
                let _ = Decoder::parse_column_metadata(&payload);
                let _ = Decoder::parse_result_end(&payload);
                let _ = Decoder::parse_begin_txn(&payload);
                let _ = Decoder::parse_commit_txn(&payload);
                let _ = Decoder::parse_rollback_txn(&payload);
                let _ = Decoder::parse_parameter_status(&payload);
                let _ = AuthChallenge::decode(&payload);
                let _ = AuthResponse::decode(&payload);
            }
            Ok(None) => break, // incomplete — graceful
            Err(_) => break,   // graceful error
        }
    }
}

#[test]
fn header_length_boundaries_dont_panic() {
    let cases: &[&[u8]] = &[
        &[],                                  // empty
        &[1],                                 // tag only
        &[1, 0, 0, 0],                        // truncated header (4/5)
        &[0, 0, 0, 0, 0],                     // invalid tag 0, len 0
        &[17, 0, 0, 0, 0],                    // invalid tag 17, len 0
        &[1, 0xFF, 0xFF, 0xFF, 0xFF],         // valid tag, length over MAX
        &[1, 0x10, 0x00, 0x00, 0x01],         // 256MB+1, over cap
        &[1, 0, 0, 0, 10, 0, 1],              // declares 10, only 2 payload bytes
        &[8, 0, 0, 0, 0],                     // handshake tag, empty payload
        &[3, 0, 0, 0, 6, 0, 0, 0, 0, 0xFF, 0xFF], // bind: param_count huge, no params
        &[16, 0, 0, 0, 3, 0xFF, 0xFE, 0x00], // parameter_status: invalid utf-8 name
        &[11, 0, 0, 0, 2, 0x00, 0x01],       // column_metadata: tiny
    ];
    for c in cases {
        drive_stream(c);
    }
}

#[test]
fn handshake_subfield_lengths_dont_panic() {
    // AuthChallenge: [challenge_id:4][nonce_len:2][nonce...]
    let auth_cases: &[&[u8]] = &[
        &[],
        &[0, 0, 0, 0, 0xFF, 0xFF],             // nonce_len=65535, no nonce
        &[0, 0, 0, 1, 0x00, 0x05, 1, 2],       // nonce_len=5, only 2 bytes
        &[0, 0, 0, 0, 0, 0],                    // nonce_len=0
    ];
    for c in auth_cases {
        let _ = AuthChallenge::decode(c);
    }
    // AuthResponse: [challenge_id:4][nonce_len:2][nonce][proof_len:2][proof]
    let resp_cases: &[&[u8]] = &[
        &[],
        &[0, 0, 0, 0, 0, 0, 0, 0],             // all-zero, empty nonce+proof
        &[0, 0, 0, 0, 0xFF, 0xFF, 0, 0],       // nonce_len huge
        &[0, 0, 0, 0, 0, 1, 0x41, 0xFF, 0xFF], // proof_len huge after 1-byte nonce
    ];
    for c in resp_cases {
        let _ = AuthResponse::decode(c);
    }
}

#[test]
fn long_random_walk_stays_graceful() {
    // small deterministic xorshift to drive a longer corpus without deps
    let mut s: u64 = 0x1234_5678_9abc_def0;
    let mut next = || { s ^= s << 13; s ^= s >> 7; s ^= s << 17; s };
    for _ in 0..5000 {
        let n = (next() % 300) as usize;
        let bytes: Vec<u8> = (0..n).map(|_| (next() & 0xFF) as u8).collect();
        drive_stream(&bytes);
    }
}
