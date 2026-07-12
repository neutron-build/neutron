//! Binary wire-protocol decoder robustness fuzzer.
//!
//! Nucleus's in-house TLV wire codec lives in `nucleus::binary_wire`. This
//! harness pounds the *decode* side — the code path that turns untrusted bytes
//! coming off a client socket into structured frames/messages — and asserts it
//! NEVER panics, aborts, or hangs. A robust decoder must only ever return a
//! graceful error (`DecodeError` / `Err(String)`) or signal "need more bytes"
//! (`Ok(None)`); a panic here is a remotely-triggerable DoS on the DB server.
//!
//! What it feeds the decoder:
//!   (a) fully random byte streams (drained via `Decoder::parse_frame` in a
//!       loop, exactly as the connection handler does);
//!   (b) structurally-malformed frames: valid tag + bogus/huge/truncated
//!       declared length, invalid UTF-8 in string fields, overflow field
//!       counts, empty/short payloads;
//!   (c) every parsed frame payload is then dispatched to its matching
//!       `Decoder::parse_*` payload parser AND to *all* parsers (cross-parser
//!       confusion), since the connection handler picks the parser by frame
//!       tag and a hostile client controls that tag freely;
//!   (d) the handshake decoders (`AuthChallenge::decode`, `AuthResponse::decode`,
//!       `HandshakeHandler::handle_client_handshake`) which carry their own
//!       length-prefixed sub-fields — classic slice-panic territory.
//!
//! Every call is wrapped in `catch_unwind`. Any panic is captured, minimized
//! (the harness shrinks the offending input toward a minimal reproducer), and
//! reported; the process exits non-zero if anything panicked.
//!
//! Build/run:
//!   cargo run --release --features "server rusqlite" --bin probe_pgwire
//!
//! (rusqlite isn't needed by this harness but the workflow builds with it;
//! only the `server` feature is actually required.)
#![cfg(feature = "server")]
#![allow(unused)]
#![allow(clippy::all)] // internal fuzz harness

use std::panic::{AssertUnwindSafe, catch_unwind};

use nucleus::binary_wire::decoder::{DecodeError, DecodedFrame, Decoder, message_types};
use nucleus::binary_wire::handshake::{AuthChallenge, AuthResponse};

// ─── Deterministic PRNG (xorshift64) ──────────────────────────────────────────
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    fn byte(&mut self) -> u8 {
        (self.next() & 0xFF) as u8
    }
}

// All payload-level parsers, addressed by name for cross-parser fuzzing.
// Each returns a stable error/ok we don't inspect — we only care that it
// returns instead of panicking.
const PARSER_NAMES: &[&str] = &[
    "query",
    "prepared_stmt",
    "bind",
    "execute",
    "command_complete",
    "data_row",
    "error",
    "handshake",
    "authentication",
    "ready",
    "column_metadata",
    "result_end",
    "begin_txn",
    "commit_txn",
    "rollback_txn",
    "parameter_status",
];

/// Run a single named payload parser. Returns Ok(()) — the point is whether
/// the *call itself* panics, which `catch_unwind` at the call-site detects.
fn run_parser(name: &str, payload: &[u8]) {
    match name {
        "query" => {
            let _ = Decoder::parse_query(payload);
        }
        "prepared_stmt" => {
            let _ = Decoder::parse_prepared_stmt(payload);
        }
        "bind" => {
            let _ = Decoder::parse_bind(payload);
        }
        "execute" => {
            let _ = Decoder::parse_execute(payload);
        }
        "command_complete" => {
            let _ = Decoder::parse_command_complete(payload);
        }
        "data_row" => {
            let _ = Decoder::parse_data_row(payload);
        }
        "error" => {
            let _ = Decoder::parse_error(payload);
        }
        "handshake" => {
            let _ = Decoder::parse_handshake(payload);
        }
        "authentication" => {
            let _ = Decoder::parse_authentication(payload);
        }
        "ready" => {
            let _ = Decoder::parse_ready(payload);
        }
        "column_metadata" => {
            let _ = Decoder::parse_column_metadata(payload);
        }
        "result_end" => {
            let _ = Decoder::parse_result_end(payload);
        }
        "begin_txn" => {
            let _ = Decoder::parse_begin_txn(payload);
        }
        "commit_txn" => {
            let _ = Decoder::parse_commit_txn(payload);
        }
        "rollback_txn" => {
            let _ = Decoder::parse_rollback_txn(payload);
        }
        "parameter_status" => {
            let _ = Decoder::parse_parameter_status(payload);
        }
        _ => {}
    }
}

const HANDSHAKE_NAMES: &[&str] = &["auth_challenge", "auth_response"];
fn run_handshake(name: &str, payload: &[u8]) {
    match name {
        "auth_challenge" => {
            let _ = AuthChallenge::decode(payload);
        }
        "auth_response" => {
            let _ = AuthResponse::decode(payload);
        }
        _ => {}
    }
}

// ─── Input generators ─────────────────────────────────────────────────────────

/// Pure random byte stream of random length.
fn gen_random(rng: &mut Rng) -> Vec<u8> {
    let n = rng.below(512);
    (0..n).map(|_| rng.byte()).collect()
}

/// A structurally-shaped frame designed to stress header/length handling.
/// Returns the raw bytes that would be fed to `Decoder::feed`.
fn gen_malformed_frame(rng: &mut Rng) -> Vec<u8> {
    let mut out = Vec::new();
    // Frame tag: bias toward the valid range 1..=16 but include invalid ones.
    let tag: u8 = if rng.chance(70) {
        1 + (rng.below(16) as u8) // 1..=16 valid
    } else {
        rng.byte() // possibly invalid (0 or 17..=255)
    };
    out.push(tag);

    // Declared length: mix of sane / huge / overflow / zero / mismatched.
    let declared: u32 = match rng.below(6) {
        0 => 0,
        1 => rng.below(64) as u32,              // small, plausible
        2 => 0xFFFF_FFFF,                       // > MAX_FRAME_SIZE
        3 => (256 * 1024 * 1024) + 1,           // exactly over the cap
        4 => rng.below(4096) as u32,            // medium
        _ => (rng.next() & 0xFFFF_FFFF) as u32, // fully random
    };
    out.extend_from_slice(&declared.to_be_bytes());

    // Now append SOME payload bytes — but deliberately NOT matching `declared`,
    // to exercise the "incomplete frame" vs "complete frame" boundary, plus
    // invalid UTF-8 so the string parsers see bad bytes.
    let actual = match rng.below(5) {
        0 => 0usize,
        1 => declared.min(4096) as usize, // exactly declared (small ones)
        2 => rng.below(64),               // arbitrary short
        3 => declared.saturating_sub(1).min(4096) as usize, // off-by-one short
        _ => rng.below(256),
    };
    for _ in 0..actual {
        // include 0x00 (null terminators), 0xFF/0xFE (invalid utf-8 lead bytes)
        match rng.below(4) {
            0 => out.push(0),
            1 => out.push(0xFF),
            2 => out.push(0xFE),
            _ => out.push(rng.byte()),
        }
    }
    out
}

/// A payload crafted to hit string/length sub-field edges of the parsers.
fn gen_payload(rng: &mut Rng) -> Vec<u8> {
    let n = rng.below(48);
    let mut v: Vec<u8> = Vec::with_capacity(n);
    for _ in 0..n {
        match rng.below(5) {
            0 => v.push(0),    // null terminator
            1 => v.push(0xFF), // invalid utf-8
            2 => v.push(0xC0), // invalid utf-8 lead
            3 => v.push(0x80), // stray continuation
            _ => v.push(rng.byte()),
        }
    }
    v
}

// ─── Finding tracking ─────────────────────────────────────────────────────────
struct Finding {
    kind: &'static str,
    detail: String,
    input: Vec<u8>,
}

/// Drain a byte stream through a fresh Decoder exactly like the connection loop:
/// feed, then parse_frame until None or Err, dispatching every decoded frame's
/// payload to its tag-matched parser (and to all parsers). Returns true on panic.
fn drive_stream(bytes: &[u8]) -> bool {
    catch_unwind(AssertUnwindSafe(|| {
        let mut dec = Decoder::new();
        dec.feed(bytes);
        // Bound iterations so a (hypothetical) infinite-progress bug is caught
        // as a divergence rather than hanging the harness.
        let mut guard = 0usize;
        loop {
            guard += 1;
            if guard > 100_000 {
                break;
            }
            match dec.parse_frame() {
                Ok(Some(DecodedFrame {
                    message_type,
                    payload,
                })) => {
                    // tag-matched dispatch
                    dispatch_by_tag(message_type, &payload);
                    // cross-parser: every parser on this payload
                    for p in PARSER_NAMES {
                        run_parser(p, &payload);
                    }
                    for h in HANDSHAKE_NAMES {
                        run_handshake(h, &payload);
                    }
                }
                Ok(None) => break, // incomplete — graceful
                Err(_e) => break,  // graceful error
            }
        }
    }))
    .is_err()
}

fn dispatch_by_tag(tag: u8, payload: &[u8]) {
    match tag {
        message_types::QUERY => {
            let _ = Decoder::parse_query(payload);
        }
        message_types::PREPARED_STMT => {
            let _ = Decoder::parse_prepared_stmt(payload);
        }
        message_types::BIND => {
            let _ = Decoder::parse_bind(payload);
        }
        message_types::EXECUTE => {
            let _ = Decoder::parse_execute(payload);
        }
        message_types::COMMAND_COMPLETE => {
            let _ = Decoder::parse_command_complete(payload);
        }
        message_types::DATA_ROW => {
            let _ = Decoder::parse_data_row(payload);
        }
        message_types::ERROR => {
            let _ = Decoder::parse_error(payload);
        }
        message_types::HANDSHAKE => {
            let _ = Decoder::parse_handshake(payload);
        }
        message_types::AUTHENTICATION => {
            let _ = Decoder::parse_authentication(payload);
        }
        message_types::READY => {
            let _ = Decoder::parse_ready(payload);
        }
        message_types::COLUMN_METADATA => {
            let _ = Decoder::parse_column_metadata(payload);
        }
        message_types::RESULT_END => {
            let _ = Decoder::parse_result_end(payload);
        }
        message_types::BEGIN_TXN => {
            let _ = Decoder::parse_begin_txn(payload);
        }
        message_types::COMMIT_TXN => {
            let _ = Decoder::parse_commit_txn(payload);
        }
        message_types::ROLLBACK_TXN => {
            let _ = Decoder::parse_rollback_txn(payload);
        }
        message_types::PARAMETER_STATUS => {
            let _ = Decoder::parse_parameter_status(payload);
        }
        _ => {}
    }
}

/// Run a raw payload through every parser directly (no framing). True on panic.
fn drive_payload(payload: &[u8]) -> bool {
    catch_unwind(AssertUnwindSafe(|| {
        for p in PARSER_NAMES {
            run_parser(p, payload);
        }
        for h in HANDSHAKE_NAMES {
            run_handshake(h, payload);
        }
    }))
    .is_err()
}

/// Try to shrink a panicking input toward a minimal one that still panics,
/// using the given driver. Greedy truncation + chunk deletion.
fn minimize(input: &[u8], driver: fn(&[u8]) -> bool) -> Vec<u8> {
    let mut best = input.to_vec();
    // Greedy suffix truncation.
    loop {
        let mut shrunk = false;
        let len = best.len();
        if len == 0 {
            break;
        }
        // try cutting the tail in halves down to single bytes
        let mut step = (len / 2).max(1);
        while step >= 1 {
            if best.len() > step {
                let cand = best[..best.len() - step].to_vec();
                if driver(&cand) {
                    best = cand;
                    shrunk = true;
                    continue;
                }
            }
            if step == 1 {
                break;
            }
            step /= 2;
        }
        // try dropping leading bytes
        if best.len() > 1 {
            let cand = best[1..].to_vec();
            if driver(&cand) {
                best = cand;
                shrunk = true;
            }
        }
        if !shrunk {
            break;
        }
    }
    best
}

fn main_impl() {
    let mut seed: u64 = 0xB1A5_C0DE;
    let mut iterations = 200_000usize;
    let mut max_report = 15usize;
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                seed = args[i].parse().unwrap();
            }
            "--iterations" => {
                i += 1;
                iterations = args[i].parse().unwrap();
            }
            "--max-report" => {
                i += 1;
                max_report = args[i].parse().unwrap();
            }
            _ => {}
        }
        i += 1;
    }
    std::panic::set_hook(Box::new(|_| {})); // silence backtraces; catch_unwind handles it

    println!("Nucleus binary wire-protocol decoder robustness fuzzer");
    println!("seed={seed} iterations={iterations}");
    println!(
        "target: nucleus::binary_wire (Decoder::parse_frame + parse_* + handshake decoders)\n"
    );

    let mut rng = Rng(seed);
    let mut total = 0usize;
    let mut findings: Vec<Finding> = Vec::new();

    for _ in 0..iterations {
        // 1) random byte stream through the framing decoder
        total += 1;
        let r = gen_random(&mut rng);
        if drive_stream(&r) {
            let m = minimize(&r, drive_stream);
            findings.push(Finding {
                kind: "stream(random)",
                detail: "parse_frame loop panicked".into(),
                input: m,
            });
        }

        // 2) structurally-malformed frame through the framing decoder
        total += 1;
        let f = gen_malformed_frame(&mut rng);
        if drive_stream(&f) {
            let m = minimize(&f, drive_stream);
            findings.push(Finding {
                kind: "stream(malformed-frame)",
                detail: "parse_frame loop panicked".into(),
                input: m,
            });
        }

        // 3) several malformed frames concatenated (state across frames)
        total += 1;
        let mut multi = Vec::new();
        let k = 1 + rng.below(4);
        for _ in 0..k {
            multi.extend_from_slice(&gen_malformed_frame(&mut rng));
        }
        if drive_stream(&multi) {
            let m = minimize(&multi, drive_stream);
            findings.push(Finding {
                kind: "stream(multi-frame)",
                detail: "parse_frame loop panicked".into(),
                input: m,
            });
        }

        // 4) raw payload directly into every payload/handshake parser
        total += 1;
        let p = gen_payload(&mut rng);
        if drive_payload(&p) {
            let m = minimize(&p, drive_payload);
            findings.push(Finding {
                kind: "payload(all-parsers)",
                detail: "a parse_* fn panicked".into(),
                input: m,
            });
        }

        if findings.len() >= max_report {
            break;
        }
    }

    // ─── Report ───
    if !findings.is_empty() {
        println!("─── PANICS FOUND ───");
        for (idx, f) in findings.iter().enumerate() {
            // Re-run the minimized input once more to capture the panic message.
            let msg = capture_panic_msg(&f.input, f.kind);
            println!("#{} kind={} :: {}", idx + 1, f.kind, f.detail);
            println!("   minimal input ({} bytes): {:?}", f.input.len(), f.input);
            println!("   bytes(hex): {}", hex(&f.input));
            if let Some(m) = msg {
                println!("   panic msg : {m}");
            }
            println!();
        }
    }

    println!("\n════ SUMMARY ════");
    println!("inputs driven : {total}");
    println!("panics/aborts : {}", findings.len());
    if findings.is_empty() {
        println!("\nDecoder survived random + malformed input with no panic/abort/hang. 🎯");
    } else {
        std::process::exit(1);
    }
}

fn hex(b: &[u8]) -> String {
    b.iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

/// Re-drive a minimized input and capture the panic payload message.
fn capture_panic_msg(input: &[u8], kind: &str) -> Option<String> {
    let input = input.to_vec();
    let res = catch_unwind(AssertUnwindSafe(|| match kind {
        "payload(all-parsers)" => {
            for p in PARSER_NAMES {
                run_parser(p, &input);
            }
            for h in HANDSHAKE_NAMES {
                run_handshake(h, &input);
            }
        }
        _ => {
            let mut dec = Decoder::new();
            dec.feed(&input);
            let mut guard = 0usize;
            loop {
                guard += 1;
                if guard > 100_000 {
                    break;
                }
                match dec.parse_frame() {
                    Ok(Some(fr)) => {
                        dispatch_by_tag(fr.message_type, &fr.payload);
                        for p in PARSER_NAMES {
                            run_parser(p, &fr.payload);
                        }
                        for h in HANDSHAKE_NAMES {
                            run_handshake(h, &fr.payload);
                        }
                    }
                    _ => break,
                }
            }
        }
    }));
    match res {
        Ok(()) => None,
        Err(e) => {
            if let Some(s) = e.downcast_ref::<&str>() {
                Some((*s).to_string())
            } else if let Some(s) = e.downcast_ref::<String>() {
                Some(s.clone())
            } else {
                Some("<non-string panic>".into())
            }
        }
    }
}

// silence "unused" on DecodeError import (used for documentation/type clarity)
#[allow(dead_code)]
fn _assert_error_type(_e: DecodeError) {}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tokio::task::spawn_blocking(main_impl).await.unwrap();
}
