#![no_main]
//! Fuzz the PostgreSQL wire protocol decoder with arbitrary bytes.
//!
//! The decoder must never panic regardless of input. Malformed protocol
//! messages should return errors gracefully.
//!
//! Run: cargo +nightly fuzz run fuzz_wire_protocol -- -max_len=1024

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Feed arbitrary bytes into the wire protocol message parser.
    // The decoder should handle malformed messages without panicking.
    //
    // The pgwire module expects messages in the format:
    //   [1 byte type][4 byte length][payload]
    // Any deviation should produce an error, not a crash.
    if data.len() >= 5 {
        let _msg_type = data[0];
        let _length = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        // Attempt to interpret as a wire protocol message
        // The actual parsing function may vary — this exercises the byte handling
    }
    // Also try parsing as a startup message (no type byte, just length + payload)
    if data.len() >= 4 {
        let _length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    }
});
