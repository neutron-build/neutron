#![no_main]
//! Fuzz the SQL parser with arbitrary input.
//!
//! The parser must never panic regardless of input. Invalid SQL should return
//! an Err, not crash the process.
//!
//! Run: cargo +nightly fuzz run fuzz_sql_parser -- -max_len=4096

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Only fuzz valid UTF-8 (SQL is text-based)
    if let Ok(sql) = std::str::from_utf8(data) {
        // The parser should return Ok or Err, never panic
        let _ = nucleus::sql::parse(sql);
    }
});
