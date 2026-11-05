//! RESP length/count headers must not reserve memory for payload that has not
//! arrived. Parsing runs before AUTH, so a ~20-byte `$536870912\r\n` or a
//! chain of `*1000000\r\n` headers used to reserve 512 MiB, or 1M elements per
//! nesting level, from any unauthenticated peer.
//!
//! A counting global allocator (this test binary only) records the largest
//! single allocation made on the test's thread while the parser runs.

#![cfg(feature = "server")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::{Cursor, ErrorKind};

use nucleus::resp::parser::read_value;
use tokio::io::BufReader;

struct Tracking;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

fn note(size: usize) {
    let _ = TRACK.try_with(|t| {
        if t.get() {
            let _ = LARGEST.try_with(|l| l.set(l.get().max(size)));
        }
    });
}

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Tracking = Tracking;

/// Parse `input` on a current-thread runtime and return the error kind plus
/// the largest single allocation made while parsing.
fn parse_and_measure(input: Vec<u8>) -> (Option<ErrorKind>, usize) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut reader = BufReader::new(Cursor::new(input));
    LARGEST.with(|l| l.set(0));
    TRACK.with(|t| t.set(true));
    let res = rt.block_on(read_value(&mut reader));
    TRACK.with(|t| t.set(false));
    (res.err().map(|e| e.kind()), LARGEST.with(|l| l.get()))
}

const LIMIT: usize = 1024 * 1024;

#[test]
fn huge_bulk_header_without_payload_does_not_reserve_it() {
    let (err, largest) = parse_and_measure(b"$536870912\r\n".to_vec());
    assert_eq!(err, Some(ErrorKind::UnexpectedEof));
    assert!(largest < LIMIT, "bulk header reserved {largest} bytes");
}

#[test]
fn huge_array_header_without_payload_does_not_reserve_it() {
    let (err, largest) = parse_and_measure(b"*1000000\r\n".to_vec());
    assert_eq!(err, Some(ErrorKind::UnexpectedEof));
    assert!(largest < LIMIT, "array header reserved {largest} bytes");
}

#[test]
fn nested_huge_array_headers_do_not_reserve_per_level() {
    let input = b"*1000000\r\n".repeat(100);
    let (err, largest) = parse_and_measure(input);
    assert_eq!(err, Some(ErrorKind::UnexpectedEof));
    assert!(
        largest < LIMIT,
        "nested array headers reserved {largest} bytes"
    );
}
