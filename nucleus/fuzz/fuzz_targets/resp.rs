#![no_main]
//! The RESP (Redis protocol) reader and command extractor, fed arbitrary bytes.
//!
//! `resp::parser::read_value` is a streaming reader over `AsyncBufRead`, so the
//! input is delivered through a reader that hands out the bytes in
//! fuzzer-chosen chunk sizes (byte 0 seeds the schedule) behind a small
//! `BufReader`, the way a slow or fragmented TCP stream would. Values are read
//! back to back (pipelining) until the reader errors, and every value goes
//! through `parse_command`.
//!
//! Run: cargo +nightly fuzz run fuzz_resp -- -max_len=4096

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use libfuzzer_sys::fuzz_target;
use nucleus::resp::parser::{parse_command, read_value};
use tokio::io::{AsyncRead, BufReader, ReadBuf};

struct Chunked<'a> {
    data: &'a [u8],
    seed: u32,
}

impl AsyncRead for Chunked<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // xorshift: chunk sizes 1..=16 bytes.
        let mut s = self.seed;
        s ^= s << 13;
        s ^= s >> 17;
        s ^= s << 5;
        self.seed = s;
        let n = (1 + (s % 16) as usize)
            .min(self.data.len())
            .min(buf.remaining());
        let (head, tail) = self.data.split_at(n);
        buf.put_slice(head);
        self.data = tail;
        Poll::Ready(Ok(()))
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&seed, rest)) = data.split_first() else {
        return;
    };
    let reader = Chunked {
        data: rest,
        seed: u32::from(seed) | 0x100,
    };
    let mut reader = BufReader::with_capacity(8 + (seed as usize % 64), reader);
    futures::executor::block_on(async {
        for _ in 0..256 {
            match read_value(&mut reader).await {
                Ok(v) => {
                    let _ = parse_command(v);
                }
                Err(_) => break,
            }
        }
    });
});
