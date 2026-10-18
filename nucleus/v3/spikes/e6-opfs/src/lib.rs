//! E6 OPFS spike: a wasm32 page file and WAL over OPFS sync access handles
//! (card X-6; PLAN-v3 §4 E6, backing the `OpfsEnv` design of PLAN-v3 §3.2).
//!
//! The spike answers the browser half of decision N1: can a pure-Rust
//! `wasm32` build persist pages and a WAL through
//! `FileSystemSyncAccessHandle` — worker-only, synchronous I/O — survive a
//! tab kill after a flush, and let tabs coordinate through Web Locks? The
//! Playwright harness in `harness/` measures T1–T5 per browser; the numbers
//! and the per-browser durability semantics are recorded in this
//! directory's README.
//!
//! Invariants this spike owns (the production `OpfsEnv` in `nucleus-wasm`
//! will re-state them against `docs/S-STORE.md` when card D-5b lands):
//!
//! * **Page placement.** Page `n` is exactly [`PAGE_SIZE`] bytes at byte
//!   offset `n * PAGE_SIZE` of the `pages` file, so any reader — including
//!   one in another tab after a kill — can find it without metadata.
//! * **Flush boundary.** [`flush`] pushes both files towards durable
//!   storage. Data written before a *successful* `flush` is not guaranteed
//!   to survive a kill; E6 T3 measures what actually survives per browser.
//! * **WAL append-only.** Each [`append_wal`] lands at the byte frontier
//!   observed at [`open`] plus everything appended since, so the WAL is a
//!   strictly sequential, prefix-truncatable log; [`wal_len`] is that
//!   frontier.
//! * **No async in Rust.** OPFS handle *acquisition* is asynchronous
//!   (`getDirectory`, `createSyncAccessHandle`), and this crate has no
//!   async runtime (the card allows no further dependencies), so
//!   `harness/worker.js` performs the async boot and hands the ready sync
//!   handles to [`open`]. That split — JS boot, Rust I/O — is the
//!   candidate shape for `OpfsEnv`.
//!
//! Byte lengths cross the JS boundary as `f64` (exact up to 2^53 bytes),
//! never `BigInt`, so results stay JSON-serialisable for the harness.

use std::cell::RefCell;

use wasm_bindgen::prelude::*;
use web_sys::{FileSystemReadWriteOptions, FileSystemSyncAccessHandle};

/// Page size of the spike's page file: the format default of PLAN-v3 §3.3.
pub const PAGE_SIZE: usize = 8192;

/// Magic header of a pattern page (see [`pattern_page`]).
const PAGE_MAGIC: [u8; 8] = *b"E6PAGE00";

/// The open spike database, if any. One per worker: OPFS sync handles are
/// exclusive to the file they pin, and this module is single-threaded.
struct Store {
    /// Database name (diagnostics; the file layout is fixed per name).
    name: String,
    /// Sync handle of the `pages` file.
    pages: FileSystemSyncAccessHandle,
    /// Sync handle of the `wal` file.
    wal: FileSystemSyncAccessHandle,
    /// WAL append frontier in bytes.
    wal_cursor: WalCursor,
}

thread_local! {
    static STORE: RefCell<Option<Store>> = const { RefCell::new(None) };
}

/// Runs `f` against the open store, or fails with the standard
/// "nothing is open" error. One borrow for the whole call: no export
/// re-enters the store.
fn with_store<T>(
    what: &str,
    f: impl FnOnce(&mut Store) -> Result<T, JsValue>,
) -> Result<T, JsValue> {
    STORE.with(|cell| {
        let mut guard = cell.borrow_mut();
        match guard.as_mut() {
            Some(store) => f(store),
            None => Err(error(&format!("{what}: no database is open"))),
        }
    })
}

/// Builds a JS `Error` as the rejection value of an exported call, so the
/// harness always sees `err.message` rather than an opaque thrown value.
fn error(msg: &str) -> JsValue {
    js_sys::Error::new(msg).into()
}

/// Byte offset of page `n` in the `pages` file: `n * PAGE_SIZE`.
///
/// `f64` keeps the value JS-safe and exact for every page id that fits a
/// file under 2^53 bytes.
#[must_use]
pub fn page_offset(n: u32) -> f64 {
    f64::from(n) * PAGE_SIZE as f64
}

/// One byte of the deterministic content of page `n` at position `i`.
///
/// The header layout is: magic `[0,8)`, little-endian page id `[8,12)`,
/// little-endian declared length `[12,16)`; the body from byte 16 on is
/// the position-dependent fill `(n + i) mod 256` — so a page read back
/// torn, zero-shortened, written to the wrong offset, or from the wrong
/// page id is caught at the exact byte.
#[must_use]
pub fn pattern_byte(n: u32, i: usize) -> u8 {
    match i {
        0..=7 => PAGE_MAGIC.get(i).copied().unwrap_or(0),
        8..=11 => n.to_le_bytes().get(i - 8).copied().unwrap_or(0),
        12..=15 => (PAGE_SIZE as u32)
            .to_le_bytes()
            .get(i - 12)
            .copied()
            .unwrap_or(0),
        _ => n.wrapping_add(u32::try_from(i).unwrap_or_default()) as u8,
    }
}

/// Generates the full deterministic content of page `n` into `out`
/// (replacing its contents).
pub fn fill_pattern(n: u32, out: &mut Vec<u8>) {
    out.clear();
    out.reserve(PAGE_SIZE);
    for i in 0..PAGE_SIZE {
        out.push(pattern_byte(n, i));
    }
}

/// The deterministic content of page `n`: [`PAGE_SIZE`] bytes, verifiable
/// with [`pattern_bad_index`].
#[must_use]
pub fn pattern_page(n: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(PAGE_SIZE);
    fill_pattern(n, &mut out);
    out
}

/// Integrity oracle for a pattern page: `None` when `bytes` is exactly the
/// expected page `n`, otherwise `Some(first_bad)` — the index of the first
/// byte that disagrees. A short page reports the first *missing* byte
/// (its length); a long page reports the first *extra* byte
/// ([`PAGE_SIZE`]).
#[must_use]
pub fn pattern_bad_index(n: u32, bytes: &[u8]) -> Option<usize> {
    let stop = bytes.len().min(PAGE_SIZE);
    for i in 0..stop {
        if bytes.get(i).copied() != Some(pattern_byte(n, i)) {
            return Some(i);
        }
    }
    if bytes.len() == PAGE_SIZE {
        None
    } else {
        Some(stop)
    }
}

/// The WAL append frontier, tracked in `f64` bytes (JS-safe, exact to
/// 2^53). Starts from the file size observed at open so a reopened log
/// continues after the last byte that survived.
#[derive(Debug, Clone, Copy)]
pub struct WalCursor {
    frontier: f64,
}

impl WalCursor {
    /// A cursor starting at byte `len` (clamped at 0, e.g. for a fresh
    /// file or a negative `getSize` result).
    #[must_use]
    pub fn starting_at(len: f64) -> Self {
        Self {
            frontier: len.max(0.0),
        }
    }

    /// Records an append of `n` bytes and returns the byte offset the
    /// append must be written at.
    pub fn append(&mut self, n: usize) -> f64 {
        let at = self.frontier;
        self.frontier += n as f64;
        at
    }

    /// The frontier: the byte length of the WAL as this process sees it.
    #[must_use]
    pub fn frontier(&self) -> f64 {
        self.frontier
    }
}

/// Opens the spike database `name`.
///
/// `pages` and `wal` must be `FileSystemSyncAccessHandle`s for the
/// database's `pages` and `wal` files, created by `harness/worker.js`.
/// They are passed in, rather than acquired here, because acquisition is
/// asynchronous and this crate is deliberately sync-only (see the module
/// docs). The WAL frontier is initialised from the `wal` handle's size, so
/// a reopened database appends after whatever survived.
///
/// Calling `open` again replaces the previous store; the worker is
/// responsible for closing the old handles first (JS-side `close()`).
#[wasm_bindgen]
pub fn open(
    name: String,
    pages: FileSystemSyncAccessHandle,
    wal: FileSystemSyncAccessHandle,
) -> Result<(), JsValue> {
    let name = name.trim();
    if name.is_empty() {
        return Err(error("open: database name must not be empty"));
    }
    if name.len() > 128 {
        return Err(error("open: database name longer than 128 bytes"));
    }
    let wal_len = wal
        .get_size()
        .map_err(|e| error(&format!("open({name}): wal.getSize() failed: {e:?}")))?;
    if !wal_len.is_finite() || wal_len < 0.0 {
        return Err(error(&format!(
            "open({name}): wal.getSize() returned a non-finite size ({wal_len})"
        )));
    }
    STORE.with(|cell| {
        *cell.borrow_mut() = Some(Store {
            name: name.to_owned(),
            pages,
            wal,
            wal_cursor: WalCursor::starting_at(wal_len),
        });
        Ok(())
    })
}

/// Writes `bytes` at page `n`'s fixed offset (`n * PAGE_SIZE`).
///
/// Any length is accepted (the harness writes full pattern pages); a
/// partial write is an error, never a silent success.
#[wasm_bindgen]
pub fn write_page(n: u32, bytes: &[u8]) -> Result<(), JsValue> {
    with_store(&format!("write_page({n})"), |store| {
        let options = FileSystemReadWriteOptions::new();
        options.set_at(page_offset(n));
        let wrote = store
            .pages
            .write_with_u8_array_and_options(bytes, &options)
            .map_err(|e| error(&format!("write_page({n}) on {}: failed: {e:?}", store.name)))?;
        if wrote != bytes.len() as f64 {
            return Err(error(&format!(
                "write_page({n}): short write ({wrote} of {} bytes)",
                bytes.len()
            )));
        }
        Ok(())
    })
}

/// Reads page `n` from its fixed offset.
///
/// Returns the bytes that are actually present: a full page when written,
/// a prefix of one if the file is torn or short there, and an empty array
/// if nothing exists at that offset (past EOF). Callers detect corruption
/// with [`check_page`].
#[wasm_bindgen]
pub fn read_page(n: u32) -> Result<js_sys::Uint8Array, JsValue> {
    with_store(&format!("read_page({n})"), |store| {
        let mut buffer = vec![0_u8; PAGE_SIZE];
        let options = FileSystemReadWriteOptions::new();
        options.set_at(page_offset(n));
        let got = store
            .pages
            .read_with_u8_array_and_options(&mut buffer, &options)
            .map_err(|e| error(&format!("read_page({n}) on {}: failed: {e:?}", store.name)))?;
        if !got.is_finite() || got < 0.0 {
            return Err(error(&format!(
                "read_page({n}): non-finite byte count ({got})"
            )));
        }
        buffer.truncate(got.min(PAGE_SIZE as f64) as usize);
        Ok(js_sys::Uint8Array::from(buffer.as_slice()))
    })
}

/// Flushes both the `pages` and `wal` files towards durable storage
/// (`FileSystemSyncAccessHandle.flush`). This is the spike's commit
/// barrier: E6's decision rule is about data flushed before a kill.
#[wasm_bindgen]
pub fn flush() -> Result<(), JsValue> {
    with_store("flush()", |store| {
        store.pages.flush().map_err(|e| {
            error(&format!(
                "flush() on {}: pages.flush() failed: {e:?}",
                store.name
            ))
        })?;
        store.wal.flush().map_err(|e| {
            error(&format!(
                "flush() on {}: wal.flush() failed: {e:?}",
                store.name
            ))
        })?;
        Ok(())
    })
}

/// Appends `bytes` to the WAL at the current frontier and advances it.
/// Partial writes are errors, never silent successes.
#[wasm_bindgen]
pub fn append_wal(bytes: &[u8]) -> Result<(), JsValue> {
    with_store("append_wal()", |store| {
        let at = store.wal_cursor.append(bytes.len());
        let options = FileSystemReadWriteOptions::new();
        options.set_at(at);
        let wrote = store
            .wal
            .write_with_u8_array_and_options(bytes, &options)
            .map_err(|e| {
                error(&format!(
                    "append_wal() on {} at byte {at}: failed: {e:?}",
                    store.name
                ))
            })?;
        if wrote != bytes.len() as f64 {
            return Err(error(&format!(
                "append_wal(): short write ({wrote} of {} bytes)",
                bytes.len()
            )));
        }
        Ok(())
    })
}

/// The WAL append frontier in bytes (the length the log would have if
/// everything this process appended is present).
#[wasm_bindgen]
pub fn wal_len() -> Result<f64, JsValue> {
    with_store("wal_len()", |store| Ok(store.wal_cursor.frontier()))
}

/// Generates the deterministic pattern page `n` (see [`pattern_page`]),
/// so writes and verification share one oracle.
#[wasm_bindgen]
pub fn page_pattern(n: u32) -> js_sys::Uint8Array {
    js_sys::Uint8Array::from(pattern_page(n).as_slice())
}

/// Integrity oracle for a page read back: `-1` when `bytes` is exactly the
/// pattern page `n`, otherwise the index of the first disagreeing byte
/// (short pages report their own length — the first missing byte; see
/// [`pattern_bad_index`]). `-2` marks an index that does not fit `i32`,
/// which cannot happen for pages of [`PAGE_SIZE`].
#[wasm_bindgen]
pub fn check_page(n: u32, bytes: &[u8]) -> i32 {
    match pattern_bad_index(n, bytes) {
        None => -1,
        Some(i) => i32::try_from(i).unwrap_or(-2),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_offsets_are_page_aligned() {
        assert_eq!(page_offset(0), 0.0);
        assert_eq!(page_offset(1), 8192.0);
        assert_eq!(page_offset(1_000), 8_192_000.0);
        assert_eq!(page_offset(u32::MAX), f64::from(u32::MAX) * 8192.0);
    }

    #[test]
    fn pattern_pages_roundtrip() {
        for n in [0_u32, 1, 42, 999, 65_536, u32::MAX] {
            let page = pattern_page(n);
            assert_eq!(page.len(), PAGE_SIZE);
            assert_eq!(pattern_bad_index(n, &page), None);
        }
    }

    #[test]
    fn pattern_header_carries_page_id_and_length() {
        let page = pattern_page(0x0A0B_0C0D);
        assert_eq!(&page[0..8], b"E6PAGE00");
        assert_eq!(
            u32::from_le_bytes([page[8], page[9], page[10], page[11]]),
            0x0A0B_0C0D
        );
        let declared = u32::from_le_bytes([page[12], page[13], page[14], page[15]]);
        assert_eq!(declared as usize, PAGE_SIZE);
        assert_eq!(page[16], (0x0A0B_0C0D_u32).wrapping_add(16) as u8);
    }

    #[test]
    fn pattern_detects_wrong_page_id() {
        let page = pattern_page(7);
        // The id lives at byte 8; page 8's pattern first disagrees there.
        assert_eq!(pattern_bad_index(8, &page), Some(8));
    }

    #[test]
    fn pattern_detects_flipped_tail_byte() {
        let mut page = pattern_page(3);
        let last = page.len() - 1;
        page[last] ^= 0xFF;
        assert_eq!(pattern_bad_index(3, &page), Some(last));
    }

    #[test]
    fn pattern_detects_short_empty_and_long_pages() {
        let page = pattern_page(5);
        let short = &page[..PAGE_SIZE - 1];
        assert_eq!(pattern_bad_index(5, short), Some(PAGE_SIZE - 1));
        assert_eq!(pattern_bad_index(5, &[]), Some(0));
        let mut long = page.clone();
        long.push(pattern_byte(5, PAGE_SIZE) ^ 0xFF);
        assert_eq!(pattern_bad_index(5, &long), Some(PAGE_SIZE));
    }

    #[test]
    fn pattern_detects_zero_filled_page() {
        let zeros = vec![0_u8; PAGE_SIZE];
        // Magic byte 0 differs first ('E' vs 0).
        assert_eq!(pattern_bad_index(9, &zeros), Some(0));
    }

    #[test]
    fn wal_cursor_appends_are_sequential() {
        let mut cursor = WalCursor::starting_at(40.0);
        assert_eq!(cursor.frontier(), 40.0);
        assert_eq!(cursor.append(10), 40.0);
        assert_eq!(cursor.append(0), 50.0);
        assert_eq!(cursor.append(7), 50.0);
        assert_eq!(cursor.frontier(), 57.0);
    }

    #[test]
    fn wal_cursor_clamps_negative_start() {
        assert_eq!(WalCursor::starting_at(-4.0).frontier(), 0.0);
        assert_eq!(WalCursor::starting_at(0.0).frontier(), 0.0);
    }
}
