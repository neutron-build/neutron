//! The walk: the reader of the WAL from a start LSN to the place it ends
//! (card A-6b; S-STORE §12.5, "Stale and torn records" (I-WAL-STALE),
//! "Splices" (I-WAL-CHAIN), "The start segment" (I-WAL-START), "Short
//! segments", "Read errors" and "Malformed records"; §14 step 3, pass 1).
//!
//! # Invariants this module owns
//!
//! - **Every stop check comes first, each gives the same valid end, the
//!   record's start** (§12.5): a bad magic, a bad `len` (below a header or
//!   running past the segment), a bad CRC, an `lsn` other than the position
//!   expected, which catches a recycled segment's old content, or a boot id
//!   older, in serial order, than the last accepted record's, or than the
//!   start's for the first record. The order is fixed, so the reason
//!   reported is determined. A walk does not stop at a newer boot id.
//! - **The chain is checked last**, after every stop check, and on both
//!   fields (I-WAL-CHAIN): a record that passes every stop check but names
//!   another predecessor, by start or by `crc`, is a splice, never a stop
//!   and never a valid end. Across a hop the predecessor is the `SegmentEnd`
//!   or the record that ended exactly at the previous segment's end.
//! - **The hops** (§12.5): after an accepted `SegmentEnd`, or after an
//!   accepted record that ends exactly at its segment's end, the walk moves
//!   to the next segment's first LSN, and the valid end is then at least
//!   that LSN. The next segment's first record is checked like any other.
//! - **What ends the walk cleanly, and what is damage.** Only a hop's next
//!   segment, missing or too short for a record header at its first LSN,
//!   ends the walk there as a stop (§12.5 "Short segments": nothing was
//!   written into it before it was durable). A missing start segment
//!   ([`WalkEnd::StartSegmentMissing`](WalkEnd)), every other read past a
//!   segment file's end-of-file ([`WalkEnd::ShortRead`](WalkEnd); S-STORE
//!   7.17, card S-ETA: no correct build shortens a segment file), a splice
//!   and a record that does not decode are detected damage: never a valid
//!   end. A read error, or an open of a segment that fails in any other way
//!   than the file's absence, is neither: the walk returns the error and
//!   claims no end (§12.5 "Read errors").
//! - **The valid end is a position, never an end-of-file**: a record's
//!   start, or, after a hop, the next segment's first LSN.
//! - **It writes nothing.** Every handle it opens is `OpenMode::Read`, and
//!   it calls no `Env` method but `open` and `read_at`, so it runs unchanged
//!   in a read-only open (§14.1) and leaves every file byte-identical
//!   whatever it finds. It takes no lock and starts nothing.
//! - **Its memory does not grow with the WAL**: one buffer, reused per
//!   record and never longer than `wal_seg_size`, and one segment handle.
//!   It keeps nothing per record: what a caller keeps, it keeps in its
//!   visitor.
//! - Every sum on a position is checked (card A-6a's follow-up A-6a-F1 (b)):
//!   a position that would pass `u64::MAX` is refused with `Invalid`,
//!   never wrapped, as `place` puts no record in a segment that does not
//!   end inside a `u64`.
//!
//! # What it does not own
//!
//! What each end means at open (§14 step 3: XX001 and salvage, step 3a's
//! coverage and splice cut, step 3b, pass 2), step 5's truncation, the
//! writer's start state, the accept hook (A-8a), and pass 1's sets: the
//! visitor is where A-7d collects and charges them (all card A-7d).

use crate::codec::Reader;
use crate::meta::{Meta, boot_id_older};
use crate::store::StoreError;
use crate::wal::record::{Body, CRC_AT, LEN_AT, RECORD_HEADER_LEN, RECORD_MAGIC};
use crate::wal::segment::{check_seg_size, segment_first, segment_path};
use crate::wal::{Frame, FrameError, Record, decode_body, read_frame};
use nucleus_env::{Env, EnvError, File, OpenMode};

/// Where a walk starts (§14 step 3): the opened meta's `wal_start_lsn`, the
/// pair its first record names (§12.2: `wal_start_prev` and
/// `wal_start_prev_crc`), and the meta's `boot_id`, the floor the first
/// record's boot id is checked against (§12.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalkStart {
    /// The LSN the walk reads from: a record start of the meta's own boot,
    /// or its append position (§12.6).
    pub lsn: u64,
    /// The start of the record before it; all-ones if none (§12.2).
    pub prev_lsn: u64,
    /// That record's `crc`; 0 if none (§12.2).
    pub prev_crc: u32,
    /// The opened meta's `boot_id` (§2.2).
    pub boot_id: u32,
}

impl WalkStart {
    /// The start a walk from meta `m` takes: its `wal_start_lsn`,
    /// `wal_start_prev`, `wal_start_prev_crc` and `boot_id` (§2.2, §14
    /// step 3).
    pub fn from_meta(m: &Meta) -> Self {
        Self {
            lsn: m.wal_start_lsn,
            prev_lsn: m.wal_start_prev,
            prev_crc: m.wal_start_prev_crc,
            boot_id: m.boot_id,
        }
    }
}

/// One record the walk accepted, as it lies in the walk's buffer: the frame
/// with every header field, and the decoded body. The borrow ends when the
/// visitor returns; a caller that keeps something copies it there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accepted<'a> {
    /// The record's frame (§12.2), every check passed.
    pub frame: Frame<'a>,
    /// Its decoded kind and ops (§12.3).
    pub record: Record<'a>,
}

/// Why a walk stopped at a valid end (§12.5's stop list, and the two ways a
/// hop's next segment ends it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StopReason {
    /// The first four bytes are not `NWAL` (§12.5: "a bad magic").
    Magic,
    /// No record fits in the rest of the segment, fewer than 53 bytes, or
    /// `len` is below a header or runs past the segment (§12.5).
    Length,
    /// `crc` is not the CRC32C of the bytes it covers (§12.5: "a bad CRC").
    Crc,
    /// The record's `lsn` is not the position it lies at (§12.5: "which
    /// catches a recycled segment's old content").
    Lsn {
        /// The `lsn` the record carries.
        found: u64,
    },
    /// The record's boot id is older, in serial order, than the floor: the
    /// last accepted record's, or the start's (§12.5).
    OlderBoot {
        /// The boot id the record carries.
        found: u32,
        /// The boot id it was found older than.
        floor: u32,
    },
    /// A segment the walk entered by a hop holds fewer bytes than a record
    /// header at its first LSN (§12.5 "Short segments"): a clean stop.
    ShortSegment,
    /// A segment the walk entered by a hop is missing (§12.5 "Short
    /// segments"): a clean stop.
    MissingSegment,
}

/// How a walk ended (§12.5, §14 step 3). Only [`WalkEnd::Stop`] is a valid
/// end: every other variant is detected damage, and a read error is no end
/// at all (it is the `Err` of [`walk`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalkEnd {
    /// The walk stopped at a valid end (§12.5: "The LSN at which the walk
    /// stopped"). `segment` is the segment `valid_end` lies in, which for
    /// [`StopReason::ShortSegment`] and [`StopReason::MissingSegment`] is
    /// the one the hop entered.
    Stop {
        /// The valid end: a record's start, or, after a hop, the next
        /// segment's first LSN.
        valid_end: u64,
        /// The first LSN of the segment the stop is in.
        segment: u64,
        /// Which stop check ended the walk.
        reason: StopReason,
    },
    /// A record that passed every stop check but does not name its
    /// predecessor (I-WAL-CHAIN): detected damage at `lsn`, never a valid
    /// end. The `prev_*` fields are the record's; the `want_*` fields, what
    /// it should have named.
    Splice {
        /// The splice's position.
        lsn: u64,
        /// The `prev_lsn` it carries.
        prev_lsn: u64,
        /// The `prev_crc` it carries.
        prev_crc: u32,
        /// The start of the record the walk accepted just before it.
        want_prev_lsn: u64,
        /// The `crc` of that record.
        want_prev_crc: u32,
    },
    /// A record that passed every stop check and the chain check but does
    /// not decode (§12.5 "Malformed records"; card A-6a's open point 5):
    /// detected damage at `lsn`, never a valid end and never a stop. Only a
    /// faulty writer, or a CRC collision, makes one.
    Malformed {
        /// The malformed record's start.
        lsn: u64,
        /// What did not decode.
        detail: String,
    },
    /// The segment the walk was to start in is missing (I-WAL-START; §14
    /// step 3): detected damage, never a valid end, since it existed
    /// durably before the meta was written.
    StartSegmentMissing {
        /// The walk's start LSN, `wal_start_lsn`.
        wal_start_lsn: u64,
    },
    /// A read past a segment file's end-of-file anywhere but at the first
    /// LSN of a segment the walk entered by a hop (§12.5 "Short segments";
    /// S-STORE 7.17, card S-ETA): detected damage, never a valid end and
    /// never a stop. `lsn` is the position the walk had reached, the start
    /// of the record whose header or body runs past the end-of-file;
    /// `file_len` is the segment file's length, which the short read gives.
    /// A start segment, a segment after a record the walk accepted in it,
    /// and a record's body are damage, since no correct build shortens a
    /// segment file: such a file has lost bytes a completed sync covered.
    ShortRead {
        /// The position the walk needed a byte at.
        lsn: u64,
        /// The first LSN of the segment whose file is short.
        segment: u64,
        /// That file's length.
        file_len: u64,
    },
}

/// The last record the walk accepted: its start, its `crc` (the chain
/// head's pair, §12.2) and its boot id (the boot-id floor).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Last {
    /// The record's start.
    pub lsn: u64,
    /// Its `crc` field.
    pub crc: u32,
    /// Its boot id.
    pub boot: u32,
}

/// What a walk found (§12.5, §14 step 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Walk {
    /// How it ended.
    pub end: WalkEnd,
    /// How many records it accepted: the visitor saw exactly these.
    pub accepted: u64,
    /// The last accepted record, or `None` when it accepted none.
    pub last: Option<Last>,
    /// The newest boot id among the accepted records, in serial order, or
    /// `None` when it accepted none (a walk may cross into newer boots).
    pub newest_boot: Option<u32>,
}

impl Walk {
    /// The valid end, for a [`WalkEnd::Stop`]; `None` for every other end,
    /// which is damage and never a valid end.
    pub fn valid_end(&self) -> Option<u64> {
        match &self.end {
            WalkEnd::Stop { valid_end, .. } => Some(*valid_end),
            _ => None,
        }
    }

    /// The chain head a boot appends from (§12.2: "the last record its
    /// recovery walk accepted, or, if that walk accepted none, the opened
    /// meta's `wal_start_prev` and `wal_start_prev_crc`"): `last`'s start and
    /// `crc`, or the start's pair when the walk accepted nothing.
    pub fn chain_head(&self, start: &WalkStart) -> (u64, u32) {
        match self.last {
            Some(last) => (last.lsn, last.crc),
            None => (start.prev_lsn, start.prev_crc),
        }
    }
}

/// The `Invalid` error of a position that would pass `u64::MAX` (card
/// A-6a's follow-up A-6a-F1 (b)): refused, never wrapped.
fn past_u64(what: &str, at: u64) -> StoreError {
    StoreError::Invalid(format!(
        "WAL walk: {what} after LSN {at} would pass u64::MAX (S-STORE §12.1)"
    ))
}

/// The record's `len` field (§12.2), read at A-6a's offset: the framing is
/// defined in one place (card A-6a's follow-up A-6a-F1 (c)).
fn header_len(buf: &[u8]) -> Result<u32, StoreError> {
    let field = buf.get(LEN_AT..CRC_AT).ok_or_else(|| {
        StoreError::Invalid("WAL walk: a record header shorter than 8 bytes".into())
    })?;
    Reader::new(field)
        .u32()
        .map_err(|e| StoreError::Invalid(format!("WAL walk: {e}")))
}

/// The length a short read shows a segment file to have: where the read
/// started, plus the bytes it returned (S-STORE §12.5 "Short segments": the
/// damage detail names "its file's length"). The sum saturates, since no
/// file is longer than `u64::MAX`; the value is reported, never read through.
fn file_len(at: u64, got: usize) -> u64 {
    at.saturating_add(u64::try_from(got).unwrap_or(u64::MAX))
}

/// Walks the WAL from `start` (§12.5; §14 step 3's pass 1), calling `visit`
/// once for each record it accepts, in LSN order, and stopping at the first
/// record it does not accept.
///
/// The procedure, in this order (each step gives its reason before the next
/// runs, so the reason reported is determined):
///
/// 1. `Invalid` unless `seg_size` is a `wal_seg_size` (§12.1), before any
///    I/O. The start segment is opened read-only: its absence ends the walk
///    at once as [`WalkEnd::StartSegmentMissing`] (detected damage, §14 step
///    3, never a valid end); any other error is returned.
/// 2. At the position `p`, in the segment `s` whose end is `seg_end`: with
///    `room = seg_end − p`, no record fits when `room < 53`: a stop, `Length`.
/// 3. 53 bytes are read at `p − s`. A read error is returned. A short read
///    is a stop, `ShortSegment`, only when `p` is the first LSN of a segment
///    the walk entered by a hop; anywhere else it is
///    [`WalkEnd::ShortRead`]: damage.
/// 4. A magic other than `NWAL`: a stop, `Magic`.
/// 5. A `len` below 53 or above `room`: a stop, `Length`.
/// 6. The other `len − 53` bytes are read. A read error is returned. A short
///    read is damage, [`WalkEnd::ShortRead`], wherever it lies.
/// 7. A CRC other than the CRC32C of bytes 12..`len`: a stop, `Crc`.
/// 8. An `lsn` other than `p`: a stop, `Lsn`.
/// 9. A boot id older than the floor, in serial order: a stop, `OlderBoot`.
///    A newer or equal id passes.
/// 10. A `prev_lsn` or `prev_crc` other than the predecessor's: a splice,
///     after every stop check and never a stop (I-WAL-CHAIN).
/// 11. A body that does not decode: [`WalkEnd::Malformed`], never a stop.
/// 12. The record is accepted: `visit` is called (its error is returned at
///     once), it becomes the predecessor and the floor, and `p` moves past
///     it. Then, if it was a `SegmentEnd` or ends exactly at `seg_end`, the
///     walk hops to `seg_end` and opens that segment read-only: its absence
///     is a stop, `MissingSegment`, at `seg_end`; any other error is
///     returned. Otherwise it goes on in the same segment.
///
/// It writes nothing and takes no lock: see the module doc.
pub fn walk<E: Env>(
    env: &E,
    seg_size: u64,
    start: &WalkStart,
    visit: &mut dyn FnMut(&Accepted<'_>) -> Result<(), StoreError>,
) -> Result<Walk, StoreError> {
    check_seg_size(seg_size)?;
    fn finished(end: WalkEnd, accepted: u64, last: Option<Last>, newest: Option<u32>) -> Walk {
        Walk {
            end,
            accepted,
            last,
            newest_boot: newest,
        }
    }
    /// A walk that stopped at a valid end.
    fn stop(
        p: u64,
        s: u64,
        reason: StopReason,
        accepted: u64,
        last: Option<Last>,
        newest: Option<u32>,
    ) -> Walk {
        finished(
            WalkEnd::Stop {
                valid_end: p,
                segment: s,
                reason,
            },
            accepted,
            last,
            newest,
        )
    }
    let mut p = start.lsn;
    let mut want = (start.prev_lsn, start.prev_crc);
    let mut floor = start.boot_id;
    let mut last = None;
    let mut newest = None;
    let mut accepted = 0u64;
    let mut s = segment_first(p, seg_size);
    let mut seg_end = s
        .checked_add(seg_size)
        .ok_or_else(|| past_u64("the end of the start segment", s))?;
    let mut file: E::File = match env.open(&segment_path(s), OpenMode::Read) {
        Ok(file) => file,
        Err(EnvError::NotFound { .. }) => {
            return Ok(finished(
                WalkEnd::StartSegmentMissing {
                    wal_start_lsn: start.lsn,
                },
                0,
                None,
                None,
            ));
        }
        Err(e) => return Err(e.into()),
    };
    // Whether the current segment was entered by a hop. The start segment
    // was not: a short read there is damage (§12.5 "Short segments").
    let mut hopped = false;
    // One buffer for every record, reused; never longer than `seg_size`.
    let mut buf: Vec<u8> = Vec::new();
    let header_len_bytes = RECORD_HEADER_LEN as u64;
    loop {
        // 2. Does a record fit?
        let room = seg_end - p;
        if room < header_len_bytes {
            return Ok(stop(p, s, StopReason::Length, accepted, last, newest));
        }
        let off = p - s;
        // 3. The header.
        buf.resize(RECORD_HEADER_LEN, 0);
        let got = file.read_at(off, &mut buf)?;
        if got < RECORD_HEADER_LEN {
            // A stop only at the first LSN of a segment the walk entered by
            // a hop (§12.5 "Short segments"); anywhere else, damage.
            let end = if hopped && off == 0 {
                WalkEnd::Stop {
                    valid_end: p,
                    segment: s,
                    reason: StopReason::ShortSegment,
                }
            } else {
                WalkEnd::ShortRead {
                    lsn: p,
                    segment: s,
                    file_len: file_len(off, got),
                }
            };
            return Ok(finished(end, accepted, last, newest));
        }
        // 4. The magic.
        let magic: Option<[u8; RECORD_MAGIC.len()]> = buf.first_chunk().copied();
        if magic != Some(RECORD_MAGIC) {
            return Ok(stop(p, s, StopReason::Magic, accepted, last, newest));
        }
        // 5. `len`.
        let len = u64::from(header_len(&buf)?);
        if len < header_len_bytes || len > room {
            return Ok(stop(p, s, StopReason::Length, accepted, last, newest));
        }
        // 6. The body, into the same buffer.
        // `len ≤ seg_size ≤ u32::MAX`, so it is a `usize` on every target;
        // the conversion stays checked.
        let len_usize = usize::try_from(len).map_err(|_| past_u64("the end of a record", p))?;
        buf.resize(len_usize, 0);
        let body = buf.get_mut(RECORD_HEADER_LEN..).ok_or_else(|| {
            StoreError::Invalid("WAL walk: a record shorter than its header".into())
        })?;
        let want_body = body.len();
        // `off + 53` lies inside the segment, since `room ≥ 53` (step 2);
        // the sum stays checked like every one on a position.
        let body_at = off
            .checked_add(header_len_bytes)
            .ok_or_else(|| past_u64("the body of the record", p))?;
        let got = file.read_at(body_at, body)?;
        if got < want_body {
            // Damage wherever it lies: a segment whose preallocation was
            // cut short holds zeros, never a header with a valid magic.
            return Ok(finished(
                WalkEnd::ShortRead {
                    lsn: p,
                    segment: s,
                    file_len: file_len(body_at, got),
                },
                accepted,
                last,
                newest,
            ));
        }
        // 7. The CRC, and the frame.
        let frame = match read_frame(buf.as_slice()) {
            Ok(frame) => frame,
            // `Magic` and `Length` cannot occur after steps 4 and 5; if one
            // does, it is that stop.
            Err(FrameError::Crc) => return Ok(stop(p, s, StopReason::Crc, accepted, last, newest)),
            Err(FrameError::Magic) => {
                return Ok(stop(p, s, StopReason::Magic, accepted, last, newest));
            }
            Err(FrameError::Length) => {
                return Ok(stop(p, s, StopReason::Length, accepted, last, newest));
            }
        };
        // 8. The position.
        if frame.lsn != p {
            return Ok(stop(
                p,
                s,
                StopReason::Lsn { found: frame.lsn },
                accepted,
                last,
                newest,
            ));
        }
        // 9. The boot id, in serial order.
        let boot = frame.txn.boot;
        if boot_id_older(boot, floor) {
            return Ok(stop(
                p,
                s,
                StopReason::OlderBoot { found: boot, floor },
                accepted,
                last,
                newest,
            ));
        }
        // 10. The chain: last, and never a stop (I-WAL-CHAIN).
        if (frame.prev_lsn, frame.prev_crc) != want {
            return Ok(finished(
                WalkEnd::Splice {
                    lsn: p,
                    prev_lsn: frame.prev_lsn,
                    prev_crc: frame.prev_crc,
                    want_prev_lsn: want.0,
                    want_prev_crc: want.1,
                },
                accepted,
                last,
                newest,
            ));
        }
        // 11. The body.
        let record = match decode_body(&frame) {
            Ok(record) => record,
            Err(StoreError::Corrupt(detail)) => {
                return Ok(finished(
                    WalkEnd::Malformed { lsn: p, detail },
                    accepted,
                    last,
                    newest,
                ));
            }
            Err(e) => return Err(e),
        };
        // 12. Accept. `Accepted` takes the frame and the record by value, so
        // what the walk keeps of them is copied first.
        let crc = frame.crc;
        let is_segment_end = matches!(record.body, Body::SegmentEnd);
        visit(&Accepted { frame, record })?;
        accepted += 1;
        last = Some(Last { lsn: p, crc, boot });
        newest = match newest {
            Some(n) if boot_id_older(boot, n) => newest,
            _ => Some(boot),
        };
        floor = boot;
        want = (p, crc);
        p = p
            .checked_add(len)
            .ok_or_else(|| past_u64("the end of a record", p))?;
        if is_segment_end || p == seg_end {
            // The hop: the next segment's first LSN, and one segment
            // handle at a time.
            drop(file);
            s = seg_end;
            p = seg_end;
            file = match env.open(&segment_path(s), OpenMode::Read) {
                Ok(file) => file,
                Err(EnvError::NotFound { .. }) => {
                    return Ok(stop(
                        p,
                        s,
                        StopReason::MissingSegment,
                        accepted,
                        last,
                        newest,
                    ));
                }
                Err(e) => return Err(e.into()),
            };
            seg_end = s
                .checked_add(seg_size)
                .ok_or_else(|| past_u64("the end of a segment", s))?;
            hopped = true;
        }
    }
}
