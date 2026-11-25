//! A serial appender: a single-threaded WAL writer (card A-6b; S-STORE
//! §12.2, "Records never span segments", the chain head and the order of
//! sealing; §12.4 in its serial form; §12.5's writer rule).
//!
//! It is the form card A-6c's concurrent writer builds on: placement, the
//! chain head, the write rule and the error latch are the same there, inside
//! an append critical section and over batches.
//!
//! # Invariants this module owns
//!
//! - **Placement** (§12.2): a record goes at the append position when it
//!   ends exactly at its segment's end or leaves room for a `SegmentEnd`
//!   after it (A-6a's `place`); otherwise a `SegmentEnd` goes there and the
//!   record at the next segment's first LSN. So a segment never ends with a
//!   gap shorter than a `SegmentEnd` (mutant SEG-TAIL-SHORT is `place`'s).
//! - **The chain** (§12.2, I-WAL-CHAIN): every record is sealed with the
//!   chain head, the start and `crc` of the record reserved just before it,
//!   `SegmentEnd` included (mutant PREV-HOP-WRONG: the record after a
//!   `SegmentEnd` names that `SegmentEnd`), and the head moves with the
//!   append position. A `SegmentEnd` carries the writing boot's id and
//!   `n = 0`. Every record carries the boot id of the boot that wrote it.
//! - **The write rule** (§12.5): before this appender's first write into a
//!   segment, [`ensure_segment`] has returned for it in this boot: the
//!   segment's creation or rename and its preallocated length are durable.
//! - **The synced report** (§12.4): `sync` syncs every segment that holds a
//!   byte in `[durable_end, written_end)`, not only those this appender
//!   wrote, and reports `durable_end`. A resumed appender starts with
//!   `durable_end` below `written_end`, since the bytes in between may be a
//!   crashed process's unsynced writes (I-BOOT-DURABLE, whose pin for the
//!   concurrent writer is card A-6c's).
//! - **The error latch** (§9.2): an `Env` error from `ensure_segment`,
//!   `write_at` or `sync_data` is stored, returned, and returned again by
//!   every later `append` and `sync`. `durable_end` never moves past it, and
//!   the appender makes no further `Env` call: a failed WAL `sync_data` is
//!   never retried as if its bytes were safe.
//! - **Memory**: at most one `SegmentEnd` and one record in bytes at a
//!   time, and the handles of the segments from `durable_end`'s to the
//!   append position's. Steps 1–4 of an `append` do no I/O and leave the
//!   appender unchanged on error.
//!
//! What is durable when a report says `durable_end = L`: every record byte
//! below `L` was written before a `sync_data` of its segment that completed,
//! and every segment holding such a byte had its creation or rename and its
//! length made durable before its first byte was written. After any power
//! cut that follows the report, a walk's valid end is at or above the end of
//! every record below `L`, or the walk reports damage that is not a stop.
//!
//! # What it does not own
//!
//! The append critical section, batches in flight, the frontiers over the
//! contiguous prefix (I-ACK-PREFIX), sealing order across batches,
//! `synchronous_commit`, group commit and `t_append_section_leaf` (card
//! A-6c); chunked records and the open-chunk set (A-6d); recycling and the
//! bound it uses (`segment::recycle_below`; A-6c, A-7c); the writer's start
//! state at open (A-7d passes the walk's valid end, its chain head and
//! `durable_end` at the walk's start); the store's poison mark, to which
//! A-7c wires this latch's error.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::store::StoreError;
use crate::wal::record::SEGMENT_END_LEN;
use crate::wal::segment::{check_seg_size, ensure_segment, segment_first, segment_path};
use crate::wal::{Body, Chain, Record, TxnId, encode, place};
use nucleus_env::{Env, EnvError, File, OpenMode};

/// The `Invalid` error of this module.
fn invalid(what: impl std::fmt::Display) -> StoreError {
    StoreError::Invalid(format!("WAL append: {what}"))
}

/// Where an appender resumes (§12.4 "After recovery"): the append position
/// and `written_end` at the valid end, the chain head from the recovery
/// (§12.2), and `durable_end` at the walk's start, the opened meta's
/// `wal_start_lsn`, never at the valid end.
///
/// A creating session starts at `Resume { append_pos: 0, prev_lsn:
/// u64::MAX, prev_crc: 0, durable_end: 0 }` (§12.2: "The creating session's
/// first record carries meta 0's, all-ones and 0").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Resume {
    /// The append position, where the first record goes.
    pub append_pos: u64,
    /// The start of the record before it (the chain head's `lsn`).
    pub prev_lsn: u64,
    /// That record's `crc` (the chain head's `crc`).
    pub prev_crc: u32,
    /// The largest LSN every record byte below which is known durable.
    pub durable_end: u64,
}

/// What one `append` did (§12.2): where the record went, and the
/// `SegmentEnd` written before it when it hopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Appended {
    /// The record's LSN.
    pub lsn: u64,
    /// The append position after it: `lsn` plus the record's length.
    pub end: u64,
    /// The record's `crc` field.
    pub crc: u32,
    /// The `SegmentEnd` written first at the old append position, its start
    /// and its `crc`; `None` when the record did not hop.
    pub segment_end: Option<(u64, u32)>,
}

/// A single-threaded WAL writer over `env` (the module doc). One thread
/// uses it at a time; it takes no lock.
pub struct Appender<'e, E: Env> {
    /// The environment every call goes through (§2).
    env: &'e E,
    /// `wal_seg_size`.
    seg_size: u64,
    /// The boot id every record it appends carries (§12.2).
    boot: u32,
    /// The append position.
    pos: u64,
    /// The chain head: the start and `crc` of the record reserved last.
    head: (u64, u32),
    /// `written_end`: equal to `pos` in a serial writer, since each write
    /// returns before the next reservation.
    written: u64,
    /// `durable_end`: what the last `sync` reported.
    durable: u64,
    /// Open handles, by first LSN, of the segments from `durable_end`'s to
    /// the append position's. Dropped as `durable_end` moves past them.
    handles: BTreeMap<u64, E::File>,
    /// The segments [`ensure_segment`] has returned for in this appender
    /// (the write rule): only these are written to.
    ensured: BTreeSet<u64>,
    /// The latched `Env` error, once one has occurred.
    failed: Option<EnvError>,
}

impl<E: Env> fmt::Debug for Appender<'_, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Appender")
            .field("seg_size", &self.seg_size)
            .field("boot", &self.boot)
            .field("pos", &self.pos)
            .field("head", &self.head)
            .field("written", &self.written)
            .field("durable", &self.durable)
            .field("handles", &self.handles.keys().collect::<Vec<_>>())
            .field("ensured", &self.ensured.iter().collect::<Vec<_>>())
            .field("failed", &self.failed)
            .finish()
    }
}

impl<'e, E: Env> Appender<'e, E> {
    /// An appender of boot `boot` over `env`, resumed at `at`. No I/O.
    ///
    /// `Invalid` unless `seg_size` is a `wal_seg_size` (§12.1),
    /// `at.durable_end ≤ at.append_pos`, and the room at `at.append_pos`
    /// (`seg_size − append_pos % seg_size`) is at least 53: a correct writer
    /// never leaves a shorter tail (§12.2). `written_end` starts at the
    /// append position and the chain head at `(prev_lsn, prev_crc)`.
    pub fn new(env: &'e E, seg_size: u64, boot: u32, at: Resume) -> Result<Self, StoreError> {
        check_seg_size(seg_size)?;
        if at.durable_end > at.append_pos {
            return Err(invalid(format!(
                "durable_end {} is above the append position {}",
                at.durable_end, at.append_pos
            )));
        }
        let offset = at
            .append_pos
            .checked_rem(seg_size)
            .ok_or_else(|| invalid(format!("wal_seg_size {seg_size}")))?;
        let room = seg_size - offset;
        if room < SEGMENT_END_LEN {
            return Err(invalid(format!(
                "the append position {} leaves a tail of {room} bytes, shorter than a SegmentEnd \
                 (S-STORE §12.2)",
                at.append_pos
            )));
        }
        Ok(Self {
            env,
            seg_size,
            boot,
            pos: at.append_pos,
            head: (at.prev_lsn, at.prev_crc),
            written: at.append_pos,
            durable: at.durable_end,
            handles: BTreeMap::new(),
            ensured: BTreeSet::new(),
            failed: None,
        })
    }

    /// The latched error, if any: every later `append` and `sync` returns it.
    fn latched(&self) -> Result<(), StoreError> {
        match &self.failed {
            Some(e) => Err(StoreError::Env(e.clone())),
            None => Ok(()),
        }
    }

    /// Latches an `Env` error: the first one is stored, and every error is
    /// returned as it was (§9.2, the error latch).
    fn latch<T>(&mut self, r: Result<T, StoreError>) -> Result<T, StoreError> {
        if let Err(StoreError::Env(e)) = &r
            && self.failed.is_none()
        {
            self.failed = Some(e.clone());
        }
        r
    }

    /// The handle a write into the segment holding `lsn` goes through: the
    /// write rule makes the segment usable first, once per appender (§12.5).
    fn writer(&mut self, lsn: u64) -> Result<&E::File, StoreError> {
        let s = segment_first(lsn, self.seg_size);
        if !self.ensured.contains(&s) {
            let file = self.latch(ensure_segment(self.env, self.seg_size, s))?;
            self.handles.insert(s, file);
            self.ensured.insert(s);
        }
        self.handles
            .get(&s)
            .ok_or_else(|| invalid(format!("segment {s} was made usable and lost its handle")))
    }

    /// Writes `bytes` at LSN `lsn`: into the segment that holds it, at
    /// `lsn − first_lsn` (§12.2: a record never spans segments).
    fn write(&mut self, lsn: u64, bytes: &[u8]) -> Result<(), StoreError> {
        let s = segment_first(lsn, self.seg_size);
        let r = self
            .writer(lsn)
            .and_then(|f| f.write_at(lsn - s, bytes).map_err(StoreError::Env));
        self.latch(r)
    }

    /// The handle `sync_data` of the segment `s` goes through: one this
    /// appender holds, or one opened `ReadWrite` for the sync (a segment an
    /// earlier boot wrote and the walk accepted, which this appender has not
    /// written to).
    fn sync_handle(&mut self, s: u64) -> Result<&E::File, StoreError> {
        if !self.handles.contains_key(&s) {
            let file = self.latch(
                self.env
                    .open(&segment_path(s), OpenMode::ReadWrite)
                    .map_err(StoreError::Env),
            )?;
            self.handles.insert(s, file);
        }
        self.handles.get(&s).ok_or_else(|| {
            invalid(format!(
                "segment {s} was opened for a sync and lost its handle"
            ))
        })
    }

    /// Appends `rec` and returns where it went (§12.2). In order:
    ///
    /// 1. a latched error is returned;
    /// 2. `Invalid` if `rec` carries another boot's id (§12.2: "Every record
    ///    carries the boot id of the boot that wrote it") or is a
    ///    `SegmentEnd` (the appender places those);
    /// 3. the record is encoded and 4. placed (A-6a's `encode` and `place`,
    ///    whose refusals are `Invalid`). Steps 1–4 do no I/O and leave the
    ///    appender unchanged on error.
    /// 5. If the record hops, a `SegmentEnd` of this boot is written at the
    ///    append position, sealed with the chain head, which moves to it.
    /// 6. The record is sealed with the chain head and written where `place`
    ///    put it; the chain head moves to it, and the append position and
    ///    `written_end` move past it.
    ///
    /// An `Env` error at step 5 or 6 is latched (the module doc).
    pub fn append(&mut self, rec: &Record<'_>) -> Result<Appended, StoreError> {
        self.latched()?;
        if rec.txn.boot != self.boot {
            return Err(invalid(format!(
                "a record of boot {} in the WAL of boot {} (S-STORE §12.2)",
                rec.txn.boot, self.boot
            )));
        }
        if matches!(rec.body, Body::SegmentEnd) {
            return Err(invalid(
                "a SegmentEnd is placed by the appender, never appended by a caller",
            ));
        }
        let unsealed = encode(rec)?;
        let len = u64::try_from(unsealed.len())
            .map_err(|_| invalid("a record longer than u64::MAX bytes"))?;
        let placed = place(self.pos, len, self.seg_size)?;
        // 5. The hop.
        let mut segment_end = None;
        if let Some(q) = placed.segment_end {
            let end = Record {
                txn: TxnId {
                    boot: self.boot,
                    n: 0,
                },
                body: Body::SegmentEnd,
            };
            let sealed = encode(&end)?.seal(Chain {
                lsn: q,
                prev_lsn: self.head.0,
                prev_crc: self.head.1,
            });
            let crc = sealed.crc();
            self.write(q, sealed.bytes())?;
            self.head = (q, crc);
            segment_end = Some((q, crc));
        }
        // 6. The record.
        let sealed = unsealed.seal(Chain {
            lsn: placed.lsn,
            prev_lsn: self.head.0,
            prev_crc: self.head.1,
        });
        let crc = sealed.crc();
        self.write(placed.lsn, sealed.bytes())?;
        self.head = (placed.lsn, crc);
        self.pos = placed
            .lsn
            .checked_add(len)
            .ok_or_else(|| invalid(format!("the end of the record at LSN {}", placed.lsn)))?;
        self.written = self.pos;
        Ok(Appended {
            lsn: placed.lsn,
            end: self.pos,
            crc,
            segment_end,
        })
    }

    /// Makes every record byte below `written_end` durable and reports
    /// `durable_end`: the writer's synced report (§12.4), which an `on` ack
    /// takes as its witness (I-ACK-PREFIX).
    ///
    /// A latched error is returned. Otherwise, if `written_end >
    /// durable_end`, every segment from `segment_first(durable_end)` to
    /// `segment_first(written_end − 1)`, ascending, is `sync_data`'d, its
    /// handle opened `ReadWrite` if this appender holds none; then
    /// `durable_end` becomes `written_end`, and the handles of the segments
    /// wholly below its segment are dropped. It syncs every segment in
    /// `[durable_end, written_end)`, not only those this appender wrote (the
    /// module doc). A failing `sync_data` is latched and leaves
    /// `durable_end` where it was.
    pub fn sync(&mut self) -> Result<u64, StoreError> {
        self.latched()?;
        if self.written > self.durable {
            let first = segment_first(self.durable, self.seg_size);
            let last = segment_first(self.written - 1, self.seg_size);
            let mut s = first;
            while s <= last {
                let r = self
                    .sync_handle(s)
                    .and_then(|f| f.sync_data().map_err(StoreError::Env));
                self.latch(r)?;
                // The next segment. The sum saturates rather than wrap:
                // `last` is the first LSN of the segment that holds the
                // last written byte, `written_end − 1`, which ends inside a
                // `u64` (A-6a's `place` puts no record in one that does
                // not), so a saturated value lies past `last` and ends the
                // loop instead of wrapping under it.
                s = s.saturating_add(self.seg_size);
            }
            self.durable = self.written;
            // The segments wholly below `durable_end`'s are never written
            // or synced again: their handles go, and so does their record
            // in `ensured`, so neither grows with the WAL. A write never
            // targets a segment below the append position's.
            let keep = segment_first(self.durable, self.seg_size);
            self.handles
                .retain(|seg, _| seg.checked_add(self.seg_size).is_none_or(|end| end > keep));
            self.ensured.retain(|seg| *seg >= keep);
        }
        Ok(self.durable)
    }

    /// The append position: where the next record goes.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// The chain head: the start and `crc` of the record appended last, or
    /// the resumed pair when nothing was appended (§12.2).
    pub fn chain_head(&self) -> (u64, u32) {
        self.head
    }

    /// `written_end` (§12.4): equal to the append position here.
    pub fn written_end(&self) -> u64 {
        self.written
    }

    /// `durable_end` (§12.4): what the last `sync` reported, or the resumed
    /// value.
    pub fn durable_end(&self) -> u64 {
        self.durable
    }
}
