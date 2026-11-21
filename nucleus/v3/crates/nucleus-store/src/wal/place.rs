//! The exact-segment-end rule (S-STORE §12.2, "Records never span
//! segments"; card A-6a, Work 6): where a record goes, given the append
//! position.
//!
//! Invariants this file owns:
//! - a placed record lies inside one segment, and after it its segment has
//!   a room of 0 or at least a `SegmentEnd`'s 53 bytes, so no segment ends
//!   with a shorter gap (§12.2; §12.5's SEG-TAIL-SHORT is a mutant of
//!   [`place`]);
//! - a record is at most `wal_seg_size / 4` ([`max_record_len`]), so a
//!   segment of fewer than 212 bytes holds none;
//! - every sum is checked, and a record is placed only in a segment whose
//!   end, its first LSN plus `wal_seg_size`, is a `u64`: a position near
//!   `u64::MAX` is `Invalid`, never an overflow.
//!
//! [`place`] is arithmetic. It neither knows nor cares whether the next
//! segment exists: a reservation that hops does nothing about that segment
//! (§12.4), and the writer makes it durable before its first write there
//! (§12.5; card A-6c).

use crate::store::StoreError;
use crate::wal::record::SEGMENT_END_LEN;

/// Where a record goes (§12.2): at `lsn`, after a `SegmentEnd` at
/// `segment_end` when the record hops to the next segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Placement {
    /// Where a `SegmentEnd` is written first, the append position, when the
    /// record does not fit in the rest of its segment; `None` when the
    /// record is placed at the append position.
    pub segment_end: Option<u64>,
    /// The record's LSN: the append position, or the next segment's first
    /// LSN after a hop. The next append position is `lsn + record_len`.
    pub lsn: u64,
}

/// The longest record a segment of `seg_size` bytes holds (§12.2: "A
/// single record is at most `wal_seg_size / 4`; larger transactions are
/// chunked").
pub fn max_record_len(seg_size: u64) -> u64 {
    seg_size / 4
}

/// Places a record of `record_len` bytes at the append position
/// `append_pos` in segments of `seg_size` bytes (§12.2, "Records never span
/// segments"; card A-6a, Work 6).
///
/// `Invalid` unless `53 ≤ record_len ≤ max_record_len(seg_size)` (so
/// `seg_size ≥ 212`). With `room` the bytes from `append_pos` to its
/// segment's end (1 to `seg_size`), `Invalid` when `room < 53`: a tail
/// shorter than a `SegmentEnd` means an earlier placement broke this rule.
/// The record goes at `append_pos` when it ends exactly at the segment's
/// end or leaves room for a `SegmentEnd` after it; otherwise a `SegmentEnd`
/// goes at `append_pos` and the record at the next segment's first LSN.
/// `Invalid` when a segment's end overflows a `u64`.
pub fn place(append_pos: u64, record_len: u64, seg_size: u64) -> Result<Placement, StoreError> {
    let invalid = |why: String| StoreError::Invalid(format!("WAL placement: {why}"));
    let max = max_record_len(seg_size);
    if record_len < SEGMENT_END_LEN || record_len > max {
        return Err(invalid(format!(
            "a record of {record_len} bytes is not between {SEGMENT_END_LEN} and \
             wal_seg_size / 4 = {max} (S-STORE §12.2)"
        )));
    }
    // `seg_size ≥ 4 × 53` here, so the remainder is defined; `checked_rem`
    // keeps that visible.
    let offset = append_pos
        .checked_rem(seg_size)
        .ok_or_else(|| invalid(format!("wal_seg_size {seg_size}")))?;
    let start = append_pos.saturating_sub(offset);
    let end = start.checked_add(seg_size).ok_or_else(|| {
        invalid(format!(
            "the segment at {start} ends past u64::MAX (append position {append_pos})"
        ))
    })?;
    let room = end.saturating_sub(append_pos);
    if room < SEGMENT_END_LEN {
        return Err(invalid(format!(
            "append position {append_pos} leaves a tail of {room} bytes, shorter than a \
             SegmentEnd (S-STORE §12.2)"
        )));
    }
    let ends_exactly = record_len == room;
    let leaves_segment_end = record_len
        .checked_add(SEGMENT_END_LEN)
        .is_some_and(|need| need <= room);
    if ends_exactly || leaves_segment_end {
        return Ok(Placement {
            segment_end: None,
            lsn: append_pos,
        });
    }
    // The hop: the record goes into the next segment, whose end must be a
    // `u64` as well.
    end.checked_add(seg_size).ok_or_else(|| {
        invalid(format!(
            "the segment at {end}, after a hop from {append_pos}, ends past u64::MAX"
        ))
    })?;
    Ok(Placement {
        segment_end: Some(append_pos),
        lsn: end,
    })
}
