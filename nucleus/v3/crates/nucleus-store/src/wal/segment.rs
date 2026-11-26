//! WAL segments: their names, segment 0, preallocation and the writer rule,
//! and recycling (card A-6b; S-STORE §2.3 step 2, §12.1, §12.5's writer rule
//! and "Short segments", §13 steps 1 and 6; I-WAL-START).
//!
//! # Invariants this module owns
//!
//! - **Names** (§12.1). A segment is
//!   `wal/<first-lsn as 16 lowercase hex digits>.log` and covers
//!   `[first_lsn, first_lsn + wal_seg_size)`. Only a name of exactly that
//!   shape, whose first LSN is a multiple of `wal_seg_size`, is a segment:
//!   anything else in `wal/` is never read, renamed or written
//!   ([`parse_segment_name`], [`list_segments`]). There is no segment
//!   header: a segment's first record starts at its first byte, and each
//!   record's own `lsn` tells a recycled segment's old content from its new
//!   (the walk's `lsn` check, §12.5).
//! - **The size floor** (§12.1): `wal_seg_size` is at least 212, four record
//!   headers, so the largest record, `wal_seg_size / 4`, holds a header.
//!   Every public function here that takes a `seg_size` refuses a smaller
//!   one, or one above `u32::MAX` (§2.1's field is a `u32`), before any I/O.
//! - **The writer rule** (§12.5): a segment is appended to only after its
//!   creation or recycling rename and its preallocated length are durable,
//!   not merely returned: the file `sync_all`'d, then `wal/`'s entry synced
//!   through `Env`, in that order, made by the writing boot itself, also for
//!   a segment that already existed ([`ensure_segment`]). A crashed process
//!   can leave a create or a rename in the page cache with no `sync_dir`
//!   behind it, and nothing a writer can read tells it so.
//! - **Segment 0 exists before any meta does** (§2.3 step 2): [`create_wal`]
//!   returns only once `wal/` and segment 0, preallocated, are durable.
//! - **Recycling** (§12.1, §13 step 6): [`recycle_below`] renames the
//!   segments wholly below a bound lowest first and makes each rename
//!   durable before the next, so while a segment is present, no higher
//!   segment has been recycled; and no target lies below the append
//!   position, at or above any present segment's name, or past `u64::MAX`
//!   (mutant RECYCLE-TARGET-LOW, S-STORE 7.17).
//!
//! # What it does not own
//!
//! The walk and its stops (§12.5; [`super::walk`]); where a record goes and
//! when it is synced (`super::append`; the concurrent writer, card A-6c);
//! which bound recycling uses and when it runs (A-7c: meta `k−1`'s
//! `wal_start_lsn`, at step 6; the bound's terms are A-6d's); the rest of
//! file creation (§2.3: page 0, image 0, meta 0; A-7c, which calls
//! [`create_wal`] as step 2 and [`ensure_segment`] for step 1's start
//! segment); whether zero-filling would make a later `sync_data` cheaper
//! (A-7e measures); the store's poison mark, to which A-7c wires an
//! `Env` error (§9.2).

use crate::store::StoreError;
use crate::wal::record::SEGMENT_END_LEN;
use nucleus_env::{Env, EnvError, File, OpenMode};

/// The directory the segments live in (§2: "`wal/` segment directory").
pub const WAL_DIR: &str = "wal";

/// The smallest `wal_seg_size` (§12.1): four record headers, so that the
/// largest record, `wal_seg_size / 4` (§12.2), holds at least a header. No
/// smaller segment holds any record; A-6a's `place` refuses it too.
pub const MIN_WAL_SEG_SIZE: u64 = 4 * SEGMENT_END_LEN;

/// The length of a segment name's hex digits: 16, a `u64` in lowercase hex.
const NAME_HEX_LEN: usize = 16;

/// The segment name's suffix.
const NAME_SUFFIX: &str = ".log";

const _: () = assert!(MIN_WAL_SEG_SIZE == 212);

/// The `Invalid` error of this module, naming what was refused.
fn invalid(what: impl std::fmt::Display) -> StoreError {
    StoreError::Invalid(format!("WAL segment: {what}"))
}

/// Whether `seg_size` is a `wal_seg_size` a segment can have (§12.1, §2.1):
/// from [`MIN_WAL_SEG_SIZE`] to `u32::MAX`. Every public function of this
/// module, of [`super::walk`] and of [`super::append`] that takes a
/// `seg_size` calls this first, before any I/O.
pub fn check_seg_size(seg_size: u64) -> Result<(), StoreError> {
    if seg_size < MIN_WAL_SEG_SIZE || seg_size > u64::from(u32::MAX) {
        return Err(invalid(format!(
            "wal_seg_size {seg_size} is not between {MIN_WAL_SEG_SIZE} and u32::MAX (S-STORE §12.1)"
        )));
    }
    Ok(())
}

/// The first LSN of the segment that holds `lsn` in segments of `seg_size`
/// bytes (§12.1: "a segment covers `[first_lsn, first_lsn + wal_seg_size)`").
/// A `seg_size` of 0 is never passed here: every caller checks it first, and
/// this stays total rather than dividing by zero.
pub fn segment_first(lsn: u64, seg_size: u64) -> u64 {
    lsn - lsn.checked_rem(seg_size).unwrap_or(0)
}

/// The path of the segment whose first LSN is `first_lsn` (§12.1:
/// "`wal/<first-lsn as 16 lowercase hex digits>.log`").
pub fn segment_path(first_lsn: u64) -> String {
    format!("{WAL_DIR}/{first_lsn:016x}{NAME_SUFFIX}")
}

/// One character of a segment name's hex digits: `[0-9a-f]` (§12.1). Uppercase
/// is not a segment's name.
fn is_lower_hex(b: u8) -> bool {
    b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
}

/// The first LSN a name in `wal/` is a segment's name of (§12.1): `Some`
/// only for exactly 16 characters of `[0-9a-f]` followed by `.log`. Anything
/// else, uppercase hex included, is not a segment: `None`.
pub fn parse_segment_name(name: &str) -> Option<u64> {
    let hex = name.strip_suffix(NAME_SUFFIX)?;
    if hex.len() != NAME_HEX_LEN || !hex.bytes().all(is_lower_hex) {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// The segments in `wal/`, by first LSN, ascending (§12.1): every name that
/// parses as a segment's and gives a multiple of `seg_size`. Other names are
/// not segments: never read, renamed or written.
pub fn list_segments<E: Env>(env: &E, seg_size: u64) -> Result<Vec<u64>, StoreError> {
    check_seg_size(seg_size)?;
    let mut segments: Vec<u64> = env
        .list_dir(WAL_DIR)?
        .iter()
        .filter_map(|name| parse_segment_name(name))
        .filter(|first| first.checked_rem(seg_size) == Some(0))
        .collect();
    segments.sort_unstable();
    Ok(segments)
}

/// Creates the WAL of a new store (§2.3 step 2; I-WAL-START): `wal/`, its
/// entry made durable, then segment 0 preallocated and durable
/// ([`ensure_segment`]), in this order.
///
/// It returns only when all of them have completed, so segment 0's creation
/// and its length are durable before any meta is written ("Segment 0 exists
/// before any meta does"). An existing `wal/` is
/// `StoreError::Env(EnvError::AlreadyExists)`: creation is for a new file.
/// Page 0 before it and image 0 and meta 0 after it are A-7c's.
pub fn create_wal<E: Env>(env: &E, seg_size: u64) -> Result<(), StoreError> {
    check_seg_size(seg_size)?;
    env.create_dir(WAL_DIR)?;
    // The directory's entry, so a power cut cannot lose `wal/` itself.
    env.sync_dir("")?;
    ensure_segment(env, seg_size, 0)?;
    Ok(())
}

/// Makes the segment whose first LSN is `first_lsn` usable by a writer: the
/// writer rule of §12.5 ("a writer appends to a segment only after its
/// creation or recycling rename (§12.1) and its preallocated length are
/// durable, not merely returned: the segment `sync_all`'d and its `wal/`
/// directory entry synced through `Env`"; §12.4: "The writer makes the
/// segment durable outside the section, before its first write there").
///
/// In order: `Invalid` unless `first_lsn` is a multiple of `seg_size` and
/// the segment ends inside a `u64` (A-6a's `place` puts no record in one
/// that does not); the segment is opened `ReadWrite`, and created
/// (`CreateNew`) only if that says `NotFound`, so an existing segment,
/// created or renamed by this boot or another, is used as it is; its length
/// is extended to `seg_size` by `set_len` (preallocation by zero-extension)
/// when it is shorter, never shortened when it is longer; then `sync_all`
/// on the file, then `sync_dir("wal")`, always, also for a segment that
/// already existed: a crashed process can leave its create or rename in the
/// page cache with no `sync_dir` behind it (§12.5 "Short segments"), so a
/// boot's writer makes any other segment durable itself before its first
/// write there.
///
/// No byte of the segment is written here. A caller writes into the segment
/// only after this has returned for it in the writing boot. It does no
/// I/O on an `Invalid` argument. Preallocation writes no zeros: whether
/// zero-filling would make a later `sync_data` cheaper is A-7e's to measure.
pub fn ensure_segment<E: Env>(
    env: &E,
    seg_size: u64,
    first_lsn: u64,
) -> Result<E::File, StoreError> {
    check_seg_size(seg_size)?;
    if first_lsn.checked_rem(seg_size) != Some(0) {
        return Err(invalid(format!(
            "first LSN {first_lsn} is not a multiple of wal_seg_size {seg_size} (S-STORE §12.1)"
        )));
    }
    if first_lsn.checked_add(seg_size).is_none() {
        return Err(invalid(format!(
            "the segment at {first_lsn} ends past u64::MAX (S-STORE §12.1)"
        )));
    }
    let path = segment_path(first_lsn);
    let file = match env.open(&path, OpenMode::ReadWrite) {
        Ok(file) => file,
        Err(EnvError::NotFound { .. }) => env.open(&path, OpenMode::CreateNew)?,
        Err(e) => return Err(e.into()),
    };
    if file.len()? < seg_size {
        file.set_len(seg_size)?;
    }
    file.sync_all()?;
    env.sync_dir(WAL_DIR)?;
    Ok(file)
}

/// Recycles the segments wholly below `bound`: each is renamed, lowest
/// first, to a new first LSN above every present segment and above the
/// append position's segment, and each rename is made durable
/// (`sync_dir("wal")`) before the next (§12.1; §13 step 6; I-WAL-START).
///
/// A segment is wholly below the bound when `first_lsn + seg_size ≤ bound`.
/// The targets are consecutive: the first is one segment above the greater
/// of the highest present segment and the segment holding `append_pos`, and
/// source `i` goes to the `i`-th of them. No target is a name any segment
/// file holds, none lies below the append position (S-STORE 7.17: a name
/// below it may be the start segment of a meta that an open selects once
/// newer metas are lost), and none is a segment the writer has reached.
/// So while a segment is present, no higher segment has been recycled.
///
/// `Invalid`, before any I/O, when `bound > append_pos` (a bound is a
/// meta's `wal_start_lsn`, never above the append position), and when a
/// target would end past `u64::MAX`.
///
/// It returns the `(source, target)` pairs, in order. It never reads,
/// writes or syncs a segment's content. On an error it returns the error:
/// every rename before the failing one is durable; the failing one may or
/// may not be in the directory, unsynced. Which bound is right (meta `k−1`'s
/// `wal_start_lsn`, one generation behind) is A-7c's; the bound's terms
/// are A-6d's.
pub fn recycle_below<E: Env>(
    env: &E,
    seg_size: u64,
    bound: u64,
    append_pos: u64,
) -> Result<Vec<(u64, u64)>, StoreError> {
    check_seg_size(seg_size)?;
    if bound > append_pos {
        return Err(invalid(format!(
            "bound {bound} is above the append position {append_pos}"
        )));
    }
    let listed = list_segments(env, seg_size)?;
    let sources: Vec<u64> = listed
        .iter()
        .copied()
        .filter(|first| first.checked_add(seg_size).is_some_and(|end| end <= bound))
        .collect();
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    // The highest name a segment file holds, or 0 when none does, and the
    // append position's segment: the first target lies above both.
    let highest = listed.last().copied().unwrap_or(0);
    let above = highest.max(segment_first(append_pos, seg_size));
    // Every target is fixed, and checked, before the first rename, so an
    // impossible name refuses the whole recycling with nothing renamed.
    let count = sources.len();
    let no_name = || {
        invalid(format!(
            "no segment name above {above} for {count} segments that ends inside a u64"
        ))
    };
    let mut target = above.checked_add(seg_size).ok_or_else(no_name)?;
    let mut plan = Vec::with_capacity(count);
    for source in sources {
        // The target must be a segment: one that ends inside a `u64`.
        if target.checked_add(seg_size).is_none() {
            return Err(no_name());
        }
        plan.push((source, target));
        target = target.checked_add(seg_size).ok_or_else(no_name)?;
    }
    for (source, target) in &plan {
        env.rename(&segment_path(*source), &segment_path(*target))?;
        // Durable before the next rename (I-WAL-START).
        env.sync_dir(WAL_DIR)?;
    }
    Ok(plan)
}
