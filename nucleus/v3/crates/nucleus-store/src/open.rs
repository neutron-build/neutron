//! The open rule (card A-2-0, Work 4): S-STORE §2.2's "At open (the open
//! rule)" as a pure function of the five header blocks' bytes, "so that
//! every combination is tested with no I/O" (§2.2; `t_meta_copies`' table
//! form, §15.1).
//!
//! # What the caller does (§14 step 1; card A-7d)
//!
//! The open reads page 0 (the file's first 4096 bytes, §2.1) and the two
//! replica blocks (§2.4), calls [`locate_page_size`], reads slots 1 and 2
//! at that page size when it returned one, and calls [`select_meta`]; a
//! place whose read failed is [`Block::Unreadable`]. Then it fails with
//! one of its own read errors on [`OpenDecision::AllUnreadable`] (58030)
//! and with XX001 on [`OpenDecision::Corrupt`]; a read-write open zeroes
//! every place whose read failed before its first write (the fence); the
//! open runs the range check on the selected meta (`T ≤ ts_ckpt_hi` and
//! `wal_start_lsn ≤ wal_ckpt_end`, else XX001 with no fallback); and it
//! falls back to [`Selection::previous`] only as §14 step 4a allows. None
//! of that is here.
//!
//! # Invariants this module owns
//!
//! - **I-META-COPIES, the selection** (§2.2, §15): each parity's candidate
//!   by §2.2's table and the tie rule, foreign copies ignored, misplaced
//!   copies never opened, and no need of page 0 while a replica copy gives
//!   the identity.
//! - **I-META-SLOT** (§15): the selected meta is a valid copy in a place of
//!   its own parity, and a misplaced copy two or more `ckpt_id`s above it
//!   fails the open.
//! - **"Nothing depends on which copy is read first"** (§2.2): the result
//!   is a function of the five blocks' bytes, all seen at once.
//!
//! A copy is a block that decodes ([`decode_meta`]); a malformed block is
//! invalid everywhere, the identity step included (card A-2-0, Work 3 and
//! Work 4 step 2).
//!
//! # Two corners, as card A-2-0 pins them (its open points 2 and 3)
//!
//! - Page 0 invalid and every decoding copy foreign to one shared identity:
//!   the bytes cannot tell those copies from the file's own (§2.2: "with
//!   page 0 invalid a copy of another file's identity cannot be told
//!   apart"), so that identity is the file's, and the newest such copy
//!   opens.
//! - Page 0 invalid and both replica reads failed: slots 1 and 2 cannot be
//!   located, so the open fails with XX001 for want of an identity, not
//!   with 58030, whose "every place's read fails" needs all four places
//!   read.

use crate::meta::{Identity, Meta, boot_id_older, decode_meta, decode_page0, meta_parity};

/// One header place's bytes as read, or the failure of its read (§2.2: "A
/// place whose read fails holds an invalid copy").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Block<'a> {
    /// The bytes read: page 0's first 4096 bytes, a replica block or a
    /// slot. Only the record at their start is examined.
    Read(&'a [u8]),
    /// The read failed.
    Unreadable,
}

/// The five header blocks an open reads (§2.2, §14 step 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderBlocks<'a> {
    /// Page 0: the file's first 4096 bytes (§2.1).
    pub page0: Block<'a>,
    /// `replicas[q]`: parity `q`'s replica block, at
    /// `meta::REPLICA_OFFSETS[q]` (§2.4).
    pub replicas: [Block<'a>; 2],
    /// `slots[q]`: slot `1 + q`, parity `q`'s primary place. `None` exactly
    /// when [`locate_page_size`] returned `None`, so the caller could not
    /// tell where slots 1 and 2 are.
    pub slots: Option<[Block<'a>; 2]>,
}

/// What the open rule decides (§2.2).
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
// One value per open, returned once: boxing `Selection` would add an
// allocation and buy nothing (card A-2-0 Work 4 fixes this shape).
#[allow(clippy::large_enum_variant)]
pub enum OpenDecision {
    /// Open the newest meta, with the previous meta if there is one.
    Open(Selection),
    /// Every place's read failed: 58030. The caller returns one of its own
    /// read errors (S-STORE's error table).
    AllUnreadable,
    /// XX001, for the reason given.
    Corrupt(OpenCorrupt),
}

/// The metas the open rule selects (§2.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    /// The file's identity: page 0's, or the one the copies share.
    pub identity: Identity,
    /// The newest meta `k`: "the candidate with the higher `ckpt_id`".
    pub newest: Meta,
    /// The previous meta: "the other parity's candidate when its `ckpt_id`
    /// is `k−1`". Whether its image is a usable previous image (its free
    /// set reads) is the open's to find (§14 step 2, card A-7d).
    pub previous: Option<Meta>,
}

/// Why the open rule fails the open with XX001 (§2.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenCorrupt {
    /// Page 0 is invalid and neither replica block holds a copy: "if
    /// neither holds a copy with valid magic and CRC, the open fails with
    /// XX001".
    NoIdentity,
    /// Page 0 is invalid and two copies differ in a static field: "if two of
    /// them differ in `format_version`, `page_size`, `checksum_alg`,
    /// `wal_seg_size` or `creation_uuid`, the open fails with XX001".
    IdentityConflict,
    /// Both places of one parity hold valid copies with one `ckpt_id` that
    /// differ in a byte other than `copy` and `crc`, and neither `boot_id`
    /// is newer: "equal `boot_id`s fail the open with XX001" (the tie rule).
    EqualBootIds {
        /// The parity whose two copies tie.
        parity: u8,
    },
    /// Neither parity has a candidate: "If there is none, the open fails
    /// with XX001".
    NoCandidate,
    /// A misplaced copy lies two or more `ckpt_id`s above the newest meta:
    /// "If a misplaced copy's `ckpt_id` exceeds `k + 1`, the open fails with
    /// XX001 (I-META-SLOT)".
    MisplacedAhead {
        /// The misplaced copy's `ckpt_id` (the highest, if several are).
        ckpt_id: u64,
        /// The newest meta's `ckpt_id`, `k`.
        newest: u64,
    },
}

/// The page size that locates slots 1 and 2 (§2.2): page 0's when page 0
/// is read and decodes; otherwise the one every replica block that is read
/// and decodes shares ("the replica copies give the page size, and so the
/// offsets of slots 1 and 2"); `None` when no replica copy decodes or two
/// disagree.
pub fn locate_page_size(page0: &Block<'_>, replicas: &[Block<'_>; 2]) -> Option<u32> {
    if let Some(id) = page0_identity(page0) {
        return Some(id.page_size);
    }
    let mut sizes = replicas
        .iter()
        .filter_map(copy_in)
        .map(|m| m.identity.page_size);
    let first = sizes.next()?;
    if sizes.all(|ps| ps == first) {
        Some(first)
    } else {
        None
    }
}

/// §2.2's open rule over the five header blocks (card A-2-0, Work 4's nine
/// steps, each citing its bullet of §2.2).
pub fn select_meta(blocks: &HeaderBlocks<'_>) -> OpenDecision {
    // Step 1, 58030. "A place whose read fails holds an invalid copy; if
    // every place's read fails, the open fails with 58030." Every place's
    // read needs all four places read, so slots 1 and 2 must be located.
    if let Some(slots) = &blocks.slots
        && blocks
            .replicas
            .iter()
            .chain(slots)
            .all(|b| *b == Block::Unreadable)
    {
        return OpenDecision::AllUnreadable;
    }

    // Each place's copy, decoded once. Unlocated slots hold no copy.
    let [replica0, replica1] = blocks.replicas.map(|b| copy_in(&b));
    let [slot1, slot2] = match blocks.slots {
        Some(slots) => slots.map(|b| copy_in(&b)),
        None => [None, None],
    };

    // Step 2, the identity (§2.2 "The file's identity").
    let identity = match file_identity(&blocks.page0, [&replica0, &replica1], [&slot1, &slot2]) {
        Ok(identity) => identity,
        Err(why) => return OpenDecision::Corrupt(why),
    };

    // Step 3, the classes (§2.2 "A valid copy"), per parity, the primary
    // place first.
    let parity0 = [
        classify(slot1, 0, &identity),
        classify(replica0, 0, &identity),
    ];
    let parity1 = [
        classify(slot2, 1, &identity),
        classify(replica1, 1, &identity),
    ];

    // Step 4, each parity's candidate (§2.2 "Each parity's candidate", the
    // table and the tie rule).
    let candidate0 = match candidate(0, &parity0) {
        Ok(c) => c,
        Err(why) => return OpenDecision::Corrupt(why),
    };
    let candidate1 = match candidate(1, &parity1) {
        Ok(c) => c,
        Err(why) => return OpenDecision::Corrupt(why),
    };

    // Steps 5 and 6. "The newest meta k is the candidate with the higher
    // ckpt_id. If there is none, the open fails with XX001." The two
    // candidates never share a ckpt_id: each has its own parity's.
    let (newest, other) = match (candidate0, candidate1) {
        (None, None) => return OpenDecision::Corrupt(OpenCorrupt::NoCandidate),
        (Some(only), None) | (None, Some(only)) => (only, None),
        (Some(a), Some(b)) if a.ckpt_id > b.ckpt_id => (a, Some(b)),
        (Some(a), Some(b)) => (b, Some(a)),
    };

    // Step 7, misplaced copies (I-META-SLOT). "If a misplaced copy's
    // ckpt_id exceeds k + 1, the open fails with XX001"; one at k + 1 or
    // below is ignored. At k = u64::MAX no ckpt_id exceeds k + 1.
    if let Some(bound) = newest.ckpt_id.checked_add(1) {
        let ahead = parity0
            .iter()
            .chain(&parity1)
            .filter_map(Class::misplaced_ckpt_id)
            .filter(|&c| c > bound)
            .max();
        if let Some(ckpt_id) = ahead {
            return OpenDecision::Corrupt(OpenCorrupt::MisplacedAhead {
                ckpt_id,
                newest: newest.ckpt_id,
            });
        }
    }

    // Step 8. "The previous meta is the other parity's candidate when its
    // ckpt_id is k−1."
    let previous = other.filter(|m| newest.ckpt_id.checked_sub(1) == Some(m.ckpt_id));

    // Step 9.
    OpenDecision::Open(Selection {
        identity,
        newest,
        previous,
    })
}

/// Page 0's identity, when page 0 is read and decodes.
fn page0_identity(page0: &Block<'_>) -> Option<Identity> {
    match page0 {
        Block::Read(bytes) => decode_page0(bytes).ok(),
        Block::Unreadable => None,
    }
}

/// The copy a block holds: its meta, when the block is read and decodes.
/// The `copy` byte plays no part in the rule (§2.2).
fn copy_in(block: &Block<'_>) -> Option<Meta> {
    match block {
        Block::Read(bytes) => decode_meta(bytes).ok().map(|(meta, _copy)| meta),
        Block::Unreadable => None,
    }
}

/// Step 2, the file's identity (§2.2 "The file's identity").
fn file_identity(
    page0: &Block<'_>,
    replicas: [&Option<Meta>; 2],
    slots: [&Option<Meta>; 2],
) -> Result<Identity, OpenCorrupt> {
    // "page 0's fields (§2.1) when page 0's magic and CRC are valid."
    if let Some(id) = page0_identity(page0) {
        return Ok(id);
    }
    // "Otherwise the open reads the two replica blocks first: if neither
    // holds a copy with valid magic and CRC, the open fails with XX001."
    if replicas.iter().all(|c| c.is_none()) {
        return Err(OpenCorrupt::NoIdentity);
    }
    // "The identity is then the fields that every copy with valid magic and
    // CRC, in any of the four places, shares; if two of them differ ...,
    // the open fails with XX001." Unlocated slots hold no copy.
    let mut copies = replicas.into_iter().chain(slots).flatten();
    let first = copies.next().ok_or(OpenCorrupt::NoIdentity)?.identity;
    if copies.all(|m| m.identity == first) {
        Ok(first)
    } else {
        Err(OpenCorrupt::IdentityConflict)
    }
}

/// A place's class against the file's identity (step 3).
enum Class {
    /// Unreadable, not decoding, or in an unlocated slot.
    Invalid,
    /// Another file's identity: ignored.
    Foreign,
    /// The file's identity in a place of the other parity: never opened.
    Misplaced(Meta),
    /// The file's identity in a place of its own parity, whichever `copy`
    /// byte it carries: "a misdirect within the parity, is still valid".
    Valid(Meta),
}

impl Class {
    /// A valid copy's meta.
    fn valid(&self) -> Option<&Meta> {
        match self {
            Class::Valid(meta) => Some(meta),
            Class::Invalid | Class::Foreign | Class::Misplaced(_) => None,
        }
    }

    /// A misplaced copy's `ckpt_id`.
    fn misplaced_ckpt_id(&self) -> Option<u64> {
        match self {
            Class::Misplaced(meta) => Some(meta.ckpt_id),
            Class::Invalid | Class::Foreign | Class::Valid(_) => None,
        }
    }
}

/// Step 3 for one place of parity `parity` (§2.2 "A valid copy").
fn classify(copy: Option<Meta>, parity: usize, identity: &Identity) -> Class {
    match copy {
        None => Class::Invalid,
        Some(meta) if meta.identity != *identity => Class::Foreign,
        Some(meta) if meta_parity(meta.ckpt_id) != parity => Class::Misplaced(meta),
        Some(meta) => Class::Valid(meta),
    }
}

/// Step 4: parity `parity`'s candidate from its places' classes, the
/// primary place first (§2.2's table).
fn candidate(parity: u8, [primary, replica]: &[Class; 2]) -> Result<Option<Meta>, OpenCorrupt> {
    let (p, r) = match (primary.valid(), replica.valid()) {
        // "invalid, misplaced or foreign | invalid, misplaced or foreign | none"
        (None, None) => return Ok(None),
        // "valid m | invalid (torn, zeros, unreadable), misplaced or foreign | m",
        // and the same with the places swapped.
        (Some(only), None) | (None, Some(only)) => return Ok(Some(only.clone())),
        (Some(p), Some(r)) => (p, r),
    };
    // "valid m | valid m′, m′ < m | m", either way round.
    if p.ckpt_id != r.ckpt_id {
        let higher = if p.ckpt_id > r.ckpt_id { p } else { r };
        return Ok(Some(higher.clone()));
    }
    // "valid m | valid m, equal but for copy and crc | m".
    if p == r {
        return Ok(Some(p.clone()));
    }
    // The tie rule: "the copy with the newer boot_id, in serial-number
    // order (§12.5), is the candidate, and equal boot_ids fail the open
    // with XX001". Two ids 2^31 apart are each older than the other, so
    // neither is newer and the open fails too (card A-2-0, open point 4).
    let newer = |a: &Meta, b: &Meta| {
        boot_id_older(b.boot_id, a.boot_id) && !boot_id_older(a.boot_id, b.boot_id)
    };
    if newer(p, r) {
        Ok(Some(p.clone()))
    } else if newer(r, p) {
        Ok(Some(r.clone()))
    } else {
        Err(OpenCorrupt::EqualBootIds { parity })
    }
}
