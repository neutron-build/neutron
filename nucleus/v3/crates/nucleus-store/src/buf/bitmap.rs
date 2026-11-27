//! A grow-only bitmap (card A-3c, Work 1), crate-private: the one shape
//! both in-memory free sets take. S-STORE §4.3: "one bit per slot in
//! `[3, slot_count)`" and "one bit per LPID in `[1, next_lpid)`"; R-1-memory
//! §5.2: each pending generation is held "in at most one bit per slot, as a
//! bitmap where a list would be larger".
//!
//! Invariants owned here:
//! - **No panic path** (PLAN-v3 §6.1): every word is reached through `get`;
//!   a bit at or past `len` reads `false`, and writing one is `Invalid`
//!   with nothing changed.
//! - **New bits are clear**: [`Bitmap::with_len`] and [`Bitmap::grow_to`]
//!   leave every bit they add clear, and `grow_to` never shortens.
//! - **Every allocation is fallible**: `try_reserve_exact`, and a failure is
//!   [`StoreError::OutOfMemory`] (N16: 53200) with the bitmap unchanged.

use std::collections::TryReserveError;

use crate::store::StoreError;

/// The bits one word holds.
const BITS: u64 = 64;

/// A bitmap of `len` bits, bit `i` in word `i / 64`, bit `i % 64`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Bitmap {
    /// `ceil(len / 64)` words; every bit past `len` is clear.
    words: Vec<u64>,
    /// The bits the bitmap covers: `0..len`.
    len: u64,
}

/// A length no `usize` can hold.
fn past_usize() -> StoreError {
    StoreError::Invalid("a bitmap length past usize".into())
}

/// A failed reservation of words.
fn oom(e: TryReserveError) -> StoreError {
    StoreError::OutOfMemory(format!("bitmap words: {e}"))
}

/// The words that cover `len` bits.
fn words_for(len: u64) -> Result<usize, StoreError> {
    usize::try_from(len.div_ceil(BITS)).map_err(|_| past_usize())
}

/// The low six bits of `n`, as a shift: always below 64, so the conversion
/// cannot fail; `u32::MAX` only names the impossible case.
fn low(n: u64) -> u32 {
    u32::try_from(n % BITS).unwrap_or(u32::MAX)
}

impl Bitmap {
    /// A bitmap of `len` clear bits.
    ///
    /// # Errors
    /// [`StoreError::OutOfMemory`] if the words cannot be reserved;
    /// [`StoreError::Invalid`] if the word count passes `usize`.
    pub(crate) fn with_len(len: u64) -> Result<Bitmap, StoreError> {
        let n = words_for(len)?;
        let mut words = Vec::new();
        words.try_reserve_exact(n).map_err(oom)?;
        words.resize(n, 0);
        Ok(Bitmap { words, len })
    }

    /// Cover `len` bits: the new ones, if any, are clear. A smaller `len`
    /// changes nothing.
    ///
    /// # Errors
    /// As [`Bitmap::with_len`]; the bitmap is then unchanged.
    pub(crate) fn grow_to(&mut self, len: u64) -> Result<(), StoreError> {
        if len <= self.len {
            return Ok(());
        }
        let n = words_for(len)?;
        let extra = n.checked_sub(self.words.len()).ok_or_else(past_usize)?;
        self.words.try_reserve_exact(extra).map_err(oom)?;
        self.words.resize(n, 0);
        self.len = len;
        Ok(())
    }

    /// Take `longer`'s place, keeping every bit: `longer` covers at least as
    /// many bits as `self`, all clear, and its words were reserved by the
    /// caller, so this copies words and allocates nothing (the free-set
    /// lock's rule, card A-3c, Work 5). Returns the old bitmap, for the
    /// caller to drop after it releases its lock.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `longer` covers fewer bits than `self`.
    pub(crate) fn adopt(&mut self, mut longer: Bitmap) -> Result<Bitmap, StoreError> {
        if longer.len < self.len {
            return Err(StoreError::Invalid(format!(
                "a bitmap of {} bits cannot take the place of one of {}",
                longer.len, self.len
            )));
        }
        for (to, from) in longer.words.iter_mut().zip(self.words.iter()) {
            *to = *from;
        }
        Ok(core::mem::replace(self, longer))
    }

    /// How many bits the bitmap covers.
    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    /// Word `w`, or 0 for a word it does not hold (its bits are all clear).
    fn word(&self, w: u64) -> u64 {
        usize::try_from(w)
            .ok()
            .and_then(|i| self.words.get(i))
            .copied()
            .unwrap_or(0)
    }

    /// Bit `i`: `false` at or past `len`.
    pub(crate) fn get(&self, i: u64) -> bool {
        self.word(i / BITS) & (1 << low(i)) != 0
    }

    /// Set bit `i` to `v`.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `i` is at or past `len`; nothing changes.
    pub(crate) fn set(&mut self, i: u64, v: bool) -> Result<(), StoreError> {
        if i >= self.len {
            return Err(StoreError::Invalid(format!(
                "bitmap bit {i} is at or past the {len} bits it covers",
                len = self.len
            )));
        }
        let w = usize::try_from(i / BITS).map_err(|_| past_usize())?;
        let word = self
            .words
            .get_mut(w)
            .ok_or_else(|| StoreError::Invalid("a bitmap word it does not hold".into()))?;
        let bit = 1 << low(i);
        if v {
            *word |= bit;
        } else {
            *word &= !bit;
        }
        Ok(())
    }

    /// The lowest set bit in `[lo, min(hi, len))`: whole words are masked
    /// and skipped, so a long clear run costs one test a word.
    pub(crate) fn first_set_in(&self, lo: u64, hi: u64) -> Option<u64> {
        let hi = hi.min(self.len);
        if lo >= hi {
            return None;
        }
        let first = usize::try_from(lo / BITS).ok()?;
        let last = usize::try_from((hi - 1) / BITS).ok()?;
        for w in first..=last {
            let mut mask = self.word(u64::try_from(w).unwrap_or(u64::MAX));
            if w == first {
                mask &= u64::MAX << low(lo);
            }
            if w == last {
                mask &= u64::MAX >> (63 - low(hi - 1));
            }
            if mask != 0 {
                let base = u64::try_from(w).ok()?;
                return base
                    .checked_mul(BITS)
                    .and_then(|b| b.checked_add(u64::from(mask.trailing_zeros())));
            }
        }
        None
    }

    /// How many bits are set.
    pub(crate) fn count_ones(&self) -> u64 {
        self.words
            .iter()
            .map(|w| u64::from(w.count_ones()))
            .fold(0, u64::saturating_add)
    }
}
