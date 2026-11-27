//! `WbOracle` (card A-3c, Work 13.7): a plain model of slots and homes,
//! written from S-STORE §4.3–§4.5 and §13 step 6 as card A-3c's Work 4
//! restates them, and from nothing in `slots.rs`: it calls no store code. It
//! is the I-NO-INPLACE judge (`row_i_no_inplace`) and predicts every
//! allocation of the slot tests.

use std::collections::BTreeMap;

use super::PS;

/// What a slot is to the oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Use {
    /// Slots 0–2.
    Fixed,
    /// A slot of the replica region.
    Reserved,
    /// Allocatable.
    Free,
    /// Referenced by the previous image and not the current one.
    Pending,
    /// Some page's home.
    Used,
    /// At or past `slot_count`.
    PastEnd,
}

/// The model: every slot's use, every page's home `(slot, epoch)`, and the
/// epoch `e` the test keeps in step with the pool's.
#[derive(Debug, Clone)]
pub struct WbOracle {
    page_size: u32,
    slot_count: u64,
    use_of_slot: Vec<Use>,
    /// `(kind, id)` to the page's home and the epoch its entry carries.
    pub home: BTreeMap<(u8, u64), (u64, u32)>,
    /// The current epoch.
    pub e: u32,
}

/// The growth in bytes of S-STORE §4.5: `max(1 MiB, file_len / 16)`.
fn growth_bytes(file_len: u64) -> u64 {
    (file_len / 16).max(1 << 20)
}

impl WbOracle {
    /// A fresh file's model at `page_size`, `slot_count` slots and epoch
    /// `e`: every slot of `[3, slot_count)` free but the reserved ones
    /// (§2.4: those overlapping the region, `⌊1 MiB / page_size⌋` up to
    /// `⌈(1 MiB + 8 KiB) / page_size⌉`).
    pub fn new(page_size: u32, slot_count: u64, e: u32) -> WbOracle {
        let ps = u64::from(page_size);
        let first_reserved = (1 << 20) / ps;
        let end_reserved = ((1u64 << 20) + (1u64 << 13)).div_ceil(ps);
        let mut use_of_slot = Vec::new();
        use_of_slot.resize(usize::try_from(slot_count).unwrap_or(usize::MAX), Use::Free);
        for (s, u) in use_of_slot.iter_mut().enumerate() {
            let s = s as u64;
            *u = if s < 3 {
                Use::Fixed
            } else if (first_reserved..end_reserved).contains(&s) {
                Use::Reserved
            } else {
                Use::Free
            };
        }
        WbOracle {
            page_size,
            slot_count,
            use_of_slot,
            home: BTreeMap::new(),
            e,
        }
    }

    /// What `slot` is.
    pub fn use_of(&self, slot: u64) -> Use {
        match self.use_of_slot.get(slot as usize) {
            Some(u) => *u,
            None => Use::PastEnd,
        }
    }

    /// The slot the next allocation hands out (§4.5): the lowest free in
    /// `[3, slot_count)`; with none free, the file grows by
    /// `max(1 MiB, file_len / 16)` rounded down to whole slots, every new
    /// slot is free and none reserved, and the lowest of them, the old
    /// `slot_count`, is taken.
    pub fn predict_alloc(&self) -> u64 {
        match (3..self.slot_count).find(|s| self.use_of(*s) == Use::Free) {
            Some(s) => s,
            None => self.slot_count,
        }
    }

    /// The slots one growth adds, at the file's present length.
    fn growth_slots(&self) -> u64 {
        growth_bytes(self.slot_count * u64::from(self.page_size)) / u64::from(self.page_size)
    }

    /// Take the model to the `slot_count` a write at `slot` needs: each
    /// growth adds [`Self::growth_slots`] free slots.
    fn grow_for(&mut self, slot: u64) {
        while slot >= self.slot_count {
            let new_count = self.slot_count + self.growth_slots();
            self.use_of_slot.resize(new_count as usize, Use::Free);
            self.slot_count = new_count;
        }
    }

    /// How many slots the model holds free.
    pub fn predict_free(&self) -> u64 {
        (3..self.slot_count)
            .filter(|s| self.use_of(*s) == Use::Free)
            .count() as u64
    }

    /// The file's length in slots the model holds.
    pub fn slot_count(&self) -> u64 {
        self.slot_count
    }

    /// A page was written: the slot becomes its home, and its previous home
    /// is freed iff that home's entry carried the current epoch (§4.4: "a
    /// writer tells by its ref's epoch, the current one").
    pub fn wrote(&mut self, kind: u8, id: u64, slot: u64, epoch: u32) {
        self.grow_for(slot);
        if let Some((old, old_epoch)) = self.home.get(&(kind, id)).copied()
            && old != 0
            && old_epoch == self.e
        {
            self.set_use(old, Use::Free);
        }
        self.set_use(slot, Use::Used);
        self.home.insert((kind, id), (slot, epoch));
    }

    /// A page's mapping was removed (a free): its home goes as `wrote`'s
    /// previous home does.
    pub fn freed(&mut self, kind: u8, id: u64) {
        if let Some((old, old_epoch)) = self.home.remove(&(kind, id))
            && old != 0
            && old_epoch == self.e
        {
            self.set_use(old, Use::Free);
        }
    }

    /// A flip: `e += 1`.
    pub fn flip(&mut self) {
        self.e += 1;
    }

    /// Step 6 (§13 step 6): the pending slots become free, then `newly` is
    /// exactly what is pending.
    pub fn release(&mut self, newly: &[u64]) {
        for s in 0..self.slot_count {
            if self.use_of(s) == Use::Pending {
                self.set_use(s, Use::Free);
            }
        }
        for &s in newly {
            self.set_use(s, Use::Pending);
        }
    }

    /// Whether some page's home is `slot`.
    pub fn is_live_home(&self, slot: u64) -> bool {
        self.home.values().any(|(s, _)| *s == slot)
    }

    /// A page's home and the epoch its entry carries.
    pub fn home_of(&self, kind: u8, id: u64) -> Option<(u64, u32)> {
        self.home.get(&(kind, id)).copied()
    }

    fn set_use(&mut self, slot: u64, u: Use) {
        if let Some(place) = self.use_of_slot.get_mut(slot as usize) {
            *place = u;
        }
    }
}

/// The reserved slots at `PS` as the oracle derives them (§2.4), for a test
/// that states them: 256 and 257.
pub fn reserved_at_ps() -> [u64; 2] {
    let ps = u64::from(PS);
    let first = (1 << 20) / ps;
    [first, ((1u64 << 20) + (1u64 << 13)).div_ceil(ps) - 1]
}
