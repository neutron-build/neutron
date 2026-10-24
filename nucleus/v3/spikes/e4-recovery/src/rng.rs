//! The spike's PRNG: splitmix64 for seeding, xoshiro256** for streams.
//!
//! Written for this spike per card X-5 Work 0 (no crates beyond thiserror;
//! the whole schedule must stay inspectable and byte-reproducible). Every
//! random decision the experiment makes goes through one of these streams,
//! and each stream is seeded deterministically from the seed, which is what
//! makes a per-seed digest reproducible across runs and across
//! `--threads` (Work 0, "Determinism is a hard requirement").
//!
//! No stream is ever iterated through a `HashMap`/`HashSet` (those are
//! banned from schedule-visible decisions by the card); iteration order
//! here is always explicit.

/// splitmix64: the seeder. One step of the generator.
pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Derive a named sub-stream seed from a seed and a stream tag, so that
/// adding or removing a stream never perturbs the others (each stream gets
/// an independent splitmix64 derivation).
pub fn derive(seed: u64, tag: u64) -> u64 {
    let mut s = seed ^ tag.wrapping_mul(0xA076_1D64_78BD_642F);
    splitmix64(&mut s)
}

/// xoshiro256** — the stream generator (the reference update). Streams are
/// owned by the phase that seeded them; accidental sharing would couple
/// two schedules, so it is deliberately not `Clone`.
#[derive(Debug)]
pub struct Rng {
    s: [u64; 4],
}

impl Rng {
    /// Seed four words from one with splitmix64 (the reference seeding).
    pub fn new(seed: u64) -> Self {
        let mut sm = seed;
        let s = [
            splitmix64(&mut sm),
            splitmix64(&mut sm),
            splitmix64(&mut sm),
            splitmix64(&mut sm),
        ];
        Self { s }
    }

    /// The next raw 64-bit word.
    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        let [s0, s1, s2, s3] = self.s;
        let n2 = s2 ^ s0;
        let n3 = s3 ^ s1;
        let n1 = n2 ^ s1;
        let n0 = n3 ^ s0;
        self.s = [n0, n1, n2 ^ t, n3.rotate_left(45)];
        result
    }

    /// A uniform value in `[0, bound)` (`bound > 0`). Rejection-free
    /// modulo sampling is fine here: every caller treats the stream as an
    /// opaque deterministic sequence, and no statistical property of the
    /// campaign depends on the removal of modulo bias.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound.max(1)
    }

    /// True with probability `num/den`.
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        den == 0 || self.below(den) < num
    }

    /// A `bool` with probability `p_num/p_den == 1/2` style helpers live at
    /// call sites; this is the coin flip.
    pub fn coin(&mut self) -> bool {
        self.next_u64() & 1 == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// splitmix64 outputs pinned against accidental change (the algorithm
    /// constants are the published ones; these two values pin this
    /// implementation so the schedule cannot silently drift). The
    /// authoritative correctness vector for the spike's checksums is the
    /// card's CRC32C one in `crc.rs`.
    #[test]
    fn splitmix_reference() {
        let mut s = 0u64;
        assert_eq!(splitmix64(&mut s), 16294208416658607535);
        assert_eq!(splitmix64(&mut s), 7960286522194355700);
    }

    /// Two streams from the same seed agree byte for byte.
    #[test]
    fn streams_reproduce() {
        let mut a = Rng::new(0xDEAD_BEEF);
        let mut b = Rng::new(0xDEAD_BEEF);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    /// Two streams from different seeds differ (the seed actually matters).
    #[test]
    fn seeds_differ() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        let mut same = true;
        for _ in 0..8 {
            same &= a.next_u64() == b.next_u64();
        }
        assert!(!same);
    }
}
