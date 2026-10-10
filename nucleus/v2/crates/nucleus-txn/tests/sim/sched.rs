//! C-SIM work item 1: the seeded PRNG, the choice source (a run is replayable
//! from `(seed, config)` or from a recorded choice list) and the trace.
//!
//! Two independent streams, both derived from the seed: the **schedule**
//! stream feeds every scheduling decision (the scheduler's action picks and
//! the probe's yield decisions), so a run's recorded choice list reproduces
//! them exactly; the **aux** stream feeds program generation and crash
//! prefixes — inputs that must stay stable while the choice list is being
//! minimized (the minimizer shortens the schedule, and the programs it
//! replays must not change underneath it).

/// A small splitmix64 PRNG: no new dependency, deterministic on every
/// platform.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut z = self.0;
        self.0 = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform `0..n` draw (`n > 0`).
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// A draw with probability `pm` per mille.
    pub fn chance_pm(&mut self, pm: usize) -> bool {
        self.below(1000) < pm
    }
}

/// Where scheduling choices come from. Live runs draw from the seed's
/// schedule stream and record every draw; replays pop the recorded list
/// first and fall back to the stream when it runs out (a shortened list
/// diverges; the fallback keeps the re-run finite and deterministic).
pub struct Choices {
    schedule: Rng,
    /// The aux stream (program generation, crash prefixes): independent of
    /// the recorded schedule, so minimizing the choice list does not
    /// reshuffle the programs being replayed.
    pub aux: Rng,
    replay: Vec<u64>,
    at: usize,
    taken: Vec<u64>,
}

impl Choices {
    pub fn new(seed: u64, replay: Option<Vec<u64>>) -> Choices {
        Choices {
            schedule: Rng::new(seed ^ 0x51ed_270b_2161_2219),
            aux: Rng::new(seed ^ 0x2545_f491_4f6c_dd1d),
            replay: replay.unwrap_or_default(),
            at: 0,
            taken: Vec::new(),
        }
    }

    /// Picks an index in `0..n`, recording the raw choice.
    pub fn pick(&mut self, n: usize) -> usize {
        debug_assert!(n > 0);
        let raw = if self.at < self.replay.len() {
            let r = self.replay[self.at];
            self.at += 1;
            r
        } else {
            self.schedule.next_u64()
        };
        self.taken.push(raw);
        (raw % n as u64) as usize
    }

    /// The choices taken so far (the recorded schedule).
    pub fn taken(&self) -> &[u64] {
        &self.taken
    }
}

/// The run trace: one line per scheduled step (probe-run steps indented),
/// plus probe points. Compared byte for byte for the determinism check.
#[derive(Default)]
pub struct Trace {
    lines: Vec<String>,
}

impl Trace {
    pub fn push(&mut self, line: String) {
        self.lines.push(line);
    }

    pub fn render(&self) -> String {
        self.lines.join("\n")
    }
}
