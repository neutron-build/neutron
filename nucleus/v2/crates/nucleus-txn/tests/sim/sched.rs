//! C-SIM work item 1: the seeded PRNG, the choice source (a run is replayable
//! from `(seed, config)` or from a recorded choice list) and the trace.
//!
//! Independent streams, all derived from the seed: the **schedule** stream
//! feeds every scheduling decision (the scheduler's action picks and the
//! probe's yield decisions), so a run's recorded choice list reproduces them
//! exactly; the **aux** stream feeds crash prefixes and a **program** stream
//! per (session, txn) feeds program generation — inputs that must stay
//! stable while the choice list is being minimized (the minimizer shortens
//! the schedule, and the programs it replays must not change underneath it).

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

/// Tag bit of a recorded **action** choice: the entry is the label hash of
/// the action that was picked (not its position), so removing other choices
/// from a replayed list does not change which action a later entry names.
const ACTION_TAG: u64 = 1 << 63;

/// FNV-1a over the action's name: the stable identity of an action within an
/// enabled set (`s1(CommitSubmit)`, `Resolver`, ...).
pub fn label(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h | ACTION_TAG
}

/// Where scheduling choices come from. Live runs draw from the seed's
/// schedule stream and record every draw. A recorded list holds two kinds of
/// entries: **action** entries (the picked action's label, see [`label`])
/// and **numeric** entries (the probe's yield draw, a compaction pick).
/// Replaying an action entry picks the enabled action with that label; an
/// entry whose action is no longer enabled is **skipped** (this is what
/// makes chunk removal sound: later entries keep their meaning). When the
/// list runs out the rest is drawn from the stream (a shortened list
/// diverges; the fallback keeps the re-run finite and deterministic).
pub struct Choices {
    schedule: Rng,
    /// The aux stream (crash prefixes): independent of the recorded
    /// schedule, so minimizing the choice list does not reshuffle it.
    pub aux: Rng,
    /// Base of the per-(session, txn) program streams.
    program_base: u64,
    replay: Vec<u64>,
    at: usize,
    taken: Vec<u64>,
}

impl Choices {
    pub fn new(seed: u64, replay: Option<Vec<u64>>) -> Choices {
        Choices {
            schedule: Rng::new(seed ^ 0x51ed_270b_2161_2219),
            aux: Rng::new(seed ^ 0x2545_f491_4f6c_dd1d),
            program_base: seed ^ 0x6a09_e667_f3bc_c908,
            replay: replay.unwrap_or_default(),
            at: 0,
            taken: Vec::new(),
        }
    }

    /// The program stream of `session`'s `n`th txn: independent of every
    /// other session, txn and of the schedule, so a program never changes
    /// when scheduling choices are removed.
    pub fn program_rng(&self, session: usize, n: usize) -> Rng {
        Rng::new(
            self.program_base
                ^ (session as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ (n as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F),
        )
    }

    /// Picks a number in `0..n` (a probability draw, a file pick),
    /// recording it. A replayed action entry at the head is left in place.
    pub fn pick(&mut self, n: usize) -> usize {
        debug_assert!(n > 0);
        let raw = match self.replay.get(self.at) {
            Some(&r) if r & ACTION_TAG == 0 => {
                self.at += 1;
                r
            }
            _ => self.schedule.next_u64() & !ACTION_TAG,
        };
        self.taken.push(raw);
        (raw % n as u64) as usize
    }

    /// Picks one of the enabled actions, given by name, and returns its
    /// index. Replay: numeric entries and entries naming an action that is
    /// not enabled are skipped.
    pub fn pick_action(&mut self, names: &[String]) -> usize {
        debug_assert!(!names.is_empty());
        while self.at < self.replay.len() {
            let r = self.replay[self.at];
            self.at += 1;
            if r & ACTION_TAG != 0 {
                if let Some(i) = names.iter().position(|n| label(n) == r) {
                    self.taken.push(r);
                    return i;
                }
            }
        }
        let i = (self.schedule.next_u64() % names.len() as u64) as usize;
        self.taken.push(label(&names[i]));
        i
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
