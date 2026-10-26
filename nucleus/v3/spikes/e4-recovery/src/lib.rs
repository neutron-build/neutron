//! The E4 recovery spike (card X-5; PLAN-v3 §4 E4).
//!
//! A self-contained prototype of the S-STORE durability path — WAL + page
//! map + copy-on-write checkpoint + replay (`docs/S-STORE.md` §12–§14) —
//! running on a deterministic simulated disk with a pre-registered crash
//! and media-fault model, judged by the P-written oracle of card X-5
//! Work 2.
//!
//! Deps: `thiserror` only; CRC32C and the PRNG are written in-crate so
//! everything on the durability path stays inspectable in the spike
//! (Work 0). Determinism is a hard requirement: no wall clock, no
//! `HashMap`/`HashSet` iteration in schedule-visible decisions, nothing
//! depends on thread timing.
//!
//! Module map:
//! - [`crc`], [`rng`]: the written-in primitives;
//! - [`sim`]: the disk, the baton scheduler, the fault model, `E4Env`;
//! - [`model`]: the reference model;
//! - [`oracle`]: Work 2's checks (P-written, binding);
//! - [`store`]: the S-STORE subset under test (Work 3–4), converted to
//!   the S-BATON step discipline of card X-5b Work 11;
//! - `monitor`: the harness-side monitors (M-*, Work 3.10);
//! - `workload`/`campaign`: the seed campaign, `verify`, `mutants`.

pub mod campaign;
pub mod crc;
pub mod harness;
pub mod model;
pub mod monitor;
pub mod oracle;
pub mod rng;
pub mod sim;
pub mod store;
