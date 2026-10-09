//! C-K3 conformance suite over both bake-off backends, bare and behind the
//! `Fault` WAL-crash wrapper (C-S1 work item 2). Failures here are recorded
//! as disqualifiers in `DECISION.md`.

#![allow(clippy::unwrap_used)] // tests may unwrap (rule: not outside tests)

use nucleus_kv::conformance::FaultHarness;

#[cfg(feature = "fjall")]
nucleus_kv::kv_conformance_tests!(fjall, nucleus_bakeoff::fjall_kv::FjallHarness::new());

#[cfg(feature = "fjall")]
nucleus_kv::kv_conformance_tests!(
    fault_fjall,
    FaultHarness(nucleus_bakeoff::fjall_kv::FjallHarness::new(),)
);

#[cfg(feature = "rocks")]
nucleus_kv::kv_conformance_tests!(rocks, nucleus_bakeoff::rocks::RocksHarness::new());

#[cfg(feature = "rocks")]
nucleus_kv::kv_conformance_tests!(
    fault_rocks,
    FaultHarness(nucleus_bakeoff::rocks::RocksHarness::new(),)
);
