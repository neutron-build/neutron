Card: DEP-RUSQLITE-2
Class: test
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code session fd71ba36-07c7-4e14-886f-f4d266f728b7 (P gate sub-agent)
Date: 2026-11-14
Gate-exit: GATE EXIT=0
Commit: 0e0040b4e47b0bf03273e2a00bf5a8db9cb9bb2b
Mutants: revert rusqlite "0.41" -> "0.37" (scratch copy) -> cargo deny check advisories (caught: RUSTSEC-2026-0459 on libsqlite3-sys 0.35.0, advisories FAILED)
Followups: none

## Gate run

Scaffold fresh-gate.sh pulled through the shallow `--no-local` clone of
nucleus/v3-scaffold at 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b (no EXTRACT FAILED).
Run on 0e0040b4e47b0bf03273e2a00bf5a8db9cb9bb2b:

    fresh-gate: commit 0e0040b4e47b0bf03273e2a00bf5a8db9cb9bb2b
    fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b
    fresh-gate: checks passed (4780 files hashed in 1.35 s)
    STEP test EXIT=0, clippy 0, fmt 0, deny 0, wasm32 0, core-paths-selftest 0, enforcement-tests 0
    GATE EXIT=0

stderr was empty. Exit code 0.

## Findings

- Scope: `git diff nucleus/v3-scaffold...HEAD` is one file,
  nucleus/v3/spikes/e0-btree/Cargo.toml, 1+/1-. The only change is the rusqlite
  version string, "0.37" -> "0.41". No deny.toml change, no code.
- Twin check: the `rusqlite = ...` line is byte-identical to
  card/v3-DEP-RUSQLITE-1's (cmp). The whole file is byte-identical to the chain's too.
- deny: `cargo deny check advisories` on a clean clone of the commit reports
  `advisories ok`. When the line goes back to "0.37" on a scratch copy, it reports
  RUSTSEC-2026-0459 (libsqlite3-sys 0.35.0) and `advisories FAILED`. Restoring the
  line turns it green again. So the green comes from the dependency moving, not
  from an ignore.
- `cargo test --release -p e0-btree --features sqlite`: rc 0, 18 passed, 0 failed
  (the same count the chain recorded). The build resolved rusqlite 0.41.1 and
  libsqlite3-sys 0.39.1.
- No unsafe/unwrap concerns: there is no code in the diff.

## Anomaly (informational, not this card)

The tip of card/v3-DEP-RUSQLITE-1 is 2e8d1b13 "X-5c-F4-F1-F1-F3-F1 verdict
MERGE-WITH-FOLLOWUPS", which sits above b69e0781 (DEP-RUSQLITE-1 verdict
MERGE). The twin comparison used that tip. The file is identical at both.

Gate-side deviation: so that it would not contend with the fresh-gate build,
the scratch deny/test/mutant runs used
CARGO_TARGET_DIR=$HOME/.cache/nv3-target/gate-DEP-RUSQLITE-2-scratch. The
fresh gate used the prescribed gate-DEP-RUSQLITE-2.
