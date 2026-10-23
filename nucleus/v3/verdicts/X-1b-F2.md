Card: X-1b-F2
Class: mechanical
Card-gate: S
Verdict: MERGE-WITH-FOLLOWUPS
Gate-tier: S
Gate-model: zai-coding-plan/glm-5.3
Gate-session: GLM gate subagent (T3 review session, card X-1b-F2)
Date: 2026-10-23
Gate-exit: GATE EXIT=0
Commit: 2afa46d0
Mutants: README-DRIFT (65,483 -> 65,489 in the W5 cell) -> re-derivation from the committed datum (536,436,736 B / 8,192 = 65,483), GATE-UNIT (16.8 -> 17.2 GiB in the W6 (b) RSS cell) -> re-derivation from the committed raw KiB (17,612,800 / 1,048,576 = 16.797; 17.2 is reachable only via the KiB/1,024,000 bug), GATE-CAVEAT-DROP (HOL p50 stability caveat removed, cells back to stable) -> re-derivation against the committed gate re-run medians (182 ms (b) / 20.8 s (c) vs the cells' 4.6 / 4.1 ms), GATE-PAGES (649,794 -> 649,843) -> re-derivation tree_bytes/8,192 (649,794, the same division the committed lib.rs pool test performs)
Followups: X-1b-F3, X-1b-F4

## Findings

**Gate run.** The literal last line was `GATE EXIT=0` on 2afa46d0 (every
step EXIT=0: test, clippy, fmt, deny, wasm32, core-paths-selftest,
enforcement-tests), run with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1b-F2`.

**Scope.** `git diff nucleus/v3-scaffold...HEAD --stat`: only
`nucleus/v3/spikes/e1-bench/README.md` (24+/12-) — exactly the Touch only
list. No protected path; no code, results JSON, workload or decision-rule
change (the rule's lines are byte-identical; the measured table cells are
unchanged apart from the RSS unit rendering and the p50 annotations).

**Re-derivations** (the `--out` JSONs were never committed; the committed
record is the README's own tables, `verdicts/X-1b.md`, and the byte
constants in `src/lib.rs` — every check below was re-run by this gate):
- W6 RSS: 17.2 bug implies raw 17,612,800 KiB; /1,048,576 = 16.797 →
  **16.8 GiB** exact. 14.8 implies raw in [15,104,000, 15,206,400] KiB →
  true value in [14.40, 14.50]; **14.4** follows the X-1b verdict, which
  held the raw JSON. Both match card and verdict.
- W5 pages: 536,436,736 / 8,192 = **65,483** exact (verdict-flagged); the
  old-pool peak 536,469,504 / 8,192 = **65,487** exact (lib.rs constant,
  round-1 verdict) — all three stray "65,489"s corrected.
- HOL: sample counts stay as committed (n=18/n=9; min–max 4–32 (b),
  4–18 (c)); the caveat cites the gate re-run medians 182 ms / 20.8 s
  verbatim and marks only p99 (= max) stable. Present and correct.
- Worker extras, same defect class, all re-derived OK: W6 page counts
  649,794 / 649,289–649,871 (exact; `tree_bytes = pages × page_size`,
  e1-arm-b engine.rs:617); W1 "12–30×" → 13.9×/14.7× at 1 thread falling
  to 2.0×/1.2× at 16; "moved ≤ 5%" → five of eight cells, max ±16%
  (−10.2% CPU t=8, ±14.9/15.5% txns/s t=4/8); the "−13% at t=4"
  citation traces to verdict X-1b. Permitted by the Goal ("every stated
  number matches the committed results data") and Work 4 ("numbers only
  where they are wrong"). Sound, not scope creep — but it exceeds the
  three named defects, recorded here for the audit trail.

**Honest limit.** No committed checker validates README numbers, and
`gate.sh` alone catches none of the four mutants — the re-derivation is
the only catcher, exactly as the card's Mutation evidence section
prescribes. All four mutants were reverted; `git status` was clean
before this file was written.

## Follow-ups

**X-1b-F3 (mechanical, S): fix the same wrong page arithmetic in
`spikes/e1-bench/src/lib.rs` comments.** Lines 86/158/268 still say
"65,489 pages" for the 536,469,504 B W5 peak (should be 65,487) and
lines 90/162/276 say "≈ 649,843 pages" for the 5,323,112,448 B W6 tree
(should be 649,794). Out of X-1b-F2's Touch-only list (README only), so
correctly left untouched there. Comments only; the test itself computes
the divisions and passes.

**X-1b-F4 (mechanical, S): note the W5 (c) peak-RSS spread in the
README.** Verdict X-1b's follow-up text asked for "the W5 (c) RSS spread
(1,592–1,770 MiB)", but card X-1b-F2 omitted it, so the W5 table still
carries the single 1,594 MiB figure with no spread note. One sentence,
numbers taken from verdict X-1b; no measured value changes.
