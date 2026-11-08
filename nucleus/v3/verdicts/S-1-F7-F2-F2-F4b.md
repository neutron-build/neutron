Card: S-1-F7-F2-F2-F4b
Class: spec
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: nv3-gate-S-1-F7-F2-F2-F4b (T3 review sub-agent, Claude Code session 3e3ee303-bb75-43ec-8799-fc492cd7cae4; it wrote neither card amendment, ruling, review, nor any S-1-F7-F2-F2-F4b commit)
Date: 2026-11-07
Gate-exit: GATE EXIT=0
Commit: f9b815db66a5fd1f0c040bce0f31e23c8d1846e9
Mutants: spec mutants checked on paper against the §15 text (all 27 new, plus CURSOR-STALE-LPID): caught as the card's Mutation-evidence table says; gate text mutants on a fresh clone at f9b815db: GM1 drop "with the list's records for its entries applied" -> UNCAUGHT; GM2 drop "The built page is written to a fresh slot, and that slot is recorded in its parent's buffer, up to the root." -> UNCAUGHT; GM3 drop "A map page whose epoch-k content it has taken counts as written for image k" -> UNCAUGHT; GM4 drop "or that the list's records name" -> UNCAUGHT; GM5 drop the fairness bound's "a frame acquisition's victim write" -> UNCAUGHT; GM6 drop §5.4's gate-waiter pin sentence -> UNCAUGHT; GM7 §4.1 latches the parent before releasing the page -> G48; GM8 drop "std::sync::RwLock ... does not qualify" -> UNCAUGHT; GM9 let a reader read under a shared latch -> G10; GM10 drop the found-flag from the parked position -> G21
Followups: none

## Gate run

- **Script.** The scaffold's `fresh-gate.sh` and `git-env.sh` were extracted through the `--no-local` shallow clone (GATE-PROMPT step 1), with no `EXTRACT FAILED` line. Scaffold id acc81c776183bd9853f7a69e2cec4262d300d89f. The script ran from `~/Workspace/neutron-v3` with `CARGO_TARGET_DIR=~/.cache/nv3-target/gate-S-1-F7-F2-F2-F4b`, on commit f9b815db66a5fd1f0c040bce0f31e23c8d1846e9.
- **Lines.** One run, in the foreground harness as a background task, waited for. `fresh-gate: commit f9b815db66a5fd1f0c040bce0f31e23c8d1846e9`, `fresh-gate: base acc81c776183bd9853f7a69e2cec4262d300d89f`, `fresh-gate: checks passed (4778 files hashed in 2.05 s)`, `fresh-gate: 2 file(s) matched through their eol conversion`; steps STEP test EXIT=0; STEP clippy EXIT=0; STEP fmt EXIT=0; STEP deny EXIT=0; STEP core-paths-selftest EXIT=0; STEP enforcement-tests EXIT=0; literal last line `GATE EXIT=0`. stderr empty, no refusal. `$fg` removed afterwards.
- **Scope.** `git diff nucleus/v3-scaffold...f9b815db --stat` lists only `nucleus/v3/docs/S-STORE.md` (+54/−20), in 7 commits on acc81c77, which is within Touch only. Hunk by hunk the diff stays inside Limits 1–17:
  - §6.8 "Root names" changes only in step (2)'s span plus the one inserted sentence (diffed character by character against the scaffold);
  - §5.6 changes only in "Reserve";
  - I-CKPT-AT-T's (a)–(c) are untouched;
  - G36's byte identities hold.
- **Acceptance.** G1–G57 were run on a fresh `--no-local` clone checked out at f9b815db (not the worker's worktree), with `nucleus/v3-scaffold` at acc81c77: all 57 pass. On the scaffold, every G fails except G36 and G38, which are identity checks and negative checks.

## Do the amendments carry the rulings and reviews?

- **Lock-order review.**
  - C1 → Work 12 and 18 (the parked position and the per-call bound, which covers point reads and probes).
  - C2's exception → Work 2.
  - C4 → Work 3.
  - C5 → Work 5 and 8.
  - C6.1–C6.4 → Work 7, 16, 9 and 6.
  - C7 → Work 14.
  - C8 → Work 13.
  - C3 is -F4a's.
  
  Each is present, in the review's terms, apart from the fairness-bound term in R1.
- **Map-order ruling.**
  - (a′), meaning the list, the map phase and one claim per thread → Work 1, 1a–1c and 1e.
  - Item 2, the two-step write → Work 1d.
  - Item 3 → G37.
  - Item 4 → G34.
  
  Faithful.
- **Map-order review.**
  - C1 → the slot sentence and the bitmap sentence.
  - C2 → option (i): the map phase sets no current ref.
  - C3 → the drain names the §5.2 victim, plus the debug assertion.
  - C4 → sources decided under latch and re-checked, never put in the page table.
  - C5 → the namespace.
  - C6 → reserved at open.
  - C7 → E4's B5 adoption guard, under the frame latch.
  
  All seven are adopted as the review wrote them.
- **Limits.** The one new widening, Limit 16's second bullet for C7, loosens nothing else. No other card pins that bullet.
- **Greps.** The map-order amendment's rewrites of G1, G12, G28–G30 and G32–G34 are all strictly stronger than before. The review amendment's G43 is not (R2).

## Substance (judged)

- **Deadlock freedom.** I walked these holder shapes:
  - data writers (one tree, top-down, after the gate);
  - readers (pin with no latch held, no I/O in the hold);
  - parked and resuming cursors (they hold nothing);
  - the stale-name first latch (it holds nothing else, and directory changes never hold a stale name);
  - eviction (a `try_latch` victim, then the current MapLeaf, which ranks last);
  - the two-step map write (the child is released before the parent is taken, and no eviction runs under a map latch);
  - claims (no map latch, so no wait edge);
  - the data phase (frame, then current MapLeaf);
  - the map phase (one latch at a time, and it waits holding none);
  - DropTree's last step (blocks holding the directory leaf, and only brief holders are ahead of it);
  - DDL, sweep and prune (one tree, or `try_latch`);
  - recovery (single-threaded);
  - the pool floor (finite through §5.4 and -F2-F2-F2).

  I found no cycle. The map↔map cycle is closed by construction.
- **Cursor parking.** Carrying the group state is exact. The conversion lands at ts `c ≤ S`, after the parked `x` (a version above `S`), and the carried candidate is then the same version, so nothing is returned twice. The point-read variant holds finalize back, as review C1 requires.
- **Fair latch.** The bounded-bypass property plus "no FIFO" avoids convoys, and the flip delay is bounded. The stated bound is wrong as written (R1).
- **Image-record list.**
  - **Crash cuts.** Before meta `k`, every listed slot is free in image `k−1`. After meta `k`, the map phase has applied the whole list, and step 3 precedes step 4.
  - **Slot reuse.** C1 closes the current-home hole. C2's race is gone, because only a live-frame write sets current refs. The C3 drain covers the §5.2 victims.
  - **Size.** The list is bounded at ≤ F (one record per page dirty at the flip) and reserved at open.
- **Two-step write and C4's re-check.** The pin bars a second eviction, the page-table entry bars a stale reload, and the gate keeps the flip out of the gap. The copy → frame → recheck → list → home chain is TOCTOU-safe in every interleaving I tried. Its two key points:
  - a copy's install happens under the frame's latch, so "`k+1`, re-check the copy" catches it;
  - a claim holds the copy's latch until its append, so "no longer the copy" implies a list record.
- **A-3c and A-4c text.** See R3.

## Rework (precise)

Every fix below changes text that the card quotes verbatim, so the card needs a P amendment (with its greps) before the implementer reworks.

**R1 (blocking): the fairness bound drops the ruling's queued-ahead term.**
- **The defect.** §5.3 "The fairness bound" says: "A waiter of either mode is granted once the holders present at its arrival have released, plus the bounded bypass". The ruling's bound was "after the holders present at its arrival release, **plus the waiters queued ahead of it**". Under any queued latch, whether task-fair, strict-FIFO or `parking_lot`, an exclusive waiter that arrives behind an earlier queued writer is granted only after that writer's hold too. Bypass counts only later arrivals, so the bound as written is false for every correct latch.
- **Why it matters.** It is the normative bound that A-3b's `t_root_latch_fair` budget is derived from.
- **The flip clause has the same gap.** "A gated writer blocked on a root delays a pending flip by at most one such hold" undercounts: the flip waits for every gated waiter queued on that latch, each behind the ones ahead of it.
- **Fix.**
  - Restore "and the waiters queued ahead of it" next to "the holders present at its arrival".
  - Bound the flip by "the holds ahead of it on that latch, each bounded as above (at most one per thread)".
  - Re-anchor G14 on both.

**R2 (blocking): G43's re-anchor is weaker than before, against the card's own rule ("the new pattern must be strictly stronger").**
- **What was lost.** The pre-review G43 pinned the clause "…, with the list's records for its entries applied." The review amendment moved the source chain into G50, and neither grep now pins that clause. GM1 drops it and passes every G. That clause is what makes image `k` hold the data-phase, claim and eviction records at all.
- **What was never pinned.** These core map-phase sentences sit inside a `.*` and have never been pinned:
  - GM4: "or that the list's records name". Without it, a MapLeaf that is unchanged since `k−1` but has listed entries is never rewritten, so image `k` loses those writes.
  - GM2: the fresh-slot and parent-buffer sentence.
  - GM3: "counts as written for image `k`" for map pages.
- **Fix.** Pin all four, together with matching B-mutants.

**R3 (fix in the same amendment): the PLAN block's A-3c and A-4c lines name the wrong tests.**
- **A-3c's replacement text.** It says "`t_step2_waits_for_claimed_copy` with its slot and map-copy variants", then lists six mutants that only the current-home, victim, source, namespace, adoption and allocation variants catch. Those six variant names appear only in a parenthetical outside the quoted replacement, so the applied row would not name them. Put them inside the quote.
- **A-4c's row.** The PLAN row at line 875 still says "`t_epoch_reserve_two_exhausters` with its deadlock variant (with A-3c, §5.6), failed by EXHAUST-DOUBLE-EVICT and EXHAUST-BLOCKING-CLAIM". The block only appends to that row, so the stale "deadlock variant" text survives, and the row never names the cross variant or CLAIM-LATCHES-MAP. Replace it the same way the A-3c row is replaced.
- **Ordering.** `t_latch_scope`'s map variant needs A-3c's map phase. State "after A-3c" on A-4c.

**R4 (minor, same amendment).**
- **(a) Pool floor.** "With no evictable frame it releases its latches and restarts (§5.2)" does not hold for a gated operation past its first mutation, because §5.6 forbids that restart. Its wait is finite only through §5.4's count. Say so, and keep the dependency on -F2-F2-F2.
- **(b) Who else holds the root during DropTree's last step.** "The root's only possible holder then is a stale-name thread's brief latch" leaves out two other holders:
  - step 2's data-phase latch, which is brief and then goes to the MapLeaf only;
  - an evictor's `try_latch`.
  
  Neither causes harm, but the sentence should list them.
- **(c) A map page's epoch copy after the map phase has taken it.** The text does not say whether the copy is released or can still be claimed. If it can be claimed, a late append for an already-built map page follows. Say that the copy is released, or that the late record is ignored and its slot released.
- **(d) Unpinned clauses.** GM5, GM6 and GM8 drop clauses that no grep pins. Pin them or accept that.

## Revert

Mutants were applied only to a scratch clone. The worker's worktree is untouched (`git status` is clean apart from this verdict).
