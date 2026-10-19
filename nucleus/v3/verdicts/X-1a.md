Card: X-1a
Class: experiment
Card-gate: P
Verdict: REWORK
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: claude-code-a17a4137-46d8-4853-9e84-cedba2bafe1b
Date: 2026-10-18
Gate-exit: GATE EXIT=0
Commit: 7b0598c0
Mutants: RELEASE-BEFORE-VISIBLE -> S5 (final counter 205/820, want 1600; also S6a with a yield), RR-NO-CONFLICT -> S4, SP-ROLLBACK-PARTIAL -> S9 + write_set unit tests + model.rs, VISIBLE-JUMPS -> S11a gated sub-check + sequencer/commit unit tests + model.rs, GC-TOO-EAGER -> S12 + snapshots unit tests, O1-REF-UNIQUE-AT-SNAPSHOT (own: insert_unique checks liveness at the snapshot, not the newest committed version) -> UNCAUGHT, O2-REF-OWNROW-IGNORED (own: update_with ignores own writes) -> S1, O3-REF-GC-DROPS-NEWEST-LE (own: gc drains ..=newest_le) -> S12, O4-REF-EPQ-AT-SNAPSHOT (own: RC f sees the snapshot value) -> S4 + S5, O5-REF-UNIQUE-TOMBSTONE-LIVE (own: a committed tombstone counts as live) -> UNCAUGHT, O6-REF-RC-NO-NEXT-STMT (own: RC next_statement is a no-op) -> S3, O7-REF-RR-CHECK-BEFORE-LOCK (own: RR newer-version check runs before the lock wait only; f then applied to the snapshot value, i.e. an RR lost update) -> UNCAUGHT
Followups: X-1a-F1, X-1a-F2

## Findings

**Gate run.** `GATE EXIT=0` on 7b0598c0, every STEP EXIT=0, run in the
foreground with a 1500 s timeout (CARGO_TARGET_DIR=~/.cache/nv3-target/gate-X-1a,
`~/.cargo/bin` on PATH, no per-package decomposition needed). The mutants ran
in the scratch copy /tmp/opencode/x1agate with
`cargo test -q --release -p e1-mvcc --no-fail-fast` (300 s timeout, about 15 s
per run). Each one was reverted with `git checkout`. The card worktree is clean.

**Scope.** All 13 changed files are under `nucleus/v3/spikes/e1-mvcc/`. The
workspace picks the crate up through `spikes/*`. Nothing touches a protected
path. The dependencies are exactly the card's list.

**Hygiene.** No `unsafe` and no `unwrap`/`expect`/`panic!` outside
`#[cfg(test)]` (suite.rs included). Every pub item has docs.

**Oracle judgment: clean.** `RefEngine` is derived from the spec. Each rule
cites the C-T0 text, and I checked every citation against
`nucleus/v2/docs/C-T0-txn-protocol.md`:
- §3.2: commit-visibility window.
- §4: read at the snapshot.
- §5.1: I-WW, lock then check.
- §5.2: EPQ on the newest committed version.
- §5.3: unique liveness from the newest committed version, and an insert over
  a tombstone proceeds whatever its ts.
- §5.4: own-row rule, no newer-version check.
- §9.2: keep every version above W plus the newest version at or below W.

No candidate engine exists yet. The reference does not use `e0-btree`, and it
shares no code with v2. It shares only the `common` components, which the
card requires. So a bug in `common` would be mirrored in the oracle and is
invisible to S13. The scenario tests cover that gap: all five card mutants
are caught outside S13.

The one deviation from the bare spec is that `newest_visible` limits the
write path to versions at or below `visible_ts`. It is justified by §3.2 and
is equivalent under the commit order the code enforces (C-COMMIT-ORDER). It is
not tuned toward any engine.

**Why REWORK.** S13 on `RefEngine` compares the reference with itself.
Whatever the oracle does that S1–S12/S14 do not pin down becomes ground truth,
and S13 will force the arms to match it. Three plausible oracle or arm bugs
pass the entire suite.

1. **D1 (blocking): S4's blocked-RR shape is missing.** The card says "RR →
   40001 *after the blocker commits*". S4 instead commits T1 before T2's
   update, so it never waits. O7 checks for a newer version before the lock
   wait and writes over the snapshot value after it. That is an RR lost update,
   and it passes everything: S5 and S11 are RC, and S13 is NoWait. This is a
   check-then-wait race an arm could easily ship. The card forbids weakening
   scenarios.
   - Fix: add an S4 variant where the RR waiter starts the update while T1
     holds the lock, T1 commits, and the waiter must get 40001.
   - Add the RC counterpart too: blocked, then EPQ, then both increments
     counted.
2. **D2 (blocking): the C-T0 §5.3 unique rules are not pinned.** O1 (liveness
   read at the snapshot) and O5 (a tombstone counts as live) both survive. In
   S6a the waiter's `begin` races the holder's commit, so the snapshot-versus-
   newest distinction is never tested deterministically.
   - Fix: add deterministic cases:
     - an RR txn whose snapshot predates a committed live insert gets 23505;
     - an insert over a committed tombstone succeeds, including a tombstone
       newer than an RR snapshot;
     - `delete` then `insert_unique` in the same txn succeeds.
   - Begin S6's waiter before the holder commits. `Txn: Send` allows that.

The rest is sound. I read through the sequencer prefix rule, refcounted
snapshot registration under the registry mutex, the write-set undo log and
`merge_scan`, the lock-table claim/insert/re-check escalation handshake, and
the commit order with fail-stop poisoning, and found no defect.

## Follow-ups

**X-1a-F1: shared-component overhead skews the E1′ ratios.** These costs are
the same for both arms, so they add a constant to each arm's cost. That
pulls the arm (b)/(c) CPU ratio toward 1, which biases the ≥ 2× decision rule
toward (b):
- `release_all` runs `retain` over all 64 shards, so each commit costs
  O(every lock held by every txn), even though `by_txn` already lists the
  txn's keys.
- Every `acquire` takes the global `by_txn` mutex.
- Every `commit_with` takes the process-global `apply_gate` mutex, a test hook
  that sits on the production path.

Escalation also defers while any foreign row is held and then rescans every
shard on each later acquire. Under concurrency, a 10M-row UPDATE therefore
never escalates and pays O(N²). Fix all of this before X-2/X-3 measure:
release through the per-txn index, put the gate behind an atomic fast path or
a feature, and define escalation-under-foreign-holders (wait, or document
no-escalate).

**X-1a-F2: S13 coverage.** All four txns begin at the start of the schedule,
and `gc()` is never called. Add `gc()` calls between ops and txns that begin
mid-schedule. GC under several live snapshots across many keys is currently
covered only by S12's one key and one reader.
