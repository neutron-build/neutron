# Gate verdicts (PLAN-v3 §7.6.1)

One file per card, `verdicts/<CARD-ID>.md`, committed by the gate onto the
card branch before it merges. `scripts/merge-card.sh` refuses a merge without
it, and `scripts/audit-merges.sh` re-checks every scaffold merge in CI.
Workers can never write here (the runner protects `verdicts/`).

Format: `key: value` lines, exactly these keys, then free text.

```
Card: <ID>
Class: spec | core | standard | test | experiment | bench | mechanical
Card-gate: P | S                 (copied from the card's Gate: line)
Verdict: MERGE | MERGE-WITH-FOLLOWUPS | REWORK | ESCALATE
Gate-tier: P | S                 (P = Opus, S = GLM)
Gate-model: <model id, e.g. claude-opus-5-5>
Gate-session: <T3 task id / agent id of the gating session>
Date: <YYYY-MM-DD>
Gate-exit: GATE EXIT=0           (the literal last line of scripts/gate.sh on the branch tip)
Commit: <full 40-hex id of the card commit the verdict covers, i.e. tip^ (required)>
Mutants: <name -> catching test | UNCAUGHT>, ...   (or "n/a")
Followups: <card ids, comma-separated> | none
```

Rules: an orchestrator never gates a card it authored, nor any spec edit; a P
card needs a P verdict; REWORK and ESCALATE verdicts are committed too (they
block the merge and keep the history).

**Trust limit.** Verdict authorship is not cryptographically authenticated: a
verdict file written by an orchestrator that claims `Gate-tier: P` is detectable
only by review (the `Gate-session` id must match a real T3 task or agent run).
In audit mode `Card-gate` is taken from the verdict, because cards are
local-only. Periodic Opus reviews spot-check `Gate-session` ids against T3.

**Commit is the full 40-hex id (X-6-F1-F2-F3-F1-F1).** `merge-card.sh`
refuses any other form; gates write it in every verdict, REWORK and ESCALATE
included. A short id had to
name exactly one commit among the objects present where the check ran, so a
commit ground to share the reviewed prefix was refused as ambiguous only while
the reviewed commit was still there. Deleting that unreachable object (any
same-uid worker can) left the ground commit alone with the prefix, and a fresh
CI clone never holds it; no hash check helps when the object is gone. A full id
cannot collide.

The merges made before this rule keep their short ids: `SHORT_COMMIT_MERGES` in
`scripts/enforce-lib.sh` lists them (44, from S-0's `ac4ff9a0` to X-5's
`266b8f4c`), and `audit-merges.sh` judges only those under the old 7-40 hex
rule, ambiguity count included. The list is keyed by the merge commit's own
full id, not a date (text a writer chooses) or an ancestry cutoff: that id pins
the merge's parents and so the verdict blob at its second parent, all of which
the audit hash-verifies, and no new merge can carry a listed id. The list is
frozen; its comment gives the command that re-derives it. A short-id merge made
after it fails the audit, and merge mode never consults it. What remains is
verdict authorship: whoever can bypass `merge-card.sh` can also commit a
verdict naming an unreviewed commit's full id, which is the trust limit above.
