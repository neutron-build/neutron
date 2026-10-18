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
Commit: <card branch tip the verdict covers>
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
