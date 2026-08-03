# Archived Nucleus docs

Superseded documents. **Do not use anything here as a work list.** Every claim
predates the current engine and several were verified fixed before archiving.

For current state: `../DATABASE_COMPLETION.md` (canonical status),
`../docs/PROBES.md` (how to find bugs), `_internal/OPEN_WORK.md` (what to do next).

Archived 2026-08-02, when `nucleus/` root went from 18 markdown files to 7. The
problem was not the count — it was that a fresh session could not tell which of
four audit documents and five status documents were live.

## Why each was archived

### The audit cluster — findings already fixed, presented as open

**`AUDIT-REPORT.md`** (2026-02-18) — ~26 numbered findings under headings like
"CRITICAL BUGS (Will Crash or Corrupt Data)", with **no status markers of any
kind**. Sampled four against current source before archiving:

| Finding | Claim | Reality |
|---|---|---|
| #1 | `accept().expect()` crashes server | fixed — no such call in `main.rs` |
| #2 | `.expect("column not found")` panics | fixed — no such call in `executor/mod.rs` |
| #12 | authentication is NEVER enabled | fixed — `main.rs` refuses non-loopback without auth |
| #17 | RLS/masking not wired into executor | fixed — RLS is in the executor security path |
| #25 | blob store uses FNV-1a | fixed — no FNV in `src/blob/` |
| #6 | `frame_data_mut()` unsound aliasing | addressed — explicit SAFETY CONTRACT, `write_guard` preferred |

Six for six. A document where every sampled critical is already fixed, and which
offers no way to tell that, sends a session chasing ghosts.

**`NUCLEUS-AUDIT.md`** (2026-03-04) — 19 items still marked `- [ ]`. Roughly half
are stale, and the ones that remain carry numbers that are all now wrong:

| Item | Claimed | Actual (2026-08-02) |
|---|---|---|
| executor monolith | 27.4K lines | 9,082 (split into `query.rs`/`policy.rs`/`ddl.rs`/`helpers.rs`) |
| LATERAL JOIN missing | not implemented | implemented (`executor/query.rs`) |
| RLS / `CREATE POLICY` | not implemented | implemented (`executor/policy.rs`) |
| unwrap/expect | 262 | 6,325 (codebase grew; the number never meant much) |
| `allow(dead_code)` | 38+ | 79 |
| `env::set_var` in tests | 16 | 37 |

Its two genuinely-open items were checked as tracked elsewhere before archiving:
KV WAL errors swallowed is **NU-037** in `_internal/AUDIT_NUCLEUS_FINDINGS_V3`,
and result-set buffering is `_internal/STREAMING_EXECUTION_PLAN.md`. Nothing
unique was lost.

**`AUDIT_PLAN.md`** (2026-06-05) — the working plan for a completed audit
session. Its findings live in `../AUDIT_FINDINGS.md`.

### The status cluster — five documents, one live tracker

`../DATABASE_COMPLETION.md` is the canonical one and is machine-checked by
`sh scripts/metrics.sh --check` (60 open / 75 done at archival). The rest:

- **`TODO-NEXT.md`** — **21 of 21 items complete.** A file named "TODO Next"
  containing no next work. Its "What Works Well" capability inventory was the
  only live content.
- **`STATUS.md`** — carried the most widely-copied wrong numbers (~3,850 declared /
  3,836 run / 50 modules / ~252k LOC, against a true 4,216 / 4,188 / 51 / 289,704).
  Its own header already said not to trust it.
- **`NUCLEUS-ROADMAP.md`** — prose roadmap, no tracked items, quoted 2,611 tests.
- **`PLAN.md`** (2026-02-18) — original vision. Superseded by
  `DATABASE_COMPLETION.md` for status and `_internal/BLUEPRINT.md` for strategy.

### Handoffs and raw research

- **`HANDOFF_INDEX_MAINTENANCE.md`** — session handoff pointing at a worktree
  (`Neutron-RLS/`) and branch (`fix/nucleus-value-type-consistency`) that **no
  longer exist**. Its own body says the stream is complete. Kept because its
  "Landmines discovered" section is still worth reading before touching index
  maintenance.
- **`COMPETITOR-GAPS.md`** — 15/15 complete. Superseded by `../PARITY-COMPARISON.md`.
- **`competitive-analysis-raw.md`** (294 KB) and **`ai-ml-database-research.md`**
  (97 KB) — raw research dumps from 2026-02. Source material, never conclusions.

## The rule this leaves behind

`nucleus/` root holds seven documents and each answers a different question:
`README` (what is it), `CLAUDE.md` (agent context), `DATABASE_COMPLETION.md`
(status — canonical), `DURABILITY.md` and `RLS_SECURITY.md` (evidence),
`PARITY-COMPARISON.md` (competitive), `AUDIT_FINDINGS.md` (findings log).

Adding an eighth is fine when it answers a question none of those do. Adding a
second status document, a second next-work list, or an undated audit snapshot is
what produced this folder.
