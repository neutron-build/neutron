# Nucleus

Multi-model database engine in Rust. Single binary, no external dependencies at runtime.

## Key Facts

- FTS uses a **custom inverted index** (NOT Tantivy)
- Geo uses a **custom R-tree** (NOT H3)
- RESP protocol module at `src/resp/`
- KV fast path at `src/wire/kv_fast_path.rs`
- The executor (`src/executor/`) is the largest module; `mod.rs` alone is ~8K lines

## After Code Changes

Run `sh scripts/metrics.sh --check` to verify docs aren't stale. The check asserts
against **DATABASE_COMPLETION.md** ("Current baseline" section) only — update its
Source LOC / file / module / test counts when the check fails. Plain
`sh scripts/metrics.sh` prints the current values.

## Doc File Purposes

Tracked (release evidence):

- `README.md` -- Product overview + support-tier table for new visitors
- `DATABASE_COMPLETION.md` -- THE completion program: milestones, gates, evidence,
  and the metrics-checked baseline. Single source of truth for status claims.
- `RLS_SECURITY.md` -- Row-level-security model and adversarial coverage

Local-only (gitignored scratch; historical, NOT release evidence):

- `PLAN.md` -- Vision and architecture principles
- `STATUS.md` -- Informal current-state assessment (numbers go stale; trust
  DATABASE_COMPLETION.md)
- `NUCLEUS-ROADMAP.md`, `AUDIT-REPORT.md`, `TODO-NEXT.md`, `COMPETITOR-GAPS.md`,
  `AUDIT_FINDINGS.md`, `M*_TEST_PLAN.md` -- older planning/audit scratch

Compatibility harnesses: `compat/orm/` (Drizzle/Prisma/SQLAlchemy end-to-end,
`sh compat/orm/run.sh`); probes under `src/bin/` (`probe_soak --rows-target` for
scale, `bench_paired scale|sweep` for paired vector recall/latency).

## Build & Test

```sh
cargo build          # debug build
cargo test --lib     # run all tests (~3.8k)
cargo clippy         # lint
sh scripts/metrics.sh         # print current metrics
sh scripts/metrics.sh --check # validate docs match code
```
