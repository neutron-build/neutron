# Nucleus

Multi-model database engine in Rust. Single binary, no external dependencies at runtime.

## Key Facts

- FTS uses a **custom inverted index** (NOT Tantivy)
- Geo uses a **custom R-tree** (NOT H3)
- RESP protocol module at `src/resp/`
- KV fast path at `src/wire/kv_fast_path.rs`
- The executor (`src/executor/mod.rs`) is the single largest file (~26K lines)

## After Code Changes

Run `sh scripts/metrics.sh --check` to verify docs aren't stale.

If the check fails, update these files:

| Metric changed | Update these files |
|---|---|
| Added/removed `#[test]` | STATUS.md header, AUDIT-REPORT.md header, TODO-NEXT.md header, COMPETITOR-GAPS.md footer, NUCLEUS-ROADMAP.md line 4 |
| Added/removed module dir | STATUS.md header, NUCLEUS-ROADMAP.md line 4 |
| Added a WAL file | NUCLEUS-ROADMAP.md compliance table + persistence summary |
| Added a new data model | README.md, NUCLEUS-ROADMAP.md compliance table |
| Changed model implementation details | README.md model descriptions |

## Doc File Purposes

- `README.md` -- Product overview for new visitors
- `PLAN.md` -- Vision and architecture principles (rarely changes)
- `STATUS.md` -- Honest current-state assessment with per-module inventory
- `NUCLEUS-ROADMAP.md` -- Phased implementation plan with sprint details
- `AUDIT-REPORT.md` -- Bugs and security findings
- `TODO-NEXT.md` -- Prioritized gap list
- `COMPETITOR-GAPS.md` -- Feature parity checklist vs specialized DBs

## Build & Test

```sh
cargo build          # debug build
cargo test --lib     # run all tests (~2000)
cargo clippy         # lint
sh scripts/metrics.sh         # print current metrics
sh scripts/metrics.sh --check # validate docs match code
```
