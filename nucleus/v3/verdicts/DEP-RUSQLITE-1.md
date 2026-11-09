Card: DEP-RUSQLITE-1
Class: test
Card-gate: P
Verdict: MERGE
Gate-tier: P
Gate-model: claude-opus-5-5
Gate-session: Claude Code session 7edf4f8c-cfdd-48c3-b0df-35aed0283a08
Date: 2026-11-09
Gate-exit: GATE EXIT=0
Commit: 648a0e5fd1ef94d1f269c64ecd7ebcd00bac0bd1
Mutants: GX-DEP-037 -> cargo deny check advisories (rc 1, RUSTSEC-2026-0459), GX-DEP-040 -> cargo deny check advisories (rc 1, RUSTSEC-2026-0459)
Followups: none

## Verdict in one paragraph

MERGE. The card does what the owner decided on 2026-11-09: `rusqlite` moves from 0.37 to 0.41, `libsqlite3-sys` resolves to 0.39.1, RUSTSEC-2026-0459 stops firing because the dependency moved, and `deny.toml` is untouched. The fresh gate at the tip is green. The whole tracked change is one line.

## Gate run

- Scaffold `187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`, extracted through the `--no-local` shallow clone. No `EXTRACT FAILED`, no `FRESH-GATE REFUSED`.
- `fresh-gate: commit 648a0e5fd1ef94d1f269c64ecd7ebcd00bac0bd1`; `fresh-gate: base 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b`; `fresh-gate: checks passed (4525 files hashed in 6.74 s)`; `fresh-gate: 2 file(s) matched through their eol conversion`.
- Steps: test 0, clippy 0, fmt 0, deny 0, wasm32 0, core-paths-selftest 0, enforcement-tests 0. Literal last line: `GATE EXIT=0`. Stderr was empty.
- The worker's one red, the ruled `runner e2e: K` row inside its container, did not reproduce here.

## Scope

- `git diff 2b4c82b4..648a0e5f` is one line in `spikes/e0-btree/Cargo.toml`: the version string on the `rusqlite` line, `"0.37"` to `"0.41"`. `features`, `optional` and every other line are as they were.
- `5aa9fcdc` and `648a0e5f` are empty record commits. The RESULT is in the messages of the three commits.
- `deny.toml` is unchanged and still reads `ignore = []` under `[advisories]`.
- Nothing changed under `spikes/e4-recovery/`, `spikes/e0-btree/src/`, any `tests/`, `examples/` or `results/`, or `spikes/e0-btree/README.md`.

## Lock

`nucleus/v3/Cargo.lock` is ignored by `.gitignore:153` and untracked, so it is not in the diff and every extract resolves its own. I therefore checked the card's Limit 1 by reconstruction on clean exports (`nucleus/v3` and `nucleus/v2`, the latter for the E1 path dependencies):

- Lock generated at `2b4c82b4`, then the one-line bump, then `cargo update -p rusqlite`. Cargo reported exactly: `rusqlite 0.37.0 -> 0.41.1`, `libsqlite3-sys 0.35.0 -> 0.39.1`, `hashlink 0.10.0 -> 0.12.2`, `foldhash 0.1.5 -> 0.2.0`, removing `hashbrown 0.15.5`, adding `rsqlite-vfs 0.2.0` and `sqlite-wasm-rs 0.6.2`. That is Limit 1's list and nothing else.
- In the full diff, the only other hunks are `hashbrown 0.17.1`'s entry merging with the removed one and `indexmap`'s dependency line losing its version disambiguator (`"hashbrown 0.17.1"` to `"hashbrown"`). Neither is a version move.
- A lock generated from scratch at the tip is byte-identical to the bumped one, and so is the worker's lock in the worktree.

## The advisory clears for the right reason

- At the tip, `cargo deny check advisories` prints `advisories ok`, rc 0, and `cargo deny --log-level error check` prints `advisories ok, bans ok, licenses ok, sources ok`, rc 0.
- `cargo tree --all-features -i libsqlite3-sys --target all` shows `libsqlite3-sys v0.39.1` under `rusqlite v0.41.1`, reached only through `e0-btree`. The advisory's `patched` range is `>= 0.39.0`.
- The check still fires on an affected version. See the two controls below.

## Lowest qualifying version

Confirmed from crates.io's sparse index on 2026-11-09, independently of the worker's probe:

| rusqlite | libsqlite3-sys requirement |
|---|---|
| 0.37.0 | ^0.35.0 |
| 0.38.0 | ^0.36.0 |
| 0.39.0 | ^0.37.0 |
| 0.40.0 | ^0.38.0 |
| 0.40.1 | ^0.38.1 |
| 0.40.2 | ^0.38.1 and ^0.38.2 |
| 0.41.0 | ^0.39.0 |
| 0.41.1 | ^0.39.0 |

No release is yanked. 0.41.0 is the lowest that qualifies, as the card claims, and `"0.41"` resolves 0.41.1.

## Acceptance on a clean export of the tip

- `cargo test -q --release --locked -p e0-btree --features sqlite`: rc 0, 18 passed, 0 failed. The sqlite-gated bench code compiled against 0.41 with no source change.
- `cargo test -q --release --locked -p e4-recovery`: rc 0, 276 passed, 0 failed.
- `scripts/gate.sh`: the fresh gate above.

## E4 digests cannot move

No `e4-recovery` file changed. `cargo tree -p e4-recovery -e normal,dev,build`, with packages and features, is identical between the two locks, for the host target and for `--target all`. It contains no `rusqlite`, `libsqlite3-sys`, `hashlink` or `foldhash`. As the card requires, no `e0-bench`, measurement, campaign or `verify` ran.

## Mutants

The card names none. A one-line dependency card has no logic to mutate, so my two check that the gate's own control still bites, each on its own copy of the tip with a freshly generated lock.

| Mutant | Resolved | Result |
|---|---|---|
| GX-DEP-037 (requirement back to `"0.37"`) | `libsqlite3-sys 0.35.0` | caught: `advisories FAILED`, RUSTSEC-2026-0459, rc 1 |
| GX-DEP-040 (`"0.40"`, the highest version below the card's) | `libsqlite3-sys 0.38.2` | caught: `advisories FAILED`, RUSTSEC-2026-0459, rc 1 |

## Notes, no action required

- Because the lock is untracked, the `deny` step sees whatever the index resolves on the day. That is how this advisory arrived and is outside this card.
- The E0/E1 SQLite comparison numbers stay as recorded, per Limit 5. They were measured on the old bundled SQLite, so any future re-measurement runs on a different SQLite and is not directly comparable.
