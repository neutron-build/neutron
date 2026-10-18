# SQLite (embedded)

Serves row: `embedded-sqlite-speedtest1`.

The engine under test is the SQLite amalgamation (3.54.1,
`sqlite-amalgamation-3540100.zip` + `sqlite-src-3540100.zip` for
`test/speedtest1.c`), compiled inside a container built from the pinned
alpine base in `image.txt`; both tarballs are verified at build time
against the SHA3-256 hashes published on sqlite.org/download.html.

Build flags (documented in speedtest1.c and the SQLite docs):
`-O2 -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_MATH_FUNCTIONS
-DSQLITE_OMIT_LOAD_EXTENSION`.

`--verify` makes every run self-verifying: speedtest1 prints a
`Verification Hash` line (accumulator + 24-byte hash over all query
results — 3.54.1 has no `--sha3` flag), the harness compares that line
across all runs, and any disagreement voids the row (PLAN-v3 §5.2
rule 4).

## Limits

Transient containers: `--memory 24g --cpus 16`, `--network host`,
artifacts in volume `nbench-sqlite-artifacts`, run databases in volume
`nbench-sqlite-data`.

## Licence / benchmark-publication check

SQLite is public domain (sqlite.org/copyright.html). No restriction; see
`../../LICENSES.md`.
