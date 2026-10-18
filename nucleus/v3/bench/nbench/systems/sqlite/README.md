# SQLite (embedded)

Serves row: `embedded-sqlite-speedtest1`.

The engine under test is the SQLite amalgamation (3.54.1,
`sqlite-src-3540100.zip`), compiled inside a container built from the
pinned alpine base in `image.txt`; the tarball is verified against the
sha256 published on sqlite.org/download.html at build time.

Build flags (documented in speedtest1.c and the SQLite docs):
`-O2 -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_SHA3
-DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_OMIT_LOAD_EXTENSION`.

`--sha3` makes every run self-verifying: speedtest1 prints a SHA3 digest
of the data it produced, the harness compares that digest across all
runs, and any disagreement voids the row (PLAN-v3 §5.2 rule 4).

## Limits

Transient containers: `--memory 24g --cpus 16`, `--network host`,
artifacts in volume `nbench-sqlite-artifacts`.

## Licence / benchmark-publication check

SQLite is public domain (sqlite.org/copyright.html). No restriction; see
`../../LICENSES.md`.
