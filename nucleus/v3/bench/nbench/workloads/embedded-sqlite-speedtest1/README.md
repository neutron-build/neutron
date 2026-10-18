# embedded-sqlite-speedtest1

SQLite 3.54.1 speedtest1, compiled inside a container from the two
sqlite.org release zips (amalgamation for `sqlite3.c`/`shell.c`, src
tree for `test/speedtest1.c`; both verified against the SHA3-256 hashes
published on sqlite.org/download.html), `--size 10000 --verify`, a
fresh database file per run, `--memory 24g --cpus 16`.

The size is bounded, not arbitrary: speedtest1's test 140 concatenates
every `z1` row matching a LIKE pattern, `z1` holds `5 * size`
number-name rows of at most ~75 bytes (speedtest1.c `testset_main`;
`maxb` is `roundup_allones(5 * size)`, so names stay short), and
SQLite's documented default `SQLITE_MAX_LENGTH` is 1e9. At
`--size 10000` (5M rows) even a match-everything concatenation stays
~3x under the limit — the bound is pinned by a unit test in
`src/params.rs`. The first attempt used `--size 100000` (50M rows) and
died in test 140 with `Error code 18: string or blob too big`
(2026-10-18); raising the compile-time limit instead would mean a
non-default SQLite build, i.e. tuning the competitor past its
documented defaults.

`--verify` prints a `Verification Hash` line per run (an accumulator
plus a 24-byte hash over every query result; 3.54.1 has no `--sha3`
option). That line in every run's log is compared across runs; any
disagreement voids the row. Metrics: wall seconds from speedtest1's own
`TOTAL` line; user/sys CPU seconds from the busybox `time` wrapper.

Cold vs warm: speedtest1 always builds its own database, so "cold" is
the first run in a fresh container (cold binary/page cache) and "warm"
runs follow.
