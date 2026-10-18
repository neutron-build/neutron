# embedded-sqlite-speedtest1

SQLite 3.54.1 speedtest1, compiled inside a container from the two
sqlite.org release zips (amalgamation for `sqlite3.c`/`shell.c`, src
tree for `test/speedtest1.c`; both verified against the SHA3-256 hashes
published on sqlite.org/download.html), `--size 100000 --verify`, a
fresh database file per run, `--memory 24g --cpus 16`.

`--verify` prints a `Verification Hash` line per run (an accumulator
plus a 24-byte hash over every query result; 3.54.1 has no `--sha3`
option). That line in every run's log is compared across runs; any
disagreement voids the row. Metrics: wall seconds from speedtest1's own
`TOTAL` line; user/sys CPU seconds from the busybox `time` wrapper.

Cold vs warm: speedtest1 always builds its own database, so "cold" is
the first run in a fresh container (cold binary/page cache) and "warm"
runs follow.
