# embedded-sqlite-speedtest1

SQLite 3.54.1 speedtest1, compiled from the sqlite-src-3540100.zip
amalgamation inside a container (sha256-verified against sqlite.org),
`--size 100000 --sha3`, a fresh database file per run, `--memory 24g
--cpus 16`. The SHA3 digest line in every run's log is compared across
runs; any disagreement voids the row.

Cold vs warm: speedtest1 always builds its own database, so "cold" is
the first run in a fresh container (cold binary/page cache) and "warm"
runs follow.
