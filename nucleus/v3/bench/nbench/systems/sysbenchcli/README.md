# sysbench 1.0.20 client container

Client-side load generator for row `oltp-sysbench-pg17`. Built locally
(`nbench-sysbenchcli:local`) from the pinned **Debian 12** base; sysbench
comes from Debian's repository — Debian's package is compiled with the
PostgreSQL (`pgsql`) driver, while Alpine's is not (verified
2026-10-18: alpine sysbench 1.0.20 rejects `--pgsql-host`; see the
Dockerfile). The tool version is recorded into the row's `versions.txt`
at init.

Licence: sysbench is GPL-2.0 — used as an unmodified standalone tool,
not a dependency; see `../../LICENSES.md`.
