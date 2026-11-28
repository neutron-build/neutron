# MySQL 8.4 (Oracle community image, InnoDB)

Serves row: `oltp-sysbench-mysql84`.

## Licence / benchmark-publication check (§5.2 rule 9 — done first)

Read 2026-11-27 from inside the pinned image itself:
`/usr/share/doc/mysql-community-server-minimal/LICENSE` ("Licensing
Information User Manual", MySQL 8.4.13 Community, last updated September
2026). The image's licence is GPLv2 (with the Universal FOSS Exception
for the client library). No clause forbids publishing benchmark results:
every "publish" occurrence in the text is the GPL's own distribution
terms or a third-party grant. Verdict: permitted — see
`../../LICENSES.md`.

## Tuning and its sources

Durability-matched to the PG row (PLAN-v3 §5.2 rule 3), per MySQL's
production guidance:

| Setting | Value | Source |
|---|---|---|
| `innodb_buffer_pool_size` | 6GB | MySQL Reference Manual, buffer pool sizing: ~25% of machine RAM (38 GiB host) — the same guidance `postgres17` follows for `shared_buffers` |
| `innodb_flush_log_at_trx_commit` | 1 | MySQL Reference Manual, InnoDB durability: the per-commit flush, the pair of the PG row's `synchronous_commit=on` (rule 3) |
| `skip-log-bin` | (set) | MySQL Reference Manual: binary logging off — replication is not this row's subject, and the setting is recorded here. The card named `--log_bin=0`, which MySQL 8.4 silently ignores (verified 2026-11-27: started with `--log_bin=0`, `@@log_bin` stayed 1); `--skip-log-bin` is the documented spelling and sets `@@log_bin=0` |
| port | 3307 | coexists with the other systems on the host network |

No variants: the row is durability-matched through the base args (the
per-commit flush), mirroring `oltp-sysbench-pg17`'s shape (no off
variant; the pairing is exercised by the pgbench row).

## Client

sysbench runs from the pinned `systems/sysbenchcli/` image with
`--db-driver=mysql` — Debian's sysbench 1.0.20 build carries the MySQL
driver as well as PostgreSQL (verified 2026-11-27 against the live
MySQL 8.4.13 container: `oltp_read_write prepare` and `run` both work
with `root`/`nbench`), so the card's source-build fallback was not
needed and `systems/sysbenchcli/` is untouched.

## Limits

`--memory 24g --cpus 16`, `--network host` (port 3307), volume
`nbench-mysql84-data`, env `MYSQL_ROOT_PASSWORD=nbench`,
`MYSQL_DATABASE=nbench`.

## Readiness

`mysqladmin -h 127.0.0.1 -P 3307 ping` via podman exec — over TCP with
the row's port, never the container's unix socket: the mysql
entrypoint's first-time init runs a temporary server with
`--skip-networking`, so a socket probe would report ready before the
real server listens (the `RespPing` lesson; see the probe unit test in
`src/runner.rs`).
