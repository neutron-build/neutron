# Finite Nucleus ORM profile assessment

`nucleus-relational-rc-v1-candidate` is an **unenabled source assessment**.
Neither this document nor its fixtures certify PostgreSQL parity, current native
engine behavior, or the new polyglot ORM packages. The initial finite candidate
covers only the listed scalar CRUD and READ COMMITTED lifecycle shapes on the
server's buffered-disk path. The broader mandatory program remains represented
by explicit blockers; excluding a feature from this first candidate does not
complete or remove that requirement.

The assessed engine tree is `315b1720f6e2292e7c85e8fec4805886340e0ff8`.
The canonical historical TypeScript recording instead binds tree
`3c5c6035b5e6602cf65ba23c917fe83848888571`, source `59fad7ae...`, binary
`be4fc47554fd2be9d060d3221b14425b11db52dc3bfa0852e1be7943990e1c6a`,
pg 8.22.0/postgres.js 3.4.8 and Node 22.23.2. Its 143 per-driver probes cover their
individual contracts. They cannot be relabeled as this tree's qualification,
Python/Go evidence, or an arbitrary future binary's certificate. The original
report remains authoritative for its own recording and is not edited here.

## Emitted shape and engine mapping

| Current client shape | Source route / existing independent probe | Assessment boundary |
|---|---|---|
| Quoted schema/table + bound SELECT/WHERE/LIMIT | Go `orm/metadata.go:115`, `query.go:207`; Python `orm/core.py:87`; Nucleus wire parameter decoding and SQL parser; historical `orm.select_where_order_limit`, scalar codec probes | Candidate must use actual installed compiler output and hostile-search_path fixture. A source implementation or one parser sample does not prove every query. |
| Bound INSERT/UPDATE/DELETE + RETURNING; explicit zero/false/empty | Python `core.py:249`; Go `write.go`, TS builders; engine DML/RETURNING; historical DML/ORM probes | Candidate excludes UPDATE FROM/DELETE USING and universal graph policy. Exact affected counts and native row oracles required. |
| BEGIN READ COMMITTED / READ ONLY / SAVEPOINT / ROLLBACK TO / RELEASE | `nucleus/src/executor/txn.rs:495`, `txn_modes.rs`; historical `txn.read_committed_sees_commits`, `txn.read_only_rejects_writes`, both savepoint probes; `probe_sessions`, `probe_txn_atomicity` | Source supports bounded modes; buffered-disk refuses stronger isolation0A000. New Session/Scope lifetime, caught-failure poisoning and commit outcomes still need fresh clients. |
| Integer/bool/text/jsonb/timestamptz fields; binary pgx/psycopg and text Node results | `wire/mod.rs:3888` numeric decode, `:5220` temporal binary values, `:5276` result OID map, `:5405` JSONB header; `tests_row_description.rs`, `probe_decode_honesty`, `probe_types` | Candidate int8/temporal precision/NULL distinctions need native PostgreSQL controls and both wire formats. Nucleus Text returns VARCHAR1043 rather than PostgreSQL TEXT25; record named-profile difference, never scalar-OID parity. |
| Decimal/string exact numeric | `types/mod.rs:19` bounded rust_decimal, `:46` normalization; historical `codec.numeric_precision`22000 and `codec.numeric_unconstrained`1.50→1.5 | Mandatory blockerNP03. Initial candidate excludes numeric; do not loosen full scalar cross-writer or typemod requirements to make a run pass. |
| SET LOCAL / role and pooled reset | `executor/session.rs`, `ddl.rs:5035`; `probe_sessions --negative-control authority/guards`, historical RLS/session probes | Candidate only explicit local-state reset. RESET ALL dropping role is a documented fail-closed difference; no universal RLS/pooler profile certification. |
| Deadline through pg `pg_backend_pid`/`pg_cancel_backend`; postgres.js native CancelRequest | `scalar_fns.rs:2232` currently returns **process PID**; `wire/mod.rs:1762` allocates a **connection PID** and cancel secret; `:1282` races execution against cancellation | SQL cancellation bridge/target identity isNP02. Wire CancelRequest already exists. Historical cancellation probes fail on absent pg_sleep before proving cancel behavior; use a supported dispatched long query/lock wait and bystander controls. |
| Python named DECLARE/FETCH stream | `python/orm/streaming.py:89`; `executor/admin.rs:1272` eagerly materializes cursor rows; `executor/mod.rs:562` documents whole-result cursor memory | NP05: client batch1 does not establish bounded server memory. `SET stream_results=on` has a different path and may fall back; probes must report actual streamed rows. |
| Schema introspection, migration DDL/history, advisory lock, CIC | `executor/pg_catalog.rs`, `ddl.rs:1254` direct catalog registration; historical catalog/DDL/locks probes | NP04. DDL catalog rollback/visibility is not atomic; full introspection has bool/check/regclass differences; advisory locks absent and CIC-in-transaction refusal wrong. Autocommit fixture provisioning is not migration certification. |
| SDK connection admission | Python `endpoint.py` rejects Nucleus markers; TS `engine.ts` recognizes Nucleus and probes capabilities; Go `Executor` is caller supplied | NP01: deliberately add an exact named-profile contract while preserving postgres-direct rejection. Transport acceptance or a borrowed native executor is not approved package support. |

## Selected bounded work

`profile.json` lists NP01–NP06 with acceptance boundaries. NP02 is the narrowest
engine authoring candidate: bind SQL backend identity to the same session target
as BackendKeyData and implement authorized cancellation without broadening
storage/catalog semantics. It needs both driver regressions, stale target and
cross-session negative controls, errors and connection reuse. This assessment
does not authorize engine edits or claim that a fake pg_sleep result proves it.

Numeric range/scale and transactional catalogs require separately scoped engine
format/lifecycle work. Declaring them unsupported in this finite candidate
contains claims; it does not discharge their mandatory program requirements.
Likewise specialty stores, stronger isolation, arrays and full query algebra must
retain explicit contracts and separate exact-binary evidence before advertising.

## Source and artifact binding

The following is a read-only collection command for the coordinator:

```sh
python3 -I conformance/polyglot/nucleus/snapshot.py --source . --output /tmp/neutron-nucleus-assessment.json
```

An archived source tree additionally needs the coordinator-captured
`--nucleus-tree 315b1720f6e2292e7c85e8fec4805886340e0ff8`.
The script hashes every referenced source and contract fixture. It refuses
changed engine trees, package enablement or a fixture relabeled as executed.
Optional `--binary` plus `--binary-provenance` binds a real binary SHA to a JSON
build record containing `source_revision`, `nucleus_tree` and `binary_sha256`.
This reads bytes only; it does not execute the binary or certify its behavior.

## Native gate still required

Read [engine instructions](../../../nucleus/CLAUDE.md),
[probe instructions](../../../nucleus/docs/PROBES.md) and
[open audit boundaries](../../../AUDIT_OPEN.md) before authoring or running a gate.
The coordinator must freeze exact binary/dependency/config/client artifacts,
create owned isolated data directories and ports, then serialize native work on
compute-2. The existing start script deletes its supplied data directory and
also exposes additional model ports, so its default directory is not an owned
qualification fixture.

Run the documented Rust format/lib/clippy/core-only/metrics checks and required
independent probes after engine changes. The server production buffered-disk
path must be exercised; default MVCC fuzzing cannot substitute for it. Run
the ORM live suite against PostgreSQL 17 as control first, then the exact named
Nucleus binary through both Node adapters, and freshly installed Python sync/
async and Go clients once NP01 is implemented. Capture actual SQL/parameter
profiles, text/binary results, native state, faults, source/toolchain/binary
hashes and independent reviewer verdicts. Compatibility smoke harnesses that
SKIP missing tools are not mandatory polyglot qualification.

The documented `probe_recover_engines --skip-section catalog` holdout has expiry
2026-09-30 and is still present in source. At this assessment's 2026-10-03 date,
that requires current gate resolution or explicit remaining release blockage;
silently extending or counting skipped catalog coverage as passing is forbidden.
Historical SIGKILL recovery is not proof of power-loss durability. No native
gate, engine build, fixture execution, performance run or certification occurred
during this source assessment.
