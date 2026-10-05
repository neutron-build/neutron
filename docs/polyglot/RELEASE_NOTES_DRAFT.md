# Release notes draft — polyglot ORM and the Nucleus profile

Unpublished draft for review. Nothing here is announced, shipped or final;
every sentence is subject to coordinator review before any publication.

This draft summarizes what the polyglot ORM work (TypeScript, Go, Python) and
the Nucleus engine profile do and do not claim. Claims are scoped to recorded
evidence: each `Qualified` bullet ends with an evidence id of the form
`name @ revision`, which resolves to one recorded qualification run. In-tree
artifacts are cited by path and recorded revision. An id binds exactly the
revision printed with it — a row recorded at an earlier revision says nothing
about the current branch tip, and re-qualification at the release revision is
required before any line here ships. A capability with no evidence row is not
claimed. No behavioral-equivalence, fitness or readiness claim beyond the
recorded contracts is made anywhere in this document.

## TypeScript

`@neutron-build/sql` 0.1.0, wire drivers `pg` 8.22.0 and `postgres` (postgres.js) 3.4.8.

### Qualified

- Unit suites for `@neutron-build/sql` and `@neutron-build/nucleus` pass at
  revision b4f692a6 [finish-ts-full-18 @ b4f692a6]
- Installed-package qualification — package build followed by the conformance
  scalar-read oracle fixture — passes at revision b4f692a6
  [finish-ts-installed-10 @ b4f692a6]
- Built-package transaction, protocol and live PostgreSQL tests pass at
  revision a11cf864 [finish-ts-ownership-04 @ a11cf864]
- Wire-protocol scalar oracle self-check (runner with artifact manifest,
  required live case) passes at revision b62e7d24
  [finish-conformance-live-11 @ b62e7d24]
- Conformance runner unit tests and contract-fixture validation pass at
  revision 8dde7eff [finish-conformance-05 @ 8dde7eff]
- Per-driver ORM capability report recorded at source revision b62e7d24
  against engine tree f17ef40489ea: all 19 ORM-area probes (schema DDL,
  RETURNING writes, upsert, joins, grouped queries, CTEs, set operations,
  nested and to-one relations, keyset pagination, prepared statements,
  transaction rollback and nested savepoints, codec values, SQL NULL versus
  JSON null) are supported through both drivers
  [conformance/live/orm/ORM_CONFORMANCE.md @ b62e7d24, committed in 6210d770]

### Bounded

- The capability report is per-probe, not a blanket statement: 143 probes per
  driver, of which `pg` records 104 supported / 38 unsupported / 1 unknown and
  `postgres` records 103 / 39 / 1. A supported verdict covers its stated probe
  contract and nothing wider.
- Driver divergence is recorded and expected: `codec.text_array_param` is
  supported through `pg` and refused through postgres.js with SQLSTATE 22000.
- Scalar Text advertises wire OID 25 (TEXT), matching PostgreSQL, since
  814332c9 (before that revision it arrived as 1043/VARCHAR; strict clients
  verifying compiled-vs-wire type identity should qualify against a revision
  at or after 814332c9).
- The installed qualification covers the declared scalar corpus only; it does
  not cover migrations, relationships beyond the recorded probes, pooling or
  deployment topologies.

### Not claimed

- PostgreSQL behavioral equivalence beyond the recorded probe contracts.
- Universal datatype identity, higher isolation levels, transactional DDL,
  advisory locking or full catalog fidelity on Nucleus (see
  Known engine gaps).
- Any endurance or long-duration stability result (deferred; none recorded).

## Go

`github.com/neutron-build/neutron/go/orm`, pgx-based typed query core and
generator (`neutron-ormgen`).

### Qualified

- Full test suites for the `go` and `cli` modules pass at revision cfb5ebbe
  [finish-go-compile-10 @ cfb5ebbe]
- ORM suite under the race detector (including the live network tests against
  PostgreSQL) plus `go vet` pass at revision cfb5ebbe
  [finish-go-orm-race-04 @ cfb5ebbe]
- Generator suite (`neutron-ormgen`, orm and related packages) passes at
  revision c1e0578d [finish-go-generator-05 @ c1e0578d]
- Archived-module installed qualification — a standalone consumer of the
  module source archive built outside the origin — passes at revision b4f692a6
  [finish-go-installed-10 @ b4f692a6]

### Bounded

- The installed qualification consumes an archived module snapshot with
  pinned dependencies (`GOWORK=off`), not a published registry artifact; the
  consumer exercises the real `orm.Select` / `orm.InsertOne` APIs.
- The initial scalar adapter does not support NaN/Infinity, arrays or
  arbitrary custom codecs; those remain separate qualification gates.
- The cross-language runner covers the scalar write/read pairs it declares
  (int8 minimum, exact negative decimal, UTC microsecond instant, SQL NULL
  and JSON null) — not broad query, migration or association coverage.

### Not claimed

- A public Go module release or registry installation.
- PostgreSQL behavioral equivalence beyond the recorded suites.
- Any endurance or long-duration stability result (deferred; none recorded).

## Python

`neutron-framework` 0.1.0, `neutron.orm` package (sync and async), `orm` extra.

### Qualified

- ORM unit suite (`tests/test_orm*.py` via pytest) plus mypy over
  `neutron/orm` and the ORM type tests pass at revision 2df155e4
  [finish-python-24 @ 2df155e4]
- Installed-wheel qualification against the disposable PostgreSQL oracle
  fixture passes at revision b4f692a6 [finish-python-installed-03 @ b4f692a6]
- Installed lifecycle qualification — generated graph rollback, bound prewrite
  callbacks, relation identity reuse, known-commit callback failure
  classification, late constraint rollback, stream cleanup and explicit
  transaction lifetime, with independent SQL verification of final rows —
  passes at revision 414eef72 [finish-installed-lifecycle-01 @ 414eef72]

### Bounded

- The lifecycle qualification is a bounded installed corpus; it is not a
  whole-ORM statement and includes no timing claims (performance calibration
  is documented separately in `conformance/polyglot/performance/README.md`).
- The lifecycle row binds revision 414eef72, which predates the other
  recorded rows; it must be re-run at the release revision before shipping.

### Not claimed

- PostgreSQL behavioral equivalence beyond the recorded suites.
- Any endurance or long-duration stability result (deferred; none recorded).

## Nucleus

The engine itself, as exercised by the qualification lane.

### Qualified

- Engine prerequisite gate — locked lib tests with the `server` feature,
  clippy over all targets, no-default-features check, metrics and build-size
  checks — passes at revision 4fea515f
  [finish-rust-prerequisites-16 @ 4fea515f]
- Catalog and recovery probes — 40-iteration recovery runs and negative
  controls on both buffered-disk and durable-mvcc engines, plus session
  authority/guards/cancellation negative-control probes — pass at revision
  67ac93d7 [finish-rust-catalog-10 @ 67ac93d7]
- The engine probe suite (`scripts/probe.sh` at CI scale) passes at revision
  c1e0578d [finish-rust-probes-run-02 @ c1e0578d]
- Backend cancellation regression tests pass at revision 995c9535
  [finish-rust-cancel-02 @ 995c9535]
- Cancellation authority against a freshly built engine binary passes at
  revision 67ac93d7 [finish-nucleus-authority-10 @ 67ac93d7]; the PostgreSQL
  control for the same authority contract passes at revision f26f9bb7
  [finish-pg-authority-03 @ f26f9bb7]
- The engine SQL probe case set passes at revision 73a3c0ca
  [finish-sql-probe-06 @ 73a3c0ca]

### Bounded

- Every id above binds the revision printed with it; several predate the
  current branch tip and none transfers to a later revision without a fresh
  row.
- Recovery probes exercise process-kill restart on the recorded paths; they
  are not power-loss durability statements.
- Cancellation authority is measured for the recorded contract (authorized
  target binding, stale-target and cross-session negative controls) and does
  not extend to arbitrary in-flight statements.

### Not claimed

- The NP01 named Nucleus profile. `packageEnabled` remains false in every
  client. Admission source exists for TypeScript, Go and Python, and
  admission-qualifier rows are recorded for all three languages
  [finish-np01-ts-07, finish-np01-go-07, finish-np01-py-11, all @ 5f70b718];
  those rows bind admission behavior at that revision and confer no package
  support, enablement or release claim.
- Extended endurance qualification is deferred; no endurance run is recorded
  and no endurance result is claimed.
- PostgreSQL behavioral equivalence for the engine (see Known engine gaps).
- Native vector and FTS model APIs; SQL Pub/Sub subscription delivery; CDC
  replay or exactly-once delivery.

## Known engine gaps

Recorded in the ORM capability report
(`conformance/live/orm/ORM_CONFORMANCE.md` @ b62e7d24) and restated here;
these bound every claim above:

1. DDL is not transactional: catalog changes survive ROLLBACK and are visible
   before COMMIT; failed migrations leave earlier DDL applied.
2. Buffered storage supplies READ COMMITTED only; REPEATABLE READ and
   SERIALIZABLE are refused with 0A000.
3. Catalog fidelity is incomplete: relation-introspection boolean shape,
   CHECK definition rendering, regclass output and the scalar Text wire OID
   differ from PostgreSQL; partial indexes are refused rather than built.
4. Row-level-security predicates are restricted to a documented subset;
   `set_config` and `ALTER TABLE ... OWNER TO` are unavailable.
5. Advisory locks, `LOCK TABLE`, `pg_sleep` and `pg_cancel_backend` are
   unavailable; row-lock timeout behavior differs from PostgreSQL's 55P03.
6. `UPDATE ... FROM`, `DELETE ... USING` and row-value comparison are refused;
   NUMERIC has a 96-bit coefficient with at most 28 fractional digits and
   does not enforce `numeric(p,s)`; date infinity is rejected; enum ORDER BY
   is lexical rather than declaration order.
