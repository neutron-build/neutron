# Installed ORM calibration and characterization

This runner compares each new ORM with its corresponding native driver:
TypeScript pg and postgres.js, Python psycopg sync and async, and Go pgx.
It does not claim cross-language equivalence of runtime costs or leadership.
There is no competitor result: a comparable pinned competitor corpus must be
qualified separately before adding one.

`profile.json` freezes the configuration and gates before execution. Each
process uses one connection, no prepared statements, 1,024 warmup iterations and
2,048 measured iterations. The read workload selects all three columns by a
primary key cycling through 64 rows. The transaction workload performs that
read and one text UPDATE inside a READ COMMITTED transaction. Every read checks
the exact bigint (above JavaScript's safe integer range), and an independent
psycopg oracle verifies the entire table after every sample. Checks and checksum
calculation are included in the measured loop for both raw and ORM consumers.
Connection creation, package loading and warmup are excluded.

Query counts include control statements. Python counts data statements at the
native cursor and adds two controls per successfully completed native
transaction context; this is not a server trace. Go traces pgx dispatches;
TypeScript counts pg client calls or postgres.js debug dispatches. Extra
dispatches refuse the sample. Memory reports retain their runtime-specific
meaning and units; they are not comparable allocation measurements across
languages. Linux RSS includes the warmed process, imports, and native driver.

First run the existing outside-origin qualification scripts against the
integrated source, serially on compute-2. They must succeed before preparation:

```sh
python3 -I conformance/polyglot/qualify_python.py --source python --work-directory /tmp/neutron-python-perf-client
python3 -I conformance/polyglot/qualify_typescript.py
python3 -I conformance/polyglot/adapters/go/qualify.py
```

The latter two print their unique owned consumer directories. TypeScript's
package build must already be complete. Keep the database URL in the inherited
private `NEUTRON_TEST_DATABASE_URL`; never put it in argv or evidence. Substitute
the actual consumer paths and exact integrated/source-archive revision below:

```sh
python3 -I conformance/polyglot/performance/prepare.py --source . --revision REVISION --python /tmp/neutron-python-perf-client --typescript TS_CONSUMER --go GO_CONSUMER --output /tmp/neutron-perf-consumers.json
python3 -I conformance/polyglot/performance/runner.py --consumers /tmp/neutron-perf-consumers.json --output /tmp/neutron-perf-calibration.json
```

The preparation step builds the Go performance executable on the host. It
preserves the original scalar manifests and adds separately verified manifests
for performance source, binaries, installed packages, toolchain and profile.
The campaign verifies all five clients' manifests before fixture mutation.
Each workload first gets a raw and ORM correctness preflight; only subsequent
four raw A/A pairs contribute to calibration. Each sample retains host load,
CPU counters (including steal on Linux), available memory, PostgreSQL settings
and raw measurements. A host lock prevents simultaneous copies of this runner;
the coordinator must serialize other workloads separately.

Exit 2 means noise or minimum-duration gates refused calibration. Retain this
evidence. Do not tune gates after seeing a campaign or discard failing samples.
Revision 1 used 64 warmup and 256 measured iterations. Its first full native
calibration passed all correctness preflights but refused the TypeScript pg
and Python sync read median-drift gates. That report remains evidence; it cannot
approve characterization. Revision 2 increases warmup and measured work before
another campaign, retaining every numeric acceptance limit. Longer samples are
an attempt to reduce transient variation, not proof that the host is stable.
All consumer bytes and installed provenance must be qualified again; results
from these two profiles are separate campaigns.


A separate reviewer must accept the passing calibration report and produce
`review.json` with `calibration_sha256`, `reviewer`, and `verdict: "accepted"`.
The runner never generates this approval. Characterization requires the exact
same descriptor, installed artifacts and frozen profile:

```sh
python3 -I conformance/polyglot/performance/runner.py --phase characterize --consumers /tmp/neutron-perf-consumers.json --calibration /tmp/neutron-perf-calibration.json --review /tmp/review.json --output /tmp/neutron-perf-characterization.json
```

Four ABBA rounds retain all raw/ORM samples, median ratios and raw bracket drift.
Failures remain in the report. The shared VM can still suffer neighbor noise;
passing these bounded gates establishes only this small workload's observed
characterization. There is no soak, no automatic publication, and no acceptance
of performance leadership.

Coordinator checks before dispatch: run
`python3 -I conformance/polyglot/performance/test_runner.py`, compile the Go
consumer through preparation, and run both live correctness preflights. No
campaign or test has been executed by the author during implementation.
