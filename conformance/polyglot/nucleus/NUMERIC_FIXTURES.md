# Numeric fixtures: native recording and stale-outcome verification

The recorded numeric outcomes predate the numeric rework. The two
`mandatory-numeric-*` cases in `conformance/polyglot/nucleus/fixtures.json`
still record the range refusal (SQLSTATE `22000`) and the scale mismatch
(`1.5` vs `1.50`), and `codec.numeric_precision` in
`conformance/live/orm/capabilities.nucleus.json` plus its summary table in
`ORM_CONFORMANCE.md` repeat the same `22000` refusal. Two tools close that
gap without hand-editing recorded evidence:

- `record_numeric_fixtures.py` re-records observed outcomes natively
  (Linux, coordinator-run; needs psycopg3 exactly like the sibling
  `engine_sql_probe.py`).
- `verify_numeric_fixtures.py` (standard library only) fails while any of
  that stale `22000` numeric outcome remains.

Both were authored in a source-only environment: the verifier's self-test
runs anywhere, but recording requires the coordinator's Linux host, the
exact Nucleus binary and an owned PostgreSQL oracle.

## Verifier

    python3 conformance/polyglot/nucleus/verify_numeric_fixtures.py --self-test
    python3 conformance/polyglot/nucleus/verify_numeric_fixtures.py

The scan checks the five fixture files that carry the recorded outcome
(`fixtures.json`, `capabilities.nucleus.json`, `x05-nucleus-leg.mjs`,
`ORM_CONFORMANCE.md`, `python/tests/test_nucleus_models.py`) and exits 1
naming every file and line that still carries the stale numeric `22000`
outcome, 0 once none remains. A line counts only when it carries both the
`22000` token and a numeric marker (the word "numeric", the key
`historical_nucleus_sqlstate`, or the exact wide-range literal from the
recorded case). Streams `NOGROUP` (the `.mjs` leg and the Python test),
date infinity and array-literal parsing legitimately keep `22000`; those
lines carry no numeric marker and are not flagged. `--file PATH` scans a
candidate replacement file before it is committed; exit 2 means a listed
file was unreadable.

## Recorder

Linux only. The PostgreSQL oracle URL travels only through an environment
variable NAME; never put the URL itself in a command line, file or issue.

    export NEUTRON_NUMERIC_ORACLE_URL=<owned-postgresql-oracle-url>
    python3 conformance/polyglot/nucleus/record_numeric_fixtures.py \
        --binary <exact-nucleus-binary> \
        --postgres-url-env NEUTRON_NUMERIC_ORACLE_URL \
        --out <empty-path>/numeric-recording

The recorder reads the numeric statements already recorded in
`fixtures.json` (read only), starts the exact binary on free loopback
ports with a temporary data directory and a random bootstrap password,
proves the running image by SHA-256 of `/proc/<pid>/exe`, runs every
statement against Nucleus and against the oracle, and writes
`numeric-observations.json` into `--out`. `--out` must not exist; nothing
outside it is written, the recorded fixtures are never touched, and the
engine is stopped and its data directory removed on every path. A
completed recording exits 0 even with differences present: they are
printed as `DIFFERENCE <case>: ...` lines and counted in the summary JSON,
never hidden and never judged by the tool.

## Reviewing the recording before replacing anything

1. In `numeric-observations.json`: `status` must be `complete`, with no
   `runnerFailure`, `binarySha256` equal to `runningImageSha256`, and both
   `engineStopped` and `engineDataDirRemoved` true.
2. Read every `differences` entry; each class means something different:
   - `nucleus-changed-vs-recorded` /
     `nucleus-sqlstate-changed-vs-recorded` — the binary no longer
     reproduces the recorded historical outcome. Expected once the
     numeric rework is in the build.
   - `postgres-vs-nucleus` — the two engines disagree live. The rework is
     not proven by this binary; investigate instead of replacing.
   - `recorded-postgres-oracle-stale` / `postgres-oracle-error` — the
     live oracle disagrees with the recorded oracle value; investigate
     the oracle before touching the Nucleus side.
3. Replace the recorded outcomes file by file, keeping each file's own
   format and generator:
   - `fixtures.json`: update the two `mandatory-numeric-*` cases from the
     observed outcomes.
   - `capabilities.nucleus.json`: never hand-edit a status — regenerate
     with its own runner (`node conformance/live/orm/run.mjs --write
     capabilities.nucleus.json` with the engine URL in
     `NEUTRON_TEST_DATABASE_URL`) and review that diff separately.
   - `ORM_CONFORMANCE.md`: refresh the `codec.numeric_*` rows from that
     regenerated report.
   - `x05-nucleus-leg.mjs` and `python/tests/test_nucleus_models.py`:
     their `22000` recordings are streams `NOGROUP`, not numeric; no
     change is expected.
4. Re-run the verifier until it exits 0, then run the consumers of these
   files (the ORM live `--check` run and the Python SDK tests) before
   committing.

## Scope

The recorder observes; it does not judge parity, regenerate ORM capability
statuses, or edit fixtures. Agreement means both engines return the same
rows, or fail with the same SQLSTATE; a recorded historical outcome that
no longer reproduces is reported as its own difference class.
