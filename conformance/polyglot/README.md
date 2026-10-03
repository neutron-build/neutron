# Polyglot conformance runner

This bounded harness establishes independent PostgreSQL fixture/oracle behavior.
The initial manifest is an **oracle self-check**, not evidence that a Neutron ORM
has passed polyglot parity. There are no synthetic Neutron adapters. Future
`adapter-read` cases must use actual packaged client APIs and return the exact
artifact identities supplied by the runner. Unsupported required cases fail.

Unit tests need only Python 3.11+:

```sh
python3 -m unittest discover -s conformance/polyglot/tests
python3 conformance/polyglot/runner.py --manifest conformance/polyglot/cases/scalars.json --validate-only
```

For live self-checks, install `psycopg[binary]>=3.2,<4` into an isolated test
virtual environment. Provide `NEUTRON_TEST_DATABASE_URL` through a private
process environment. The database principal needs permission to create and drop
schemas in a disposable test database. Never run against production.

Provide an artifact manifest via `--artifact-manifest` or
`NEUTRON_ARTIFACT_MANIFEST`. Its shape is
`{"files":[{"path":"relative/artifact","sha256":"64 lowercase hex digits"}]}`.
Paths resolve inside `--artifact-root` (default current directory); hashes must
match before any database mutation. Include actual adapter/package artifacts
and source evidence when qualifying clients. The runner does not infer that a
source hash is a published package hash.

```sh
python3 conformance/polyglot/runner.py \
  --manifest conformance/polyglot/cases/scalars.json \
  --artifact-manifest /private/path/artifacts.json --required
```

`NEUTRON_LIVE_REQUIRED=1` also requires a live database. Missing optional live
configuration returns `not-run`, never `pass`. Empty/unknown manifests fail.
Commands in manifests are trusted executable configuration, not user input.
Adapter subprocesses receive one JSON request on stdin and must emit one JSON
response on stdout. Timeout kills their process group. Credentials are never
part of request envelopes, arguments or runner results. Diagnostics redact the
configured URL and URI/password patterns; adapters must not emit other secrets.

Each case gets a random schema and independent ownership token. Setup uses
transactional CREATE (never IF NOT EXISTS) and a schema ownership comment.
Cleanup checks the token before dropping anything, even after an ambiguous
setup timeout. A cleanup failure fails the run and names the schema for manual
reconciliation. Database administrators can replace comments/objects; fixtures
assume no concurrent privileged object replacement. Abrupt runner termination
can leave schemas; it never causes cleanup of unowned schemas.

The native oracle imports psycopg, never Neutron. It reads exact decimal/int8
and timestamp text directly from PostgreSQL, with explicit UTC and ISO/YMD DateStyle, and compares
SQL NULL and JSON null separately. Its expected scalar record is independently
specified in the runner. This first case does not cover arrays, domains, write
omission, ORM lifecycle, or all cross-language permutations.

## Contract fixtures and native Python investigation

Validate the independently versioned expectations with:

```sh
python3 conformance/polyglot/validate_contracts.py
```

See `contracts/data/VALUES.md` and `EXECUTION_CONTRACT.md`. Validation checks
fixture structure, not runtime compliance.

The bounded native transport spike requires both `psycopg[binary]>=3.2,<4`
and `asyncpg>=0.29,<1` in the test virtual environment. With the same private
`NEUTRON_TEST_DATABASE_URL`, run:

```sh
python3 conformance/polyglot/spikes/python_driver.py --driver psycopg --mode sync
python3 conformance/polyglot/spikes/python_driver.py --driver psycopg --mode async
python3 conformance/polyglot/spikes/python_driver.py --driver asyncpg --mode async
```

It checks native typed scalar reads against an independent SQL-text oracle,
actual query cancellation and subsequent same-connection reuse, followed by
owned schema cleanup. It does not compare performance or certify ORM sessions,
write codecs, pool acquisition, or the existing Neutron compatibility adapter.
