# Live data-model conformance

The HTTP suite one directory up proves each SDK serves the same *framework*
contract. This one proves each SDK speaks the same *database* — over pgwire,
against a running engine, for all fourteen Nucleus data models.

It exists because on 2026-08-11 a manual sweep found eighteen call shapes that
had **never worked** from Python: `Document.get`, `Graph.neighbors`,
`Graph.shortest_path`, `CDC.read`, `TimeSeries.range_count`/`range_avg`,
`Streams.xrange`/`xread`, `Blob.get`/`meta`, `Datalog.query` and the KV range
reads. Every one of them had a green test. The tests were mocked, and the first
line of `test_nucleus_models.py` says so: *"mocked — no real DB required."*

A mock proves the client builds the SQL the test expects. It cannot know that
the server describes that statement with zero result columns, or types a
parameter as TEXT that the client binds as an integer. Those are the failures
that actually happen. The engine's own testing is not the gap — 4,248 unit
tests, 347 integration tests, a differential fuzzer at zero divergences against
SQLite, 49 probes. Every one of those bugs lived at the client boundary, where
nothing looked.

## Why a spec instead of seven test files

Six hand-written ports drift the moment one of them answers an encoding question
the others do not. So the cases live in [`spec.json`](spec.json) as data, and
each SDK ships a thin executor mapping op names onto its own client. Adding a
case is one edit and all seven pick it up.

The cross-SDK behavioural diff falls out of the same structure: run one spec
through seven executors, compare the recorded statuses, and any disagreement is
drift by construction rather than by inspection.

```
conformance/live/
  spec.json                     the cases — the only place behaviour is specified
  runner/run.mjs                runs every available executor, prints the matrix, fails on drift
  executors/<sdk>/              one per SDK; maps op names to that SDK's client
```

## Running it

```bash
# start an engine
nucleus start --data /tmp/nucleus-live --port 55432

export NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres
node runner/run.mjs                 # every executor that exists
node runner/run.mjs python go       # only these
```

Without `NEUTRON_TEST_DATABASE_URL` the run **fails** rather than skipping. A
suite that silently skips is the same as no suite, which is exactly how the
Python live tests came to never run at all.

Note for local runs: Nucleus goes read-only below a 3% free-disk watermark and
reports it clearly. If most of the suite fails with `DiskFullError`, that is the
engine protecting itself, not a conformance failure.

## Statuses

| status | meaning |
|---|---|
| `pass` | behaved as specified |
| `fail` | did not — the run fails |
| `xfail` | expected to fail, and did; carries a reason in the spec |
| `xpass` | marked `xfail` and **passed** — the run fails, because the note explaining the expected failure is now false |
| `unsupported` | the SDK has no surface for the op, and says so in its own `unsupported.json` with a reason |
| `absent` | no executor for that SDK — unproven, never counted as agreement |

`xpass` failing is deliberate. Twice already a known-broken note here has been
stale, and a suppression that cannot expire becomes the blind spot it was
written to document.

## What the first run found (Python, 2026-08-13, against current `main`)

Three findings that were **not** in any existing list:

1. **`SELECT $1::jsonb` describes zero result columns and returns one.** Also
   `::json`, `::uuid` and `::bytea`. A strict extended-protocol client —
   asyncpg, and by construction pgx and JDBC — aborts with a protocol error.
   This is the same defect class as the eighteen never-worked shapes, except it
   is plain SQL rather than a Nucleus extension, so it is a hole in *"any
   PostgreSQL client works"*. `psql` does not catch it: it does not validate the
   described column count against the row it receives.

2. **`SELECT $1::int` returns the raw big-endian bytes of the integer as text.**
   No error. The caller receives `'\x00\x00\x00\x01'` where it asked for `1` —
   a silent wrong answer, which is worse than the protocol error above.

3. **`VECTOR_DISTANCE` in a select list returns zero rows.** `SELECT id FROM t`
   returns two rows; `SELECT id, VECTOR_DISTANCE(embedding, VECTOR('[1,0,0]'),
   'cosine') FROM t` returns none, on the same table, over the simple protocol
   in `psql`. Vector similarity search has never returned a result through the
   SDK, and vector is a model this project publishes recall and latency claims
   about.

4. **`Streams.xack` cannot consume `Streams.xadd`'s output.** `xadd` returns
   `'1786606665529-0'`; `xack` takes `(id_ms: int, id_seq: int)`. The two halves
   of the consumer-group API do not compose.

And two existing findings that turned out **stale**, which is the other half of
the point:

- The comma-split bug recorded as CONFIRMED across four SDKs does not reproduce
  in Python on current `main`. Tested with the discriminating shape — two
  members, one containing a comma, so a naive split yields three — `lrange`,
  `smembers` and `hgetall` all return both members intact.
- `L3` says `KV_LRANGE` and `KV_ZRANGE` both read `-1` literally. `LRANGE` was
  fixed. `ZRANGE` was not, and separately joins member and score with `:` into
  one string, so a member containing `:` is ambiguous. The note was half right
  and pointed at the wrong function.

## Adding a case

Add it to `spec.json`. If an op is new, add one method per executor — they are
deliberately a flat switch with no cleverness, so the mapping stays auditable.
Do not add an assertion to one SDK's executor: an assertion that lives in an
executor is drift waiting to happen.
