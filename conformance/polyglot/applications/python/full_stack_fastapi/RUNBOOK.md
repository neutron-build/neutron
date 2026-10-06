# PY-APP native comparison runbook (coordinator only)

Status: authored. Nothing here has been executed. The converted application, the derived
tests, the scenario, the catalog check and the runner were written and statically checked
only (`ast` parsing, the inventory and source verifiers). A first native run may expose
compile-time or runtime defects in them; report those as defects of the authored files,
never work around them by weakening a check. The original template is ground truth: a
disagreement means the converted code, the SDK or the authored harness is wrong.

## Preconditions

- Python 3.14 (the template requires `>=3.14` and uses PEP 758 syntax), `uv`, `git`, and
  network access to GitHub for the fetch and to a package index for `uv sync`.
- A disposable PostgreSQL (12+; 14+ recommended) whose role can `CREATE SCHEMA` and
  `DROP SCHEMA` in one database and can create the trusted `uuid-ossp` extension if it is
  not already installed (the template's Alembic revision `d98dd8ec85a3` runs
  `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`). Set the URL privately in
  `NEUTRON_PY_APP_DATABASE_URL`; never echo it, put it in a command line, a log or a report.
  It must not already contain an `options` query parameter. Direct connection, no pooler.
- `SDK_ROOT`: the repository root of the integrated SDK artifact under test (it provides
  `python/`). Record its `git rev-parse HEAD` and cleanliness.
- Serialized lease; nothing here uses ssh.

## 1. Static verification (any machine)

```sh
cd conformance/polyglot/applications/python/full_stack_fastapi
python3 verify_source.py          # 36 frozen template files, bytes + SHA256
python3 verify_inventory.py       # authored inventory, rewrites, parity, fail-closed, parse
```

`verify_inventory.py --update` is an authoring step; the coordinator only verifies.

## 2. Prepare both trees (fetches public source, executes nothing)

```sh
EVID=/fresh/private/evidence/py-app-NN        # must not exist
python3 prepare_original.py "$EVID/original"
python3 prepare_converted.py --original "$EVID/original" "$EVID/converted"
```

`prepare_original.py` fetches the pinned commit at depth 1, requires `HEAD` to equal the
pin and a clean tree, re-verifies all 40 frozen files (36 plus the 4 hash-only ones) and
adds only `backend/tests/neutron_corpus/{__init__,test_neutron_scenario}.py`; hashes are in
`reconstruction.json`. `prepare_converted.py` copies that tree, overlays the authored files
from `converted/`, applies the exact rewrites to the frozen tests (failing on any count
mismatch, any assertion or test-name change) and records `converted-staging.json`.

## 3. Environments (from the frozen lock, unchanged)

```sh
ORIG="$EVID/original/full-stack-fastapi-template"
CONV="$EVID/converted/full-stack-fastapi-template"
( cd "$ORIG/backend" && uv sync --frozen --python 3.14 )
( cd "$CONV/backend" && uv sync --frozen --python 3.14 )
ORIG_PY="$ORIG/.venv/bin/python"; CONV_PY="$CONV/.venv/bin/python"

uv pip freeze --python "$CONV_PY" > "$EVID/freeze-before.txt"
uv pip install --python "$CONV_PY" "$SDK_ROOT/python[orm]"      # non-editable install
uv pip freeze --python "$CONV_PY" > "$EVID/freeze-after.txt"
python3 compare_freeze.py "$EVID/freeze-before.txt" "$EVID/freeze-after.txt"
```

`uv.lock` is used unchanged (`--frozen`); do not edit the template's pins. The SDK install
adds packages the lock does not carry (asyncpg, structlog, cyclopts and so on);
`compare_freeze.py` fails if it changed or removed any locked package. Record
`uv pip freeze` of both environments, the interpreter versions and the SDK artifact hash.
Whether the SDK's declared ranges (`psycopg>=3.2,<4`, starlette, pydantic) are compatible
with the lock is an untested assumption this step settles; the lock has psycopg 3.3.6.

## 4. Run

```sh
cd conformance/polyglot/applications/python/full_stack_fastapi
export NEUTRON_PY_APP_DATABASE_URL=...        # private; set, never printed
"$CONV_PY" run_comparison.py --evidence-dir "$EVID" \
  --original-python "$ORIG_PY" --converted-python "$CONV_PY"
```

The runner needs an interpreter that has psycopg (the converted one). For each side it
creates a uniquely named schema (`w2pyapp_<side>_<12 hex>`) with the database URL's
`options` parameter putting that schema first on `search_path`, runs `alembic upgrade
head`, (converted only) `shared/catalog_check.py`, then `python -m pytest tests` with a
JUnit file and the scenario transcript, and drops exactly the schema it created. The child
environment is built from scratch: `DATABASE_URL` (derived, with options),
`NEUTRON_PYAPP_SCHEMA`, a random `SECRET_KEY` and superuser password, `FASTAPI_ENV=development`
(as the template's `scripts/test.sh` does), and a loopback SMTP sink (`smtp_sink.py`) so
email-sending routes never reach a network. The URL, its password and the generated secrets
are redacted from every stored log.

Baseline or debugging, one side without comparison:

```sh
"$CONV_PY" run_comparison.py --evidence-dir "$EVID" --original-python "$ORIG_PY" \
  --converted-python "$CONV_PY" --only original
```

Use a fresh evidence directory per attempt (the runner refuses an existing `results/`).
Run `--only original` first: it is the unchanged template, and its result decides whether a
later converted failure is attributable to the conversion.

Results land in `$EVID/results/{original,converted}/` (`junit.xml`, `transcript.json`,
`pytest.log`, `alembic.log`, `versions.json`, converted `catalog.json`/`catalog.log`) and
`$EVID/results/verdict.json`. To recompare without re-running:

```sh
python3 compare_results.py --original "$EVID/results/original" --converted "$EVID/results/converted"
```

Manual equivalent of one side, if the runner is not usable: create a schema, export the
variables above for the child shell (not in a shell history file), then from
`$TREE/backend` run `"$PY" -m alembic upgrade head` and
`FASTAPI_ENV=development "$PY" -m pytest tests -p no:cacheprovider`, and drop the schema.

## Pass, partial, fail, blocked

**Pass** requires all of:

1. Section 1 static checks succeed on the checkout under test.
2. The original side passes all 59 expected tests (58 template tests, from the frozen
   sources by AST, plus `tests.neutron_corpus.test_neutron_scenario::test_scenario`) with no
   skip, error or failure. Both sides report exactly the same set.
3. The converted side passes the same 59.
4. The two scenario transcripts are identical after normalization (the scenario also
   asserts independent native row state on each side).
5. The converted `catalog.json` reports `catalog_admission: true`.
6. Evidence records the SDK `git rev-parse HEAD`, both `uv pip freeze` outputs, interpreter
   and package versions (`versions.json`), `reconstruction.json`, `converted-staging.json`,
   the PostgreSQL server version, and the log and JUnit hashes.

**Partial** (never reported as pass): a JUnit file, a transcript or the catalog result is
missing for a side that otherwise ran. **Fail**: any converted-side test failure, a test-set
difference, a transcript difference or a catalog mismatch while the original passed.
**Blocked / not run** (not a verdict on the application): the original side did not pass
every expected test, `uv sync` or the SDK install failed, `alembic` could not migrate,
the database was unavailable, or the SDK did not import in the converted environment.

Triage: an original-side failure first questions the environment (SMTP sink, DNS, Python
version, extension privileges), never the template; a converted-only failure is a
conversion, harness or SDK defect. Several tests patch `settings.SMTP_HOST` to
`smtp.example.com`, so those routes attempt to resolve that name on both sides exactly as
upstream CI does; run with outbound DNS available or accept identical behaviour on both
sides.

## Untested assumptions this run will settle

- `import neutron.orm` works inside the template's locked environment on Python 3.14
  (it imports the whole `neutron` package, which needs starlette, structlog, cyclopts and
  asyncpg).
- FastAPI 0.141.1 (locked) accepts `async def` routes and dependencies exactly as written,
  and runs async generator dependency cleanup (`DbSession.close`) after the route.
- The explicit commit in each route happens before the response is sent.
- pydantic-settings accepts the derived URL (a `postgresql+psycopg://` DSN with a percent-
  encoded `options` query) and `str(settings.DATABASE_URL)` preserves the query.
- `dataclasses.asdict` output validates into the SQLModel public models.
- Mapped-column OIDs: `varchar` for the 255-length and unbounded string columns, binary
  `timestamptz` round trips with exact microseconds, `uuid`.
- The unconverted API behaviours listed in `README.md` ("Not covered") behave as stated.

## What a pass would and would not mean

It would establish that, for the template's own 58 backend tests and the shared scenario
(users, items, auth, passwords, deactivation, conflicts, cascade deletion, bcrypt upgrade),
the converted persistence boundary produces the same observable HTTP behaviour and the
same independently observed PostgreSQL state as the unchanged SQLModel original, and that
the mappings match the migrated catalog. It would not establish: relationship mapping,
`OwnedRelation` cascades, the async Session or the request managers' admission and
shutdown draining, optimistic-conflict handling under concurrent writers, explicit-NULL
and negative-pagination error equivalence, performance, the frontend, deployment, or any
general SQLModel/SQLAlchemy replacement.
