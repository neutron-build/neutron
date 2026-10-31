# PY-APP: full-stack-fastapi-template backend conversion

A bounded conversion of the public FastAPI project template's persistence boundary
from SQLModel/SQLAlchemy to the Neutron Python ORM (`python/neutron/orm`), with a
harness that runs the template's own backend tests and a shared HTTP scenario against
the unchanged original and against the converted application on PostgreSQL and
compares the outcomes.

Status: authored. The source pin and every frozen byte were verified against the
public repository, and the preparation scripts were run (they fetch and copy files;
they execute no application code). The converted application, the derived tests, the
scenario and the comparison runner have never been executed: no test has run on either
side. The application is not qualified. See `RUNBOOK.md` for the native commands and the
pass, partial, fail and blocked criteria.

## Frozen source

| | |
|---|---|
| Repository | `https://github.com/fastapi/full-stack-fastapi-template` |
| Commit | `cb740b656d7a0a6c5e12c7bf8e50343ec94ee9c7` (2026-09-01, "Update release notes") |
| License | MIT, Copyright (c) 2019 Sebastian Ramirez; `upstream/full-stack-fastapi-template/LICENSE` |
| Python | `>=3.14` (`.python-version` is 3.14; the template uses PEP 758 `except A, B:`) |

`source-manifest.json` records 36 files copied under `upstream/` (path, bytes, SHA256,
raw URL) and 4 hash-only files (`uv.lock`, root `pyproject.toml`, `.python-version`,
`.env`) that are checked only against a reconstructed full tree.
`python3 verify_source.py` checks the copies without executing anything.
`python3 prepare_original.py <fresh dir>` fetches the commit at depth 1, requires
`HEAD` to equal the pin and a clean tree, and compares all 40 files to the manifest.
The full tree is reconstructed so that the unchanged baseline has every file it imports;
nothing is deleted to make a baseline build.

The pin was verified independently of any earlier note: the commit exists in the public
repository and every file hash a prior note listed for it matches. Master was 18 commits
ahead when checked; between the pin and master only `backend/pyproject.toml` and
`uv.lock` (dependency bumps) differ in the backend, so the pin was kept. The prior note's
claim that revision `e2412789c190` is the schema is wrong: the schema the application
uses is the result of all five Alembic revisions (UUID keys, `ON DELETE CASCADE`,
`created_at`), which the harness runs unchanged.

## What is converted

The persistence boundary only. The FastAPI app, routes, status codes and detail texts,
JWT, password hashing (pwdlib argon2/bcrypt), email, the Pydantic API models, Alembic and
the schema stay the template's.

| Template boundary | Conversion |
|---|---|
| `User`/`Item` SQLModel tables | Mutable dataclasses in `app/persistence.py` with `Table`/`ColumnSpec`/`ModelMapping` over `uuid`, `varchar`, `bool`, nullable `timestamptz`. The SQLModel table classes stay in `app.models`, unused for persistence (Alembic's metadata still reads them). |
| `SessionDep` (`Session(engine)`) | One `DbSession` per request: one `Database` connection plus one `Session(db)` sharing it (fresh identity map), closed by the dependency, which rolls back anything a route did not commit. Routes commit explicitly, as the template's do. |
| `crud.py` | Same function names and keyword signatures. `get_user_by_email` finds the id with a typed `select` and returns the Session-tracked object. `update_user` assigns the `exclude_unset` fields, so unset fields are untouched. `authenticate` keeps the dummy-hash timing path and the bcrypt-to-argon2 upgrade. |
| `routes/items.py` | Owner-scoped and superuser listing with exact `COUNT` and `ORDER BY created_at DESC OFFSET LIMIT` through the typed `Query`; 404, 403, create, partial update, delete. |
| `routes/users.py`, `login.py`, `private.py`, `deps.py` | Same contract; every database access converted. Hashing and email run in the framework thread pool. |
| `core/db.py`, `initial_data.py` | `init_db` over `DbSession`; `initial_data.py` converted but not exercised (the test fixture calls `init_db`). |
| `Relationship(cascade_delete=True)` | Not mapped. User deletion bulk-deletes the owner's items through `Session.bulk_delete`, then deletes the user; the database foreign key also cascades. |
| `exclude_unset` partial update | Only assigned attributes become dirty; the Session writes only dirty columns. |

## Not covered, refused or different (recorded, not hidden)

1. **No relationship properties.** `User.items` and `Item.owner` are not modelled; the
   SDK's `OwnedRelation`/`delete_graph` exist but are budget-bounded and were not used.
2. **No public predicate reader on a Session.** `Session.get` is key-only and
   `SessionRequests`/`AsyncSessionRequests` yield a Session without its Database, so
   lookups by email, ordered and paginated lists and counts use the `Database` that
   `Session(db)` shares (the documented sharing pattern). The request managers'
   admission limit and shutdown draining are therefore not exercised, and the lifecycle
   is application code.
3. **Thread ownership.** The synchronous Session is thread-owned and FastAPI may run a
   request's dependencies and endpoint on different worker threads, so every database
   call runs on one dedicated thread (`run_db`), which also keeps blocking I/O off the
   event loop. This serializes the application's database work and does not exercise the
   async Session API.
4. **Optimistic updates.** The Session updates `WHERE` every mapped column equals its
   loaded value. A concurrent external edit between load and flush raises
   `ConflictError` (HTTP 500 here); the template overwrites blindly. Not exercised by the
   tests or the scenario.
5. **Explicit NULL on a non-null column** (for example `{"email": null}` on update)
   fails before SQL with `ValueError`; the template fails in the database with an
   `IntegrityError`. Both are HTTP 500. Negative `skip`/`limit` also fail as 500 on both
   sides, with different exception types. Not exercised.
6. **Malformed token subject.** A signed token whose `sub` is not a UUID returns 403
   "Could not validate credentials"; the template's behaviour there is unspecified.
7. **Identity map.** `Session.get` returns the cached object and nothing expires on
   commit. The test fixture's long-lived Session uses `fresh_reads` (a re-read of tracked
   objects) so it observes commits made by the application's own connections; this is
   test-support behaviour, not application behaviour.
8. **Not touched:** email delivery and templates, the React frontend, Docker, deployment,
   Sentry, `backend_pre_start`, and the Alembic migrations (run unchanged as the schema
   source; the catalog check compares the mappings to what they created).
9. `ORDER BY created_at DESC` has no tiebreak on either side, as in the template; the
   converted query asks for PostgreSQL's own `NULLS FIRST` placement to match.

## Evidence the harness produces

For each side, in a uniquely named schema created and dropped by the runner (`CREATE
SCHEMA` fails if it exists; only schemas the runner created are dropped):

- the template's `alembic upgrade head`, then every template test (58) plus the shared
  scenario, as a JUnit file; the original side runs the tests byte-identical to the pin,
  the converted side runs tests derived by exact rewrites (`rewrites.py`, 19 text
  replacements across 7 files: the SQLModel `Session`/`select` imports and the eight
  statements that read the database through SQLModel; every `assert` statement and test
  function name is unchanged, checked by AST at preparation time);
- the shared scenario (`shared/scenario_test_source.py`, identical bytes on both sides):
  roughly 70 recorded HTTP exchanges with the superuser, signup, items, profile, passwords,
  deactivation, conflicts, cascade deletion and a natively inserted bcrypt user, each
  checked against an independent psycopg connection (rows, hashes, exact `created_at`
  instants, counts, cascade, ordering) and recorded as a normalized transcript;
- for the converted side, `Database.catalog_table` admission of both mappings, a native
  comparison of the complete column set, primary keys, the unique email index and the
  `item.owner_id` `ON DELETE CASCADE` foreign key (`shared/catalog_check.py`);
- `compare_results.py` verdict: pass only if every expected test passes on both sides, the
  transcripts are identical and the catalog check passes.

## Layout

| Path | Purpose |
|---|---|
| `upstream/` | 36 frozen template files (MIT, with `LICENSE`). |
| `source-manifest.json`, `verify_source.py` | Pin, per-file bytes and SHA256; static verification. |
| `prepare_original.py` | Reconstructs the exact original in a fresh directory and adds the scenario. |
| `converted/` | Authored converted application files and the converted `conftest.py`, overlaid onto the template paths. |
| `rewrites.py`, `prepare_converted.py` | Derive the converted tests from the frozen tests; stage the converted tree. |
| `shared/` | The scenario (both sides) and the catalog check (converted side). |
| `run_comparison.py`, `smtp_sink.py`, `compare_results.py`, `compare_freeze.py` | Coordinator runner, loopback SMTP sink, verdict, dependency-drift check. |
| `verify_inventory.py`, `harness-manifest.json`, `harness_lib.py` | Static verification and inventory of the authored files. |
| `RUNBOOK.md` | Exact native commands and criteria. |
