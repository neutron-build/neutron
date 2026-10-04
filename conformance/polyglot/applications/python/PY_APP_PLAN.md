# PY-APP plan: full-stack-fastapi-template conversion (groundwork only)

Status: design note. Nothing here is frozen, fetched, executed or converted.
`framework_lifecycle.py` and `polymorphic_sti.py` remain authored examples and are
not substitutes for this external-application evidence. The Go analogue is
`../go/README.md` (`source-manifest.json`, `verify_source.py`, `prepare_original.py`);
PY-APP should copy that shape.

## Corpus

- Public repository: `fastapi/full-stack-fastapi-template` (formerly published as
  `tiangolo/full-stack-fastapi-template`), license MIT (`LICENSE` is part of the freeze).
- Backend stack at the selected pin: FastAPI + SQLModel (on SQLAlchemy) + Alembic +
  psycopg 3, PostgreSQL. SQLModel is the peer model; this is integration conversion
  evidence, not direct SQLAlchemy parity.

## Source pin

The coordinator's local corpus plan (`Neutron/_internal/architecture/polyglot-plan/corpus-plan.local.json`,
entry `PY-APP`; git-ignored, outside this repository) declares commit
`cb740b656d7a0a6c5e12c7bf8e50343ec94ee9c7`. This worker did not fetch anything and could
not verify these bytes; treat the pin as UNVERIFIED until `verify_source.py` below passes.
Declared frozen files (path, bytes, sha256):

| Path | Bytes | SHA256 |
|---|---|---|
| LICENSE | 1076 | 0bd4e76cdd06ab24cce2de5e74f490920897f90a22ad35e8403ed006d4560f48 |
| backend/app/models.py | 3653 | 1b7384d0dc779cca9ebc9d2b05ccd1864988c2043a3692384ff7a731819050f0 |
| backend/app/crud.py | 2463 | f69d79e858ee22cee7792494b0df0370afaed20439ed16bbf6f70767926254d1 |
| backend/app/api/routes/items.py | 3446 | 7f0fa55d1f7b02188c4abd4f88aa6db92fe03de38258b765ed050ec72032b410 |
| backend/app/api/routes/users.py | 6918 | 566f253330d75bdbf04f2fd59b380dd2c51041d931bd7af6b2064ee66ea96e6d |
| backend/app/api/deps.py | 1711 | 84c30966b3b862c643009c14146d1924dec50be5b0efb9f56798e2fda5189daa |
| backend/app/core/db.py | 1190 | 22b8642606ae619a1ea07b5e3c3bee49c2fc0ded5d0e583e810fba6e7ffadfc9 |
| backend/app/alembic/versions/e2412789c190_initialize_models.py | 1728 | a73fcd9ff8a143b0a75228b63b6589e0ecbfa14c27286d33d4adeffe99c40075 |
| backend/tests/api/routes/test_items.py | 5165 | be711def80eb52caeb1f2b0d9a933e294ea4f2f798ae2dc8924fb41fdda5528a |
| backend/tests/api/routes/test_users.py | 16867 | 20676b10b47c74505cca440ec00fd21d5d4b376f46190de2ec7c8194f579f1e8 |
| backend/pyproject.toml | 1892 | a2c49d0900e917115a65bd838c20e193940846e231d3fc44dc9670b6e2c1ee1e |
| uv.lock | 255646 | 0b70f5b6fcbc69ddaace35bd66078bd92c4a381a6cded72c01ad65938c54ef50 |

PIN NEEDED (not in the repository, not read by this worker):

- Full-tree closure. The unchanged baseline also needs files outside the list above
  (at least `backend/app/core/config.py`, `core/security.py`, `main.py`,
  `backend/tests/conftest.py`, `tests/utils/*`, `backend/app/api/main.py`,
  `backend/app/utils.py`, `backend/app/backend_pre_start.py`/`initial_data.py` if
  imported). Reconstruct the full tree at the pin like Go `prepare_original.py` and
  freeze the additional imported files; do not delete files that fail to import to
  fabricate a baseline.
- Executed toolchain: Python version and the resolved sqlmodel, sqlalchemy, fastapi,
  starlette, pydantic, psycopg, pwdlib/argon2 versions from the frozen `uv.lock`. Record
  both sides' interpreter and wheel hashes; install from `uv.lock` unchanged.
- Whether `psycopg` in the lock satisfies Neutron's `psycopg>=3.2,<4` (the declared
  template line is `psycopg[binary]>=3.3.4,<4`).

## Bounded slice to convert

Convert the persistence boundary only; keep the FastAPI app, routes' public contract,
auth/JWT, password hashing and email behavior application-owned and unchanged.

1. Models: `User` and `Item` as mutable dataclasses with `ModelMapping` over native
   `uuid`/`text`/`timestamptz` columns, `Item.owner_id` FK `ON DELETE CASCADE` from the
   Alembic revision schema (catalog-verified, not inferred from the ORM).
2. `SessionDep` in `deps.py`: replace the SQLModel `Session(engine)` dependency with a
   request-owned `SessionRequests`/`AsyncSessionRequests` session (fresh identity map,
   commit on success, rollback on failure).
3. `crud.py`: create/update user, lookup by email, authenticate (timing behavior kept),
   create item with ownership injection.
4. `routes/items.py` and the item/user-deletion paths of `routes/users.py`: owner-scoped
   and superuser listing with exact total count, 404 missing, 403 foreign (exact detail),
   create, partial update (`exclude_unset` maps to `OMIT`; explicit NULL stays validated by
   the app model), delete, user delete cascading to owned items.

Excluded: login/token routes, email/recovery, the React frontend, Alembic execution (the
revision is used as a schema oracle), SQLModel/Pydantic model validation (stays as the
API layer), and any claim of full SQLAlchemy/SQLModel replacement.

## Oracles

Business (original tests, unchanged, run against both sides): owner-only listing and count,
superuser sees all, 404/403 details, ownership injection and UUID, partial update, item delete
committed, user delete cascades items, unique email, password update security behavior.

Native (independent psycopg, not the ORM under test): FK and cascade catalog rows, UUID and
timestamptz column identity, owner/count rows, snapshots before/after commit and rollback,
failed unique insert leaves the session reusable. The original list ordering is
timestamp-only and nondeterministic; add the same explicit id tiebreaker to both sides and
label it a deviation.

## Required work, in order

1. Add `upstream/` (full tree at the pin), `source-manifest.json`, `dependency-source-manifest.json`
   and `verify_source.py`/`prepare_original.py` modelled on the Go versions; verify bytes only.
2. Reconstruct and run the unchanged application and its tests on PostgreSQL (coordinator, serial).
3. Write the conversion and an `original-native` harness; run both sides against the same oracles.
4. Independent native acceptance with source/toolchain/artifact hashes. Until then PY-APP is not claimed.
