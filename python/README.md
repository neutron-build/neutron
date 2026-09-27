# neutron-py

The AI application development framework for Python — Starlette underneath,
Pydantic throughout, with a first-class client for [Nucleus](https://github.com/neutron-build/neutron/tree/main/nucleus), the
multi-model database the rest of Neutron is built on.

```bash
pip install neutron-py
```

> The distribution is **`neutron-py`**, not `neutron` — that name on PyPI
> belongs to OpenStack's networking service.

## A first app

```python
from pydantic import BaseModel
from neutron import App, Router

app = App(title="My API", version="1.0.0")
router = Router()

class User(BaseModel):
    id: int
    name: str
    email: str

@router.get("/users/{user_id}")
async def get_user(user_id: int) -> User:
    return await app.db.sql.query_one(
        User, "SELECT * FROM users WHERE id = $1", user_id
    )

app.include_router(router)
```

Return a Pydantic model and you get validation, serialisation and an OpenAPI
3.1 schema from the same declaration. `GET /health`, `GET /openapi.json` and
`GET /docs` are mounted for you.

## What ships

| Area | Module |
|---|---|
| Routing, handlers, dependency injection | `neutron/router.py`, `handler.py`, `depends.py` |
| Middleware (the contract's 10-layer order) | `neutron/middleware.py`, `default_stack()` |
| Errors as RFC 7807 problem+json | `neutron/error.py` |
| OpenAPI 3.1 generation | `neutron/openapi.py` |
| Nucleus client — all 14 data models | `neutron/nucleus/` |
| AI: providers, agents, RAG, MCP | `neutron/ai/` |
| Auth, cache, jobs, realtime | `neutron/auth/`, `cache/`, `jobs/`, `realtime/` |
| CLI (`python -m neutron`) | `neutron/cli.py` |
| Test helpers | `neutron/test/` |

## Extras

```bash
pip install "neutron-py[ai]"      # AI providers, agents, RAG
pip install "neutron-py[crypto]"  # password hashing
pip install "neutron-py[granian]" # the Granian server
pip install "neutron-py[rich]"    # richer CLI output
pip install "neutron-py[all]"     # everything above
```

`[test]` is the development extra and is what CI installs.

## Nucleus

One pgwire connection reaches every data model; the non-relational models are
SQL functions rather than separate services or ports.

```python
from neutron.nucleus import NucleusClient

db = await NucleusClient.connect("postgresql://localhost:5432/mydb")

await db.kv.set("session:abc", "user-1", ttl=3600)
await db.vector.search("docs", embedding, k=5)
await db.document.insert("events", {"kind": "signup"})
```

Any PostgreSQL client works against Nucleus, so `asyncpg` and `psycopg` remain
available if you would rather not use this client at all.

## Documentation

Published docs — overview, quickstart, routing, middleware, database, realtime,
deployment — start at **https://neutron.build/docs/python/overview**. The wire-level
contract every Neutron SDK implements is
[`FRAMEWORK_CONTRACT.md`](https://github.com/neutron-build/neutron/blob/main/FRAMEWORK_CONTRACT.md); this SDK scores 12/12 on
its conformance matrix.

## Development

```bash
pip install -e ".[test]"
pytest
```

Live-database tests skip unless `NEUTRON_TEST_DATABASE_URL` points at a running
Nucleus instance.
