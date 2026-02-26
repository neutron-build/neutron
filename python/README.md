# Neutron Python

Nucleus database client for Python — all 9 data models, async-first, Pydantic-native. Plus a Mojo ML inference bridge.

## Philosophy

Light core, modular data models. `pip install neutron-nucleus` installs the SQL client only. Every other data model is an opt-in extra with zero additional dependencies — because Nucleus exposes everything over a single pgwire connection via SQL functions.

## Install

```bash
pip install neutron-nucleus          # SQL only
pip install neutron-nucleus[vector]  # + vector search
pip install neutron-nucleus[mojo]    # + Mojo ML bridge
pip install neutron-nucleus[all]     # everything
```

## Schema — Pydantic-Native

Schema is defined as Pydantic models. If you know Pydantic, you already know the schema language:

```python
from neutron_nucleus import sql, kv, vector, ts, doc

class User(sql.Table, table="users"):
    id: int = Field(primary_key=True)
    name: str = Field(max_length=100)
    email: str = Field(unique=True, index=True)
    age: int | None = None

sessions = kv.Store("sessions", ttl=3600)
embeddings = vector.Collection("embeddings", dimensions=1536)
events = ts.Metric("events", retention="30d")
posts = doc.Collection("posts")
```

No metaclass magic. `__init_subclass__` handles registration. Models bind to a client at query time, not at definition time — multiple clients, no global state.

## SQL Client

```python
async with NucleusClient("postgres://localhost:5432/mydb") as db:
    # Query
    users = await db.sql(User).filter(age__gte=18).order_by("name").limit(20).all()
    user  = await db.sql(User).get(id=1)

    # Insert
    user = await db.sql(User).insert(User(name="Alice", email="a@b.com", age=30))

    # Update / Delete
    await db.sql(User).filter(id=1).update(name="Alicia")
    await db.sql(User).filter(age__lt=0).delete()

    # Transaction
    async with db.transaction() as tx:
        await tx.sql(User).insert(...)
        await tx.sql(Order).insert(...)
```

Filter kwargs use Django-style suffixes: `age__gte`, `name__contains`, `name__startswith`, `id__in`, `created_at__lt`.

## All 9 Data Models

```python
# KV
await db.kv.set("user:42:token", "abc123", ttl=3600)
token = await db.kv.get("user:42:token")
count = await db.kv.incr("page:views:home")

# Vector search
results = await db.vector(embeddings).search(query_vec, k=10, filter={"title__contains": "Hello"})
# results: list[ScoredRecord] with .score and .record

# Timeseries
await db.ts(events).insert(DataPoint(value=72.4, tags={"host": "web-01"}))
buckets = await db.ts(events).aggregate(start=..., end=..., bucket="1h", agg="avg")

# Document
post_id = await db.doc(posts).insert(BlogPost(title="Hello", body="World"))
results = await db.doc(posts).query("$.author == 'alice'")

# Graph (Cypher)
alice_id = await db.graph.add_node(Person(name="Alice"))
await db.graph.add_edge(alice_id, bob_id, Follows(since="2024-01-01"))
friends = await db.graph.cypher("MATCH (p:Person)-[:FOLLOWS]->(f) RETURN f", {"p": "Alice"})

# FTS
results = await db.fts("articles").search("machine learning", limit=10)

# Pub/Sub
await db.pubsub.publish("notifications", {"type": "message", "body": "Hello"})
async for event in db.pubsub.subscribe("notifications"):
    print(event)
```

## Mojo ML Bridge

The unique differentiator — embed text with a Mojo model and store directly in Nucleus:

```python
from neutron_nucleus.extras.mojo import MojoBridge, embed_and_store

bridge = await MojoBridge.launch(
    binary=Path("neutron-mojo-serve"),
    model=Path("llama-3.2-1b.gguf")
)

# Generate text
tokens = await bridge.generate("Hello, world!", max_tokens=100)

# Embed and store in one call
await embed_and_store(bridge, db, embeddings, "Article text...", {"title": "Hello"})

# Later: semantic search over what you stored
results = await db.vector(embeddings).search(await bridge.embed("similar text"), k=10)
```

Bridge tiers (auto-selected):
- **subprocess + stdio** — zero shared memory, works everywhere, for infrequent inference
- **mmap shared memory** — <1ms latency, for hot inference loops
- **Mojo .so extension** — tightest integration (future, pending Mojo roadmap)

## Transport Protocol

`NucleusClient` uses **asyncpg** under the hood — binary wire protocol by default, ~5× faster than text-mode drivers for high-volume queries (1M rows/sec vs 200K rows/sec). Custom Nucleus types (Vector, GeoPoint, DocumentID) are registered as binary codecs at pool creation time, not per-query.

Pool defaults that match Nucleus workloads:

```python
# Configured automatically — shown here for reference
NucleusClient("postgres://localhost:5432/mydb",
    min_size=8,          # always-warm connections
    max_size=16,         # scales under burst
    max_inactive_connection_lifetime=300,  # 5-minute idle recycle
    max_queries=50_000,  # recycle connection after N queries
)
```

## Async-First, Sync Available

Every method is `async`. A sync client wraps async calls on a dedicated thread-bound event loop — same pattern as Tortoise ORM: one persistent event loop thread, all calls go through `asyncio.run_coroutine_threadsafe`:

```python
# Async (recommended)
async with NucleusClient(...) as db:
    users = await db.sql(User).all()

# Sync (thread-safe, dedicated event loop thread — not asyncio.run() per call)
with NucleusSyncClient(...) as db:
    users = db.sql(User).all()
```

## Pub/Sub Backpressure

`db.pubsub.subscribe()` is an async generator backed by a bounded `anyio` queue. If the consumer is slow, the producer pauses — no silent message drops:

```python
# Backpressure is automatic — producer slows down when queue fills
async for event in db.pubsub.subscribe("notifications", max_buffer=100):
    await process_message(event)  # next message waits until this returns
```

This uses anyio v4.2+ memory object streams. The queue bound defaults to 100 messages. Overflow behavior is configurable: `on_overflow="drop_oldest"`, `"drop_newest"`, or `"block"` (default).

## Extras

| Extra | Deps | Provides |
|-------|------|---------|
| `kv` | none | KV client |
| `vector` | none | Vector search |
| `ts` | none | Timeseries |
| `doc` | none | Document store |
| `graph` | none | Graph (Cypher) |
| `fts` | none | Full-text search |
| `geo` | none | Geo queries |
| `pubsub` | anyio | Pub/Sub streams |
| `mojo` | msgpack | Mojo ML bridge |
| `uvloop` | uvloop | 2-4x event loop throughput on Linux/macOS |
| `all` | all of above | Everything |

Most extras have zero extra deps — Nucleus exposes all data models via SQL functions over a single pgwire connection.

## What We Took From Each Library

| Library | What we adopted |
|---------|----------------|
| asyncpg | Binary pgwire transport, row objects without allocation overhead |
| psycopg3 | Unified sync+async behind identical signatures, type adapters |
| Pydantic v2 | Schema as Python classes, `model_validate` for row deserialization |
| redis-py | Namespace-grouped KV commands (`db.kv.get`, `db.kv.set`) |
| qdrant-client | Strongly typed result objects — no raw dicts returned |

## What We Avoided

| Library | What we avoided |
|---------|----------------|
| SQLAlchemy | Implicit lazy loading (fatal in async), two separate APIs (Core vs ORM), five session abstractions |
| Tortoise ORM | Global `init()` required, models implicitly bound to a global registry |
| Peewee | Sync-only, async via thread pool band-aid |
| motor | Separate package wrapping sync driver in asyncio (deprecated by MongoDB in 2025) |
| redis-py | `AsyncRedis` inheriting from `Redis` with shadowed signatures |

## Requirements

- Python 3.11+
- Nucleus running (local or remote, pgwire port 5432)

## File Structure

```
python/
├── neutron_nucleus/
│   ├── client.py           # NucleusClient, NucleusSyncClient
│   ├── schema/
│   │   ├── sql.py          # sql.Table base class
│   │   ├── kv.py           # kv.Store
│   │   ├── vector.py       # vector.Collection
│   │   ├── ts.py           # ts.Metric
│   │   └── doc.py          # doc.Collection
│   ├── query/
│   │   └── builder.py      # .filter().order_by().limit().all()
│   └── extras/
│       ├── kv.py
│       ├── vector.py
│       ├── ts.py
│       ├── doc.py
│       ├── graph.py
│       ├── fts.py
│       ├── geo.py
│       ├── pubsub.py
│       └── mojo.py         # MojoBridge, embed_and_store
├── tests/
├── pyproject.toml
└── README.md
```

## Implementation Order

1. Core SQL client — `filter()`, `get()`, `insert()`, `update()`, `delete()`, transactions
2. KV extra — simplest model, validates the extras pattern
3. Vector extra — highest demand (AI/ML)
4. Mojo bridge — the unique differentiator, proves end-to-end ML→DB workflow
5. TS, Doc, Graph — in parallel once pattern established
6. FTS, Geo, Pub/Sub, Columnar — remaining extras
7. DDL migrations — `db.sql.migrate()`
8. Type stubs — full `mypy --strict` pass

## Status

Planned — not yet implemented.
