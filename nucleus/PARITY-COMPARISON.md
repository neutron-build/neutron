# Nucleus vs HelixDB vs Polygres — Feature Parity

Local-only reference (gitignored). Verified against Nucleus source and public
competitor docs on 2026-07-16. Re-verify before any external use; competitor
claims are taken from their READMEs/docs and may drift.

## What each one actually is (not the same category)

- **Nucleus** — From-scratch Rust multi-model engine. 14 data models, single
  binary, pgwire + RESP. Standard SQL clients work. Dev preview.
- **HelixDB** — From-scratch Rust engine, **graph + vector primary** (KV/doc/
  relational secondary). Object-storage-backed. Custom DSL, not SQL. YC-backed,
  shipping cloud, v3.0.8, ~5.6k GitHub stars.
- **Polygres** — **Not an engine.** Managed Postgres 17 + pgVector + pgGraph
  (Evokoa's own pgrx ext) + a Python SDK / HTTP retrieval API. Beta, SDK v0.1.0
  (released 2026-07-10), ~2 stars. Bet: "don't reinvent Postgres, extend it."

## Feature parity matrix

Legend: YES = shipped | NO = absent | PARTIAL = exists but limited | n/a

### Core data models

| Model | Nucleus | HelixDB | Polygres |
|---|---|---|---|
| Relational / SQL | YES (pgwire v3) | YES (secondary) | YES (Postgres 17) |
| Key-Value | YES (+ RESP/Redis wire) | YES | (via Postgres) |
| Document (JSON) | YES (JSONB TLV + GIN) | YES | YES (JSONB) |
| Vector ANN | YES (HNSW + IVFFlat, cosine/L2/IP) | YES (HNSW) | YES (pgVector HNSW) |
| Graph | YES (CSR, BFS/DFS/Dijkstra, shortest-path) | YES (primary) | YES (pgGraph CSR) |
| Full-text search | YES (custom inverted idx, BM25, 6 stemmers) | YES | YES (tsvector) |
| Columnar analytics | YES | NO | NO |
| Timeseries | YES (Gorilla compression) | NO | NO |
| Geo | YES (custom R-tree) | NO | NO |
| Blob / object store | YES (BLAKE3 dedup) | NO | NO |
| Datalog | YES | NO | NO |
| Pub/Sub | YES (LISTEN/NOTIFY) | (likely) | YES (Postgres) |
| Sparse vectors | YES | NO | NO |
| Tensors | YES | NO | NO |

Nucleus wins model coverage by a wide margin. Neither competitor has any of
the bottom eight rows.

### Query / access surface

| Surface | Nucleus | HelixDB | Polygres |
|---|---|---|---|
| Standard SQL | YES | NO (custom DSL) | YES |
| PostgreSQL wire protocol | YES | NO (HTTP `/v1/query`) | YES |
| Redis wire protocol (RESP) | YES | NO | NO |
| Works with arbitrary pg client / ORM | YES | NO | YES |
| Multi-language SDKs | (via pg drivers) | YES (Rust/TS/Go/Python) | YES (Python) |
| Coding-agent integration | YES (`neutron mcp`) | YES (`helix chef`) | YES (Agent Skills) |

Nucleus's pgwire compatibility is a real adoption moat vs HelixDB's custom DSL.

### Hybrid / cross-model retrieval (the headline gap)

| Capability | Nucleus | HelixDB | Polygres |
|---|---|---|---|
| FTS + Vector composition in SQL | YES (`WHERE FTS_MATCH ... ORDER BY VECTOR_DISTANCE`) | — | — |
| Cross-model transactions | YES (in-process rollback verified) | YES | PARTIAL (pgGraph is read-only derived state) |
| **Graph + Vector fused scoring** (`graph_first` / `vector_first` / `joint` with blended ranks) | **NO** | **YES (core)** | **YES (core)** |

**This is the single material feature gap.** Both competitors lead their
marketing with hybrid graph+vector retrieval (the GraphRAG primitive). Nucleus
can compose FTS+Vector and Graph+Relational in SQL but has no native fused
operator.

### Verified against Nucleus source (2026-07-16)

- `grep` for vector/ANN/cosine/embedding refs in `src/graph/` → 0 hits beyond
  one doc comment. Graph module has no vector awareness.
- `grep` for graph/adjacency/CSR/traverse in `src/vector/` → only HNSW's
  *internal* proximity graph, not the data-graph model.
- FTS+Vector hybrid test exists: `src/executor/tests/test_cross_model.rs:22`
  (`test_fts_vector_hybrid_search`) — confirmed it's filter+sort, not score
  fusion.
- Sparse-vector blend exists (`hybrid_score` in `src/sparse/mod.rs:440`) but
  blends sparse vs dense *vectors*, not graph proximity vs vector distance.

So: no `GRAPH_VECTOR_SEARCH(start_node, embedding, ...)` operator with RRF or
weighted fusion. Competitors have it as their flagship.

### Indexes

| Index | Nucleus | HelixDB | Polygres |
|---|---|---|---|
| B-tree | YES | YES | YES |
| HNSW (vector) | YES | YES | YES |
| IVFFlat (vector) | YES | NO | (pgVector) |
| GIN (document) | YES | — | YES |
| R-tree (geo) | YES (custom, NOT H3) | NO | NO |
| Inverted (FTS, custom NOT Tantivy) | YES | YES | YES (tsvector) |
| Graph CSR + property B-tree | YES | YES | YES (pgGraph) |

### Durability / scale / ops

| Capability | Nucleus | HelixDB | Polygres |
|---|---|---|---|
| Per-model WAL | YES | YES | YES (Postgres) |
| Cross-model crash-atomic commit | **NOT CLAIMED** (in-process rollback only) | YES | YES (Postgres) |
| Backup / PITR | incomplete | YES (cloud) | YES (Postgres) |
| Distributed / Raft | incomplete / unsupported | single-writer + read replicas | pooled (PgBouncer-style) |
| Object storage backend (S3) | NO | YES | (Postgres on K8s) |
| Encryption at rest | YES (AES-256-GCM) | ? | (Postgres) |
| Compression | YES (LZ4 + per-column adaptive) | ? | (Postgres) |
| Managed cloud | NO | YES (GA) | YES (beta) |

### Maturity / licensing

| | Nucleus | HelixDB | Polygres |
|---|---|---|---|
| Status | Dev preview, ~4,216 declared tests | v3.0.8, 181 releases, GA cloud | Beta, SDK v0.1.0 |
| License | BSL 1.1 -> MIT (4 yr) | Apache-2.0 | Apache-2.0 (SDKs/ext) |
| Auth | SCRAM roles + RLS | (cloud auth) | Postgres roles |

## What Nucleus has that neither competitor has

1. **10+ data models they don't ship at all** (columnar, timeseries, geo, blob,
   datalog, pubsub, sparse, tensor). For any workload outside graph+vector+rel,
   they have no answer.
2. **RESP/Redis wire protocol** — drop-in Redis client compatibility for KV.
3. **Real cross-model write transactions.** Polygres's pgGraph is derived
   read-only state, not txn-write graph. HelixDB has ACID but only across its
   narrow model set.
4. **pgwire SQL compatibility** vs HelixDB's new DSL — every Postgres client and
   ORM works against Nucleus with zero changes.
5. **Custom indexes** (FTS not Tantivy, Geo custom R-tree not H3) — full
   engine control, no extension-packaging constraints.

## The two gaps that actually matter

1. **Graph + vector hybrid retrieval operator** (engineering gap, closeable).
   Both competitors' flagship feature. A `GRAPH_VECTOR_SEARCH(...)` SQL function
   doing graph expansion + ANN scoring + RRF/weighted fusion would close it and
   is the single highest-leverage parity item. Nucleus already has both halves
   (CSR graph + HNSW vector); they just aren't wired together.

2. **Managed cloud + durability story** (commercial gap, harder). Both ship
   managed products; Nucleus ships a binary. Cross-model crash-atomic commit is
   not yet claimed and Raft is incomplete. Closing the hybrid-retrieval gap
   still leaves nothing for a team to *use* without self-hosting a preview.

## Tracking-file note

`nucleus/COMPETITOR-GAPS.md` is 15/15 closed but tracks only Elasticsearch,
Dragonfly/Valkey, Neo4j, ClickHouse, CockroachDB. It does NOT track the
graph-vector cohort (HelixDB, Polygres, SurrealDB, etc.) and so gives a false
"all caught up" signal. Add a new section:

```
## vs HelixDB / Polygres (graph-vector cohort)
- [ ] Graph+Vector hybrid retrieval (graph_first / vector_first / joint fusion)
- [ ] Object-storage-backed durability option
- [ ] Single-writer + read-replica serving (blocked on Raft completion)
- [ ] Managed cloud offering
```

## Wider competitive landscape (added 2026-07-16)

The HelixDB/Polygres comparison is narrow. The real field for a multi-model,
AI-era DB is wider. Grouped by how directly each threatens Nucleus's position.

### Tier 1 — direct conceptual rivals (must track)

| DB | Why it matters | Stars/maturity |
|---|---|---|
| **SurrealDB** | Rust multi-model (document + graph + vector), ships one endpoint. The truest peer to Nucleus's pitch and the first name any informed reviewer reaches for. | ~30k, shipping |
| **Weaviate** | Established player in hybrid vector+keyword+graph retrieval. The bar the GraphRAG gap is measured against. | ~11k, mature |
| **Qdrant** | Pure vector at scale; payload filtering + product/binary quantization. The bar for vector cost/quality. | ~20k, mature |
| **DuckDB** | Embedded OLAP. The bar for the columnar analytics claims. | ~25k, mature |

### Tier 2 — popular and commercially relevant

| DB | Why it matters |
|---|---|
| **Supabase** / **Neon** | The actual winners of "Postgres for apps." Polygres is a niche Supabase. They own Nucleus's target buyer commercially. |
| **Apache AGE** | The OSS Postgres graph extension pgGraph positions against. Context for the pgGraph fight. |
| **Redis Stack** (search/vector/graph/JSON) | Multi-model Redis framing — a different "one DB many models" angle. Nucleus already tracks DragonflyDB/Valkey but not Redis Stack's multi-model surface. |
| **pgvector + extensions stack** (TimescaleDB + AGE + pgvector) | The pragmatic incumbent stack Nucleus is implicitly replacing. |

### Tier 3 — conceptually adjacent (niche, worth knowing)

| DB | Why |
|---|---|
| **CozoDB** | Datalog-based embedded DB. Relevant because Nucleus ships a Datalog engine and almost nobody else does. |
| **Memgraph** / **TigerGraph** | Graph specialists — the bar for graph traversal performance at scale. |
| **TiDB** / **SingleStore** | HTAP (OLTP + OLAP in one). Competes with Nucleus's SQL+columnar combo. |
| **Fauna** | Serverless multi-model. Different deployment story. |
| **TerminusDB** | Graph + document with Prolog-style querying. Conceptually close to Nucleus's datalog+graph combo. |

### Tier 4 — vector specialists (overlap with Tier 1)

Pinecone, Milvus, Chroma, pgvector (standalone), Weaviate (listed above).
Nucleus's vector model competes here on parity, not novelty. Pinecone/Milvus
win on raw scale and managed infra; Chroma on DX; pgvector on Postgres compat.

## RAG / agent-memory capabilities (added 2026-07-16)

### Premise correction

**No database enlarges an LLM's context window.** The window is fixed by the
model (128k GPT-4, 200k Claude, 1M Gemini). Polygres/HelixDB/Qdrant do
**retrieval** — selecting a smaller, higher-relevance subset to fit *into* the
fixed budget. Polygres's "more context for agents" marketing is, technically,
*better-selected* context per token, via RAG.

The real question: which retrieval patterns can Nucleus serve, and at what
cost/quality? That is the actual competitive axis for the agent-memory narrative.

### Capability matrix

| RAG pattern | Nucleus today | After gaps closed |
|---|---|---|
| Plain vector RAG (semantic similarity) | YES — HNSW + IVFFlat + filtered ANN | same |
| Keyword + semantic (BM25 + vector) | YES — SQL composition (`FTS_MATCH` + `ORDER BY VECTOR_DISTANCE`) | same |
| **GraphRAG** (graph-expand + vector-score fused) | **NO — flagship gap** | YES — would match Polygres/HelixDB/Weaviate |
| **Vector quantization** (fit 10M+ vectors in RAM) | **NO — no PQ/SQ/binary quant** | separate gap; Qdrant + pgvector both ship it |
| **Symbolic/rule-based context** (Datalog over facts) | **YES — unique asset, unused** | same |
| Hybrid score fusion (RRF / weighted blend) | NO native operator | YES with the GraphRAG work |
| Multi-turn agent memory (session state) | YES via KV + RESP | same |

### Two RAG-cost primitives missing (not one)

The hybrid-retrieval gap is the visible one. The **quantization gap is quieter
but matters at scale** — production RAG past ~10M vectors depends on PQ/SQ/
binary quantization to stay in RAM and keep ANN fast. Verified absence:
`grep` for `quantiz|pq|product.quant|binary.quant|scalar.quant` in
`src/vector/` returns 0 hits. pgVector and Qdrant both ship this.

### Nucleus's unused unique angle: Datalog as context enrichment

Polygres is pgVector + pgGraph glued together. Nucleus ships a Datalog engine
that does symbolic reasoning over facts (transitive closure, rule inference) —
a context-enrichment mode LLMs are structurally bad at. A retrieval story built
on **GraphRAG (close the gap) + Datalog-driven retrieval (the unique asset)**
is stronger than copying Polygres's bundling. Concretely: `DATALOG_QUERY` over
imported relational + graph facts, fed into the prompt alongside vector chunks,
gives the LLM derived facts it cannot infer from raw text. Nobody else in the
field offers this at the storage layer.

### Memory-layer competitors (sit ABOVE the DB layer)

These are not databases — they're "memory" products that use a DB underneath.
Polygres/HelixDB position as the storage backend for them. If Nucleus competes
on the agent-memory narrative, it competes at the storage layer, not here.

| Product | What it does |
|---|---|
| **Mem0** | Memory layer for LLMs; vector + graph under the hood |
| **Letta** (was MemGPT) | Agent memory with context compaction / virtual context mgmt |
| **Zep** | Temporal knowledge graph for long-term agent memory |
| **Cognee** | Builds knowledge graphs from unstructured data for RAG |

Opportunity: none of these are tightly bound to a specific engine. A Nucleus
backend that ships GraphRAG + Datalog retrieval natively could be an
attractive substrate for them, especially if a managed offering appears.

## Benchmark backlog (add to `nucleus/benches/` later)

Concrete, measurable items. Each is specified so it can be turned into a
criterion benchmark or a harness in `typescript/benchmarks/` later. Priority
ranks leverage on the parity story, not effort.

| # | Capability to benchmark | Metric | Comparison targets | Pri |
|---|---|---|---|---|
| B1 | GraphRAG fused retrieval (`GRAPH_VECTOR_SEARCH`) | p50/p99 latency vs result-set size | Weaviate, HelixDB, Polygres | P0 |
| B2 | Vector ANN recall@10 + qps | recall@10, throughput | Qdrant, pgvector, Milvus | P0 |
| B3 | Vector quantization (once implemented) | RAM/1M vectors, recall delta | Qdrant PQ, pgvector binary | P1 |
| B4 | Graph shortest-path / traversal | ops/sec vs node count | Neo4j, Memgraph, pgGraph | P1 |
| B5 | FTS hybrid (BM25 + vector) end-to-end | end-to-end query latency | Elasticsearch, Typesense | P1 |
| B6 | Datalog inference over relational+graph facts | rules/sec, fact-set size | CozoDB, TerminusDB | P2 |
| B7 | Cross-model transaction (5-model BEGIN/COMMIT) | commit latency vs single-model | n/a (unique surface) | P2 |
| B8 | Columnar scan / aggregation | scan GB/sec | DuckDB, ClickHouse | P2 |
| B9 | Timeseries ingest + compressed size | points/sec, compression ratio | TimescaleDB, InfluxDB, QuestDB | P2 |
| B10 | KV ops via RESP wire | ops/sec, p99 | Redis, DragonflyDB, Valkey | P2 |
| B11 | psql wire-compat round-trip | connect + simple query latency | real PostgreSQL | P3 |

Notes for when benchmarking starts:
- P0 items are the parity story — they must exist and be defensible before any
  external comparison claim is published.
- Never hand-write RPS numbers (per AGENTS.md accuracy discipline). Use the
  harness in `typescript/benchmarks/` and record raw output.
- Each benchmark should publish a machine config + dataset + seed so results
  are reproducible and not debunkable.

## Sources

- Nucleus: `nucleus/README.md`, `nucleus/CLAUDE.md`, `nucleus/COMPETITOR-GAPS.md`,
  `nucleus/src/executor/tests/test_cross_model.rs`, `nucleus/src/graph/`,
  `nucleus/src/vector/`, `nucleus/src/sparse/`.
- HelixDB: `github.com/HelixDB/helix-db` README (v3.0.8), `docs.helix-db.com`,
  YC launch page.
- Polygres: `polygres.com`, `docs.evokoa.com/polygres`,
  `github.com/Evokoa/polygres-sdk`, `github.com/Evokoa/pgGraph` (v0.1.8).
