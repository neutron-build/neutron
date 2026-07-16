# Neutron Julia

Nucleus database client for Julia — all 14 data models over pgwire, typed for multiple dispatch, with native bridges into the scientific-computing and ML ecosystem.

## Philosophy

Light core, modular data models. `NeutronJulia` connects to Nucleus (or plain PostgreSQL) over a single pgwire connection via LibPQ.jl. SQL works against any Postgres; every other model is a Nucleus SQL function surfaced as a typed Julia method. Ecosystem integrations (DataFrames, DifferentialEquations, Flux, CUDA, ...) ship as package extensions — zero weight until you `using` the corresponding package.

## Install

```julia
using Pkg
Pkg.add(url="https://github.com/neutron-build/neutron", subdir="julia")

# Or, from a local clone:
Pkg.develop(path="path/to/neutron/julia")
```

Requires Julia 1.9+ (package extensions). Core dependencies are `LibPQ`, `JSON3`, `StructTypes`, and `Tables`.

## Connect + Query

`connect` opens a pgwire connection and auto-detects Nucleus capabilities via `VERSION()`. Model accessors return typed handles so every operation resolves through multiple dispatch:

```julia
using NeutronJulia

client = NeutronJulia.connect("postgres://localhost:5432/mydb")

# SQL — works against Nucleus or plain PostgreSQL
s = sql(client)
rows = query(s, "SELECT id, name FROM users WHERE age > \$1", 18)  # columntable NamedTuple
row  = query_one(s, "SELECT * FROM users WHERE id = \$1", 42)       # NamedTuple, throws NucleusError(404) if empty
n    = execute!(s, "UPDATE users SET name = \$1 WHERE id = \$2", "Alicia", 1)  # affected rows

close(client)
```

`query` returns a Tables.jl columntable (a NamedTuple of column vectors); with DataFrames.jl loaded you can wrap it directly in a `DataFrame`.

## Typed Model Handles

Each accessor returns a lightweight handle bound to the connection — no global state, and the same call works on a `NucleusClient` or a transaction:

```julia
kv(client)         # KVModel
vector(client)     # VectorModel
timeseries(client) # TimeSeriesModel
document(client); graph(client); fts(client); geo(client)
blob(client); streams(client); columnar(client); datalog(client); cdc(client); pubsub(client)
```

## All 14 Data Models

```julia
# Key-Value — full Redis-compatible surface (base, lists, hashes, sets, sorted sets, HyperLogLog)
k = kv(client)
set!(k, "session:abc", "hello"; ttl=3600)   # kv_set!
get(k, "session:abc")                         # kv_get -> "hello" (get/delete! extend Base)
incr!(k, "page:views")                        # kv_incr!
rpush!(k, "queue", "job1"); lrange(k, "queue", 0, -1)
hset!(k, "user:1", "name", "Alice"); hget(k, "user:1", "name")
sadd!(k, "tags", "julia"); zadd!(k, "board", 100.0, "player1")

# Vector search — Julia arrays are native embeddings
vec = vector(client)
create_index!(vec, "docs"; column="embedding", metric=Cosine)
hits = search(vec, "docs", Float32[0.1, 0.2, 0.3]; k=10, metric=Cosine)  # Vector{SearchResult}
d = vector_distance(vec, a, b; metric=L2)     # also cosine_distance, inner_product, dims

# TimeSeries — time-stamped float series
ts = timeseries(client)
insert!(ts, "sensor:temp", round(Int64, time() * 1000), 23.5)
insert!(ts, "sensor:temp", timestamps_ms, values)   # batch, transaction-wrapped
last_value(ts, "sensor:temp"); range_avg(ts, "sensor:temp", start_ms, end_ms)
set_retention!(ts, "sensor:temp", 30)

# Graph (Cypher)
g = graph(client)
alice = add_node!(g, "Person"; properties=Dict("name" => "Alice"))
add_edge!(g, alice, bob, "KNOWS")
neighbors(g, alice); shortest_path(g, alice, bob)

# Full-text search, Geo, Document, Blob, Streams, Columnar, Datalog, CDC, Pub/Sub
search(fts(client), "machine learning"; limit=10)
distance(geo(client), a, b); within(geo(client), a, b, 500.0)   # points + radius (metres)
publish!(pubsub(client), "notifications", "payload")
```

Every non-SQL model calls `require_nucleus` first, so running one against plain PostgreSQL throws a `NotNucleusError` rather than a confusing SQL error.

## Ecosystem Bridges

These load automatically as package extensions the moment you `using` the matching package alongside `NeutronJulia` (Julia 1.9+). All are implemented in `ext/`:

| Package | Extension | Provides |
|---------|-----------|----------|
| DataFrames.jl | `NeutronJuliaDataFramesExt` | `query_df`, `insert_dataframe!`, `DataFrame(result)` conversion |
| DifferentialEquations.jl | `NeutronJuliaDiffEqExt` | `store!(ts, sol, prefix)` / `load_solution` — ODE/SDE/DAE solutions in TimeSeries |
| ModelingToolkit.jl | `NeutronJuliaMTKExt` | `store!(ts, sol, sys, prefix)` — symbolic variable names as series |
| Graphs.jl | `NeutronJuliaGraphsExt` | `to_graphs_jl` / `import_from_graphs_jl!` — Nucleus Graph ↔ `SimpleDiGraph` |
| Flux.jl | `NeutronJuliaFluxExt` | `embed_and_store!` — run inputs through a model, store embeddings in Vector |
| CUDA.jl | `NeutronJuliaCUDAExt` | `load_embeddings_gpu`, `gpu_batch_cosine`, `gpu_topk` — GPU similarity search |
| Makie.jl | `NeutronJuliaMakieExt` | `plot_timeseries!` — plot a TimeSeries series onto a Makie `Axis` |

```julia
using DifferentialEquations, NeutronJulia

client = NeutronJulia.connect("postgres://localhost:5432/mydb")
ts = timeseries(client)

prob = ODEProblem(lorenz!, u0, tspan, p)
sol  = solve(prob, Tsit5(), saveat=0.01)

store!(ts, sol, "lorenz:run1"; variable_names=["x", "y", "z"])
t, u = load_solution(ts, "lorenz:run1", ["x", "y", "z"])
```

Runnable end-to-end scripts live in `examples/` (`lorenz.jl`, `sensor_pipeline.jl`, `graph_analysis.jl`).

## Transactions

`transaction` runs a do-block, commits on success, and rolls back on any exception. All 14 model accessors are available on the `tx` handle:

```julia
transaction(client) do tx
    execute!(sql(tx), "INSERT INTO users (name) VALUES (\$1)", "Alice")
    set!(kv(tx), "user:alice", "active")
end
```

## Connection Pool

A thread-safe pool of pre-established connections, backed by a `Channel`. `with_pool` guarantees release even if the body throws, and dead connections are transparently replaced on release:

```julia
pool = ConnectionPool("postgres://localhost:5432/mydb"; size=4)

with_pool(pool) do client
    query(sql(client), "SELECT count(*) FROM events")
end

idle_count(pool)   # connections currently available
close(pool)
```

Lower-level `acquire(pool)` / `release(pool, client)` are available when you need manual control.

## Feature Detection

`connect` populates a `NucleusFeatures` struct from the server's `VERSION()` string. When connected to plain PostgreSQL every model flag is `false`, and Nucleus-only operations raise `NotNucleusError`. Server-side problems surface as `NucleusError`, which carries RFC 7807 fields (`type`, `title`, `status`, `detail`) consistent with every other Neutron SDK.

## Testing

```bash
julia --project=. -e 'import Pkg; Pkg.test()'
```

Unit tests (error types, feature detection, the `require_nucleus` guard, value types, enums, and model/struct layout) run with no database. Integration tests are gated on environment variables — set `NUCLEUS_TEST_URL` (or `POSTGRES_TEST_URL`) to exercise SQL, and `NUCLEUS_TEST_NUCLEUS=true` to run the KV, TimeSeries, and transaction suites against a live Nucleus instance.

## File Structure

```
julia/
├── src/
│   ├── NeutronJulia.jl     # module, includes, exports
│   ├── client.jl           # connect, NucleusClient, transaction, model accessors
│   ├── features.jl         # VERSION() feature detection
│   ├── errors.jl           # NucleusError (RFC 7807), NotNucleusError, require_nucleus
│   ├── pool.jl             # ConnectionPool, with_pool
│   ├── models/             # sql, kv, vector, timeseries, document, graph, fts,
│   │                       #   geo, blob, streams, columnar, datalog, cdc, pubsub
│   └── utils/              # types, gpu helpers
├── ext/                    # 7 package extensions (DataFrames, DiffEq, MTK, Graphs,
│                           #   Flux, CUDA, Makie)
├── examples/               # lorenz.jl, sensor_pipeline.jl, graph_analysis.jl
├── test/runtests.jl
└── Project.toml
```

## Requirements

- Julia 1.9+
- Nucleus running (local or remote, pgwire port 5432) for the non-SQL models

## License

MIT.
