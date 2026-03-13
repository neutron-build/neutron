"""
    NeutronJulia

Scientific computing SDK for the Nucleus database. Provides idiomatic Julia
access to all 14 Nucleus data models over pgwire (LibPQ.jl transport).

# Quick start
```julia
using NeutronJulia

client = NeutronJulia.connect("postgres://localhost:5432/mydb")

# Key-Value
k = kv(client)
set!(k, "session:abc", "hello", ttl=3600)
val = get(k, "session:abc")  # "hello"

# TimeSeries
ts = timeseries(client)
insert!(ts, "sensor:temp", round(Int64, time() * 1000), 23.5)

# Vector search
vec = vector(client)
results = search(vec, "embeddings", Float32[0.1, 0.2, 0.3]; k=10, metric=Cosine)

# SQL
s = sql(client)
rows = query(s, "SELECT * FROM users WHERE id = \$1", 42)

close(client)
```

Ecosystem integrations load automatically as package extensions when the
corresponding package is `using`-ed:
- DataFrames.jl  → DataFrame results from SQL/Columnar
- DifferentialEquations.jl → store ODE/SDE solutions in TimeSeries
- ModelingToolkit.jl → symbolic variable names in TimeSeries
- Graphs.jl → bidirectional Nucleus Graph ↔ SimpleDiGraph
- Flux.jl → embedding generation + Vector storage
- CUDA.jl → GPU-accelerated vector similarity search
- Makie.jl → plot_timeseries! recipe
"""
module NeutronJulia

using LibPQ
using JSON3
using StructTypes
using Tables

# Extend Base functions so our typed methods don't create new globals.
# This must come before any include() that defines methods on these functions.
import Base: get, delete!, insert!, close, isopen

# ── Core ─────────────────────────────────────────────────────────────────────
include("errors.jl")
include("features.jl")
include("utils/types.jl")
include("client.jl")

# ── Data Models (all 14) ─────────────────────────────────────────────────────
include("models/sql.jl")
include("models/kv.jl")
include("models/vector.jl")
include("models/timeseries.jl")
include("models/document.jl")
include("models/graph.jl")
include("models/fts.jl")
include("models/geo.jl")
include("models/blob.jl")
include("models/streams.jl")
include("models/columnar.jl")
include("models/datalog.jl")
include("models/cdc.jl")
include("models/pubsub.jl")

# ── Connection Pool ───────────────────────────────────────────────────────────
include("pool.jl")

# ── Exports ───────────────────────────────────────────────────────────────────

# Error types + guard function
export NucleusError, NotNucleusError, require_nucleus

# Client + transaction + pool
export NucleusClient, NucleusFeatures, NucleusTransaction
export connect, transaction
export ConnectionPool, acquire, release, with_pool, idle_count

# Model handle types
export SQLModel, KVModel, VectorModel, TimeSeriesModel
export DocumentModel, GraphModel, FTSModel, GeoModel
export BlobModel, StreamsModel, ColumnarModel, DatalogModel
export CDCModel, PubSubModel

# Model accessors (client → model handle)
export sql, kv, vector, timeseries, document, graph, fts, geo
export blob, streams, columnar, datalog, cdc, pubsub

# Value types
export TimeSeriesPoint, SearchResult, FTSResult
export GeoPoint, GraphNode, GraphEdge, BlobMeta
export DistanceMetric, L2, Cosine, InnerProduct
export GraphDirection, GraphOut, GraphIn, GraphBoth

# SQL model
export query, query_one, execute!

# KV model (kv_* prefix exports avoid clashing with Base.get / Base.delete!)
export kv_get, kv_set!, kv_setnx!, kv_delete!, kv_exists, kv_incr!
export kv_ttl, kv_expire!, kv_dbsize, kv_flushdb!
export lpush!, rpush!, lpop!, rpop!, lrange, llen, lindex
export hset!, hget, hdel!, hexists, hgetall, hlen
export sadd!, srem!, smembers, sismember, scard
export zadd!, zrange, zrangebyscore, zrem!, zcard
export pfadd!, pfcount
# Non-conflicting convenience aliases
export set!, setnx!, exists, incr!, ttl, expire!, dbsize, flushdb!
# Note: `get` and `delete!` extend Base — do NOT re-export; use kv_get / kv_delete!

# Vector model
export search, to_vector_literal, dims, cosine_distance, inner_product
export vector_distance, create_index!

# TimeSeries model
# Note: `insert!` extends Base — do NOT re-export at top level; it is dispatched correctly
export last_value, ts_count, range_count, range_avg
export set_retention!, match_pattern, time_bucket

# Document model
export doc_get, doc_query, doc_path, doc_count

# Graph model
export add_node!, add_edge!, delete_node!, delete_edge!
export graph_query, neighbors, shortest_path, node_count, edge_count

# FTS model
export index!, fuzzy_search, remove!, fts_doc_count, fts_term_count

# Geo model
export distance, distance_euclidean, within, area, makepoint, st_x, st_y

# Blob model
export store!, blob_get, blob_delete!, meta, tag!, list, blob_count, dedup_ratio

# Streams model
export xadd!, xlen, xrange, xread, xgroup_create!, xreadgroup, xack!

# Columnar model
export columnar_count, columnar_sum, columnar_avg, columnar_min, columnar_max

# Datalog model
export assert_fact!, retract!, rule!, datalog_query, clear!, import_graph!

# CDC model
export cdc_read, cdc_count, cdc_table_read

# PubSub model
export publish!, channels, subscriber_count, subscribe

end # module NeutronJulia
