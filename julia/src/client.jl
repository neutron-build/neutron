"""
    NucleusClient

Connection wrapper around LibPQ.jl with auto-detected Nucleus capabilities.
"""
mutable struct NucleusClient
    conn::LibPQ.Connection
    features::NucleusFeatures
    url::String
end

"""
    connect(url; kwargs...) -> NucleusClient

Connect to a Nucleus (or plain PostgreSQL) instance over pgwire.
Auto-detects Nucleus capabilities on connect via VERSION().

# Example
```julia
client = NeutronJulia.connect("postgres://localhost:5432/mydb")
client = NeutronJulia.connect("postgres://user:pass@host:5432/db")
```
"""
function connect(url::String; kwargs...)::NucleusClient
    conn = LibPQ.Connection(url; kwargs...)
    features = detect_features(conn)
    return NucleusClient(conn, features, url)
end

Base.close(c::NucleusClient) = close(c.conn)
Base.isopen(c::NucleusClient) = isopen(c.conn)

# ── Model accessors (return typed handles for multiple dispatch) ─────────────

sql(c::NucleusClient)        = SQLModel(c.conn)
kv(c::NucleusClient)         = KVModel(c.conn, c.features)
vector(c::NucleusClient)     = VectorModel(c.conn, c.features)
timeseries(c::NucleusClient) = TimeSeriesModel(c.conn, c.features)
document(c::NucleusClient)   = DocumentModel(c.conn, c.features)
graph(c::NucleusClient)      = GraphModel(c.conn, c.features)
fts(c::NucleusClient)        = FTSModel(c.conn, c.features)
geo(c::NucleusClient)        = GeoModel(c.conn, c.features)
blob(c::NucleusClient)       = BlobModel(c.conn, c.features)
streams(c::NucleusClient)    = StreamsModel(c.conn, c.features)
columnar(c::NucleusClient)   = ColumnarModel(c.conn, c.features)
datalog(c::NucleusClient)    = DatalogModel(c.conn, c.features)
cdc(c::NucleusClient)        = CDCModel(c.conn, c.features)
pubsub(c::NucleusClient)     = PubSubModel(c.conn, c.features)

# ── Transactions ─────────────────────────────────────────────────────────────

"""Cross-model transaction wrapping LibPQ BEGIN/COMMIT/ROLLBACK."""
mutable struct NucleusTransaction
    conn::LibPQ.Connection
    features::NucleusFeatures
    active::Bool
end

"""
    transaction(f, client) -> result

Run `f(tx)` inside a database transaction. Commits on success, rolls back on
any exception. All 14 model accessors are available on `tx`.

# Example
```julia
transaction(client) do tx
    execute!(sql(tx), "INSERT INTO users (name) VALUES (\$1)", "Alice")
    set!(kv(tx), "user:alice", "active")
end
```
"""
function transaction(f::Function, client::NucleusClient)
    LibPQ.execute(client.conn, "BEGIN")
    tx = NucleusTransaction(client.conn, client.features, true)
    try
        result = f(tx)
        LibPQ.execute(client.conn, "COMMIT")
        tx.active = false
        return result
    catch e
        LibPQ.execute(client.conn, "ROLLBACK")
        tx.active = false
        rethrow(e)
    end
end

# Transaction model accessors (same as NucleusClient)
sql(tx::NucleusTransaction)        = SQLModel(tx.conn)
kv(tx::NucleusTransaction)         = KVModel(tx.conn, tx.features)
vector(tx::NucleusTransaction)     = VectorModel(tx.conn, tx.features)
timeseries(tx::NucleusTransaction) = TimeSeriesModel(tx.conn, tx.features)
document(tx::NucleusTransaction)   = DocumentModel(tx.conn, tx.features)
graph(tx::NucleusTransaction)      = GraphModel(tx.conn, tx.features)
fts(tx::NucleusTransaction)        = FTSModel(tx.conn, tx.features)
geo(tx::NucleusTransaction)        = GeoModel(tx.conn, tx.features)
blob(tx::NucleusTransaction)       = BlobModel(tx.conn, tx.features)
streams(tx::NucleusTransaction)    = StreamsModel(tx.conn, tx.features)
columnar(tx::NucleusTransaction)   = ColumnarModel(tx.conn, tx.features)
datalog(tx::NucleusTransaction)    = DatalogModel(tx.conn, tx.features)
cdc(tx::NucleusTransaction)        = CDCModel(tx.conn, tx.features)
pubsub(tx::NucleusTransaction)     = PubSubModel(tx.conn, tx.features)
