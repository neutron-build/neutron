"""
KV model — full Redis-compatible API via KV_* SQL functions.
Covers: base ops, list, hash, set, sorted set, HyperLogLog.
"""
struct KVModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

# ── Base Operations ───────────────────────────────────────────────────────────

"""KV_GET(key) → value or nothing"""
function kv_get(m::KVModel, key::String)::Union{String, Nothing}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_GET(\$1)", [key])
    val = first(result)[1]
    return ismissing(val) ? nothing : val
end

"""KV_SET(key, value [, ttl_secs]) → nothing"""
function kv_set!(m::KVModel, key::String, value; ttl::Union{Int, Nothing}=nothing)
    require_nucleus(m.features, "KV")
    if ttl === nothing
        LibPQ.execute(m.conn, "SELECT KV_SET(\$1, \$2)", [key, string(value)])
    else
        LibPQ.execute(m.conn, "SELECT KV_SET(\$1, \$2, \$3)", [key, string(value), ttl])
    end
    return nothing
end

"""KV_SETNX(key, value) → Bool (true if key was set)"""
function kv_setnx!(m::KVModel, key::String, value)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_SETNX(\$1, \$2)", [key, string(value)])
    return _bool(result)
end

"""KV_DEL(key) → Bool"""
function kv_delete!(m::KVModel, key::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_DEL(\$1)", [key])
    return _bool(result)
end

"""KV_EXISTS(key) → Bool"""
function kv_exists(m::KVModel, key::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_EXISTS(\$1)", [key])
    return _bool(result)
end

"""KV_INCR(key [, amount]) → Int64 new value"""
function kv_incr!(m::KVModel, key::String; by::Int=1)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_INCR(\$1, \$2)", [key, by])
    return _int(result)
end

"""KV_TTL(key) → Int64 (-1=no TTL, -2=missing)"""
function kv_ttl(m::KVModel, key::String)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_TTL(\$1)", [key])
    return _int(result)
end

"""KV_EXPIRE(key, ttl_secs) → Bool"""
function kv_expire!(m::KVModel, key::String, ttl_secs::Int)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_EXPIRE(\$1, \$2)", [key, ttl_secs])
    return _bool(result)
end

"""KV_DBSIZE() → Int64"""
function kv_dbsize(m::KVModel)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_DBSIZE()")
    return _int(result)
end

"""KV_FLUSHDB() → nothing"""
function kv_flushdb!(m::KVModel)
    require_nucleus(m.features, "KV")
    LibPQ.execute(m.conn, "SELECT KV_FLUSHDB()")
    return nothing
end

# ── List Operations ───────────────────────────────────────────────────────────

"""KV_LPUSH(key, value) → Int64 list length"""
function lpush!(m::KVModel, key::String, value)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_LPUSH(\$1, \$2)", [key, string(value)])
    return _int(result)
end

"""KV_RPUSH(key, value) → Int64 list length"""
function rpush!(m::KVModel, key::String, value)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_RPUSH(\$1, \$2)", [key, string(value)])
    return _int(result)
end

"""KV_LPOP(key) → value or nothing"""
function lpop!(m::KVModel, key::String)::Union{String, Nothing}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_LPOP(\$1)", [key])
    val = first(result)[1]
    return ismissing(val) ? nothing : val
end

"""KV_RPOP(key) → value or nothing"""
function rpop!(m::KVModel, key::String)::Union{String, Nothing}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_RPOP(\$1)", [key])
    val = first(result)[1]
    return ismissing(val) ? nothing : val
end

"""KV_LRANGE(key, start, stop) → Vector{String}"""
function lrange(m::KVModel, key::String, start::Int, stop::Int)::Vector{String}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_LRANGE(\$1, \$2, \$3)", [key, start, stop])
    return _split_csv(first(result)[1])
end

"""KV_LLEN(key) → Int64"""
function llen(m::KVModel, key::String)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_LLEN(\$1)", [key])
    return _int(result)
end

"""KV_LINDEX(key, index) → value or nothing"""
function lindex(m::KVModel, key::String, idx::Int)::Union{String, Nothing}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_LINDEX(\$1, \$2)", [key, idx])
    val = first(result)[1]
    return ismissing(val) ? nothing : val
end

# ── Hash Operations ───────────────────────────────────────────────────────────

"""KV_HSET(key, field, value) → Bool"""
function hset!(m::KVModel, key::String, field::String, value)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_HSET(\$1, \$2, \$3)",
                           [key, field, string(value)])
    return _bool(result)
end

"""KV_HGET(key, field) → value or nothing"""
function hget(m::KVModel, key::String, field::String)::Union{String, Nothing}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_HGET(\$1, \$2)", [key, field])
    val = first(result)[1]
    return ismissing(val) ? nothing : val
end

"""KV_HDEL(key, field) → Bool"""
function hdel!(m::KVModel, key::String, field::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_HDEL(\$1, \$2)", [key, field])
    return _bool(result)
end

"""KV_HEXISTS(key, field) → Bool"""
function hexists(m::KVModel, key::String, field::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_HEXISTS(\$1, \$2)", [key, field])
    return _bool(result)
end

"""KV_HGETALL(key) → Dict{String,String}"""
function hgetall(m::KVModel, key::String)::Dict{String, String}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_HGETALL(\$1)", [key])
    raw = first(result)[1]
    ismissing(raw) && return Dict{String, String}()
    pairs_raw = split(raw, ",")
    return Dict(split(p, "=")[1] => split(p, "=")[2] for p in pairs_raw if occursin("=", p))
end

"""KV_HLEN(key) → Int64"""
function hlen(m::KVModel, key::String)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_HLEN(\$1)", [key])
    return _int(result)
end

# ── Set Operations ────────────────────────────────────────────────────────────

"""KV_SADD(key, member) → Bool"""
function sadd!(m::KVModel, key::String, member::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_SADD(\$1, \$2)", [key, member])
    return _bool(result)
end

"""KV_SREM(key, member) → Bool"""
function srem!(m::KVModel, key::String, member::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_SREM(\$1, \$2)", [key, member])
    return _bool(result)
end

"""KV_SMEMBERS(key) → Set{String}"""
function smembers(m::KVModel, key::String)::Set{String}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_SMEMBERS(\$1)", [key])
    raw = first(result)[1]
    ismissing(raw) && return Set{String}()
    return Set(split(raw, ","))
end

"""KV_SISMEMBER(key, member) → Bool"""
function sismember(m::KVModel, key::String, member::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_SISMEMBER(\$1, \$2)", [key, member])
    return _bool(result)
end

"""KV_SCARD(key) → Int64"""
function scard(m::KVModel, key::String)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_SCARD(\$1)", [key])
    return _int(result)
end

# ── Sorted Set Operations ─────────────────────────────────────────────────────

"""KV_ZADD(key, score, member) → Bool"""
function zadd!(m::KVModel, key::String, score::Float64, member::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_ZADD(\$1, \$2, \$3)", [key, score, member])
    return _bool(result)
end

"""KV_ZRANGE(key, start, stop) → Vector{String}"""
function zrange(m::KVModel, key::String, start::Int, stop::Int)::Vector{String}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_ZRANGE(\$1, \$2, \$3)", [key, start, stop])
    return _split_csv(first(result)[1])
end

"""KV_ZRANGEBYSCORE(key, min, max) → Vector{String}"""
function zrangebyscore(m::KVModel, key::String, min_score::Float64, max_score::Float64)::Vector{String}
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_ZRANGEBYSCORE(\$1, \$2, \$3)",
                           [key, min_score, max_score])
    return _split_csv(first(result)[1])
end

"""KV_ZREM(key, member) → Bool"""
function zrem!(m::KVModel, key::String, member::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_ZREM(\$1, \$2)", [key, member])
    return _bool(result)
end

"""KV_ZCARD(key) → Int64"""
function zcard(m::KVModel, key::String)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_ZCARD(\$1)", [key])
    return _int(result)
end

# ── HyperLogLog ───────────────────────────────────────────────────────────────

"""KV_PFADD(key, element) → Bool"""
function pfadd!(m::KVModel, key::String, element::String)::Bool
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_PFADD(\$1, \$2)", [key, element])
    return _bool(result)
end

"""KV_PFCOUNT(key) → Int64 approx distinct count"""
function pfcount(m::KVModel, key::String)::Int64
    require_nucleus(m.features, "KV")
    result = LibPQ.execute(m.conn, "SELECT KV_PFCOUNT(\$1)", [key])
    return _int(result)
end

# ── Convenience aliases matching the PLAN's public API naming ─────────────────
# These delegate to the kv_* prefixed versions above.

"""get(m::KVModel, key) → String or nothing"""
get(m::KVModel, key::String) = kv_get(m, key)

"""set!(m::KVModel, key, value; ttl) → nothing"""
set!(m::KVModel, key::String, value; ttl::Union{Int, Nothing}=nothing) =
    kv_set!(m, key, value; ttl=ttl)

"""setnx!(m::KVModel, key, value) → Bool"""
setnx!(m::KVModel, key::String, value) = kv_setnx!(m, key, value)

"""delete!(m::KVModel, key) → Bool"""
delete!(m::KVModel, key::String) = kv_delete!(m, key)

"""exists(m::KVModel, key) → Bool"""
exists(m::KVModel, key::String) = kv_exists(m, key)

"""incr!(m::KVModel, key; by) → Int64"""
incr!(m::KVModel, key::String; by::Int=1) = kv_incr!(m, key; by=by)

"""ttl(m::KVModel, key) → Int64"""
ttl(m::KVModel, key::String) = kv_ttl(m, key)

"""expire!(m::KVModel, key, ttl_secs) → Bool"""
expire!(m::KVModel, key::String, ttl_secs::Int) = kv_expire!(m, key, ttl_secs)

"""dbsize(m::KVModel) → Int64"""
dbsize(m::KVModel) = kv_dbsize(m)

"""flushdb!(m::KVModel) → nothing"""
flushdb!(m::KVModel) = kv_flushdb!(m)
