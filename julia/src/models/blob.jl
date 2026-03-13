"""Blob model — binary large objects via BLOB_* SQL functions."""

struct BlobModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

struct BlobMeta
    key::String
    size::Int64
    content_type::String
    tags::Dict{String, String}
end

"""BLOB_STORE(key, data_hex [, content_type]) → nothing"""
function store!(m::BlobModel, key::String, data::Vector{UInt8};
                content_type::String="application/octet-stream")
    require_nucleus(m.features, "Blob")
    hex = bytes2hex(data)
    LibPQ.execute(m.conn, "SELECT BLOB_STORE(\$1, \$2, \$3)", [key, hex, content_type])
    return nothing
end

"""BLOB_GET(key) → Vector{UInt8} or nothing"""
function blob_get(m::BlobModel, key::String)::Union{Vector{UInt8}, Nothing}
    require_nucleus(m.features, "Blob")
    result = LibPQ.execute(m.conn, "SELECT BLOB_GET(\$1)", [key])
    val = first(result)[1]
    return ismissing(val) ? nothing : hex2bytes(val)
end

"""BLOB_DELETE(key) → Bool"""
function blob_delete!(m::BlobModel, key::String)::Bool
    require_nucleus(m.features, "Blob")
    result = LibPQ.execute(m.conn, "SELECT BLOB_DELETE(\$1)", [key])
    return _bool(result)
end

"""BLOB_META(key) → Dict{String,Any} or nothing"""
function meta(m::BlobModel, key::String)::Union{Dict{String, Any}, Nothing}
    require_nucleus(m.features, "Blob")
    result = LibPQ.execute(m.conn, "SELECT BLOB_META(\$1)", [key])
    val = first(result)[1]
    return ismissing(val) ? nothing : JSON3.read(val, Dict{String, Any})
end

"""BLOB_TAG(key, tag_key, tag_value) → Bool"""
function tag!(m::BlobModel, key::String, tag_key::String, tag_value::String)::Bool
    require_nucleus(m.features, "Blob")
    result = LibPQ.execute(m.conn, "SELECT BLOB_TAG(\$1, \$2, \$3)",
                           [key, tag_key, tag_value])
    return _bool(result)
end

"""BLOB_LIST([prefix]) → Vector{Dict{String,Any}}"""
function list(m::BlobModel; prefix::Union{String, Nothing}=nothing)::Vector{Dict{String, Any}}
    require_nucleus(m.features, "Blob")
    if prefix === nothing
        result = LibPQ.execute(m.conn, "SELECT BLOB_LIST()")
    else
        result = LibPQ.execute(m.conn, "SELECT BLOB_LIST(\$1)", [prefix])
    end
    val = first(result)[1]
    ismissing(val) && return Dict{String,Any}[]
    return JSON3.read(val, Vector{Dict{String, Any}})
end

"""BLOB_COUNT() → Int64"""
function blob_count(m::BlobModel)::Int64
    require_nucleus(m.features, "Blob")
    result = LibPQ.execute(m.conn, "SELECT BLOB_COUNT()")
    return _int(result)
end

"""BLOB_DEDUP_RATIO() → Float64"""
function dedup_ratio(m::BlobModel)::Float64
    require_nucleus(m.features, "Blob")
    result = LibPQ.execute(m.conn, "SELECT BLOB_DEDUP_RATIO()")
    v = _float(result)
    return v === nothing ? 1.0 : v
end

# Public aliases matching PLAN naming
get(m::BlobModel, key::String) = blob_get(m, key)
delete!(m::BlobModel, key::String) = blob_delete!(m, key)
