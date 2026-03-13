"""Streams model — append-only logs via STREAM_X* SQL functions."""

struct StreamsModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

"""STREAM_XADD(stream, field1, val1, ...) → String entry ID"""
function xadd!(m::StreamsModel, stream::String, fields::Pair{String}...)::String
    require_nucleus(m.features, "Streams")
    parts = String[]
    for (k, v) in fields
        push!(parts, k)
        push!(parts, string(v))
    end
    n = length(parts) + 1
    placeholders = join(["\$$(i)" for i in 1:n], ", ")
    sql_str = "SELECT STREAM_XADD($placeholders)"
    result = LibPQ.execute(m.conn, sql_str, [stream, parts...])
    val = first(result)[1]
    return ismissing(val) ? "" : val
end

"""STREAM_XLEN(stream) → Int64"""
function xlen(m::StreamsModel, stream::String)::Int64
    require_nucleus(m.features, "Streams")
    result = LibPQ.execute(m.conn, "SELECT STREAM_XLEN(\$1)", [stream])
    return _int(result)
end

"""STREAM_XRANGE(stream, start_ms, end_ms, count) → Vector{Dict{String,Any}}"""
function xrange(m::StreamsModel, stream::String,
                start_ms::Int64, end_ms::Int64;
                count::Int=100)::Vector{Dict{String, Any}}
    require_nucleus(m.features, "Streams")
    result = LibPQ.execute(m.conn,
        "SELECT STREAM_XRANGE(\$1, \$2, \$3, \$4)",
        [stream, start_ms, end_ms, count])
    val = first(result)[1]
    ismissing(val) && return Dict{String,Any}[]
    return JSON3.read(val, Vector{Dict{String, Any}})
end

"""STREAM_XREAD(stream, last_id_ms, count) → Vector{Dict{String,Any}}"""
function xread(m::StreamsModel, stream::String, last_id_ms::Int64;
               count::Int=100)::Vector{Dict{String, Any}}
    require_nucleus(m.features, "Streams")
    result = LibPQ.execute(m.conn,
        "SELECT STREAM_XREAD(\$1, \$2, \$3)",
        [stream, last_id_ms, count])
    val = first(result)[1]
    ismissing(val) && return Dict{String,Any}[]
    return JSON3.read(val, Vector{Dict{String, Any}})
end

"""STREAM_XGROUP_CREATE(stream, group, start_id) → Bool"""
function xgroup_create!(m::StreamsModel, stream::String,
                        group::String, start_id::Int64)::Bool
    require_nucleus(m.features, "Streams")
    result = LibPQ.execute(m.conn,
        "SELECT STREAM_XGROUP_CREATE(\$1, \$2, \$3)",
        [stream, group, start_id])
    return _bool(result)
end

"""STREAM_XREADGROUP(stream, group, consumer, count) → String (JSON)"""
function xreadgroup(m::StreamsModel, stream::String, group::String,
                    consumer::String; count::Int=10)::String
    require_nucleus(m.features, "Streams")
    result = LibPQ.execute(m.conn,
        "SELECT STREAM_XREADGROUP(\$1, \$2, \$3, \$4)",
        [stream, group, consumer, count])
    val = first(result)[1]
    return ismissing(val) ? "[]" : val
end

"""STREAM_XACK(stream, group, id_ms, id_seq) → Bool"""
function xack!(m::StreamsModel, stream::String, group::String,
               id_ms::Int64, id_seq::Int64)::Bool
    require_nucleus(m.features, "Streams")
    result = LibPQ.execute(m.conn,
        "SELECT STREAM_XACK(\$1, \$2, \$3, \$4)",
        [stream, group, id_ms, id_seq])
    return _bool(result)
end
