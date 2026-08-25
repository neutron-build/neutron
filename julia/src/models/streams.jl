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
    return _entry_id(first(result)[1])
end

# The engine answers STREAM_XADD with the entry id on success; failures are
# statement errors LibPQ raises. A NULL cell is a contract violation, not "".
function _entry_id(raw)::String
    ismissing(raw) && error("STREAM_XADD returned NULL; success always carries an entry id")
    return raw
end

# STREAM_XRANGE/STREAM_XREAD answer "" for a missing stream — an empty TEXT
# cell, which LibPQ delivers as an empty String, not missing. NULL never
# occurs on these paths; a NULL cell is a contract violation.
function _stream_entries(raw, fn_name)::Vector{Dict{String,Any}}
    ismissing(raw) && error(fn_name, " returned NULL; expected \"\" for a missing stream or a JSON array")
    isempty(raw) && return Dict{String,Any}[]
    return JSON3.read(raw, Vector{Dict{String, Any}})
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
    return _stream_entries(first(result)[1], "STREAM_XRANGE")
end

"""STREAM_XREAD(stream, last_id_ms, count) → Vector{Dict{String,Any}}"""
function xread(m::StreamsModel, stream::String, last_id_ms::Int64;
               count::Int=100)::Vector{Dict{String, Any}}
    require_nucleus(m.features, "Streams")
    result = LibPQ.execute(m.conn,
        "SELECT STREAM_XREAD(\$1, \$2, \$3)",
        [stream, last_id_ms, count])
    return _stream_entries(first(result)[1], "STREAM_XREAD")
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

# STREAM_XREADGROUP answers Value::Text on its only Ok arm — a JSON array,
# "[]" when caught up (stream_entries_to_json) — and a missing group is a
# NOGROUP statement error LibPQ raises, so NULL and "" are both contract
# violations.
function _group_entries(raw)::Vector{Dict{String,Any}}
    ismissing(raw) && error("STREAM_XREADGROUP returned NULL; a missing group is a NOGROUP statement error")
    isempty(raw) && error("STREAM_XREADGROUP returned an empty payload; a missing group should raise NOGROUP instead")
    return JSON3.read(raw, Vector{Dict{String,Any}})
end

"""STREAM_XREADGROUP(stream, group, consumer, count) → Vector{Dict{String,Any}}"""
function xreadgroup(m::StreamsModel, stream::String, group::String,
                    consumer::String; count::Int=10)::Vector{Dict{String,Any}}
    require_nucleus(m.features, "Streams")
    result = LibPQ.execute(m.conn,
        "SELECT STREAM_XREADGROUP(\$1, \$2, \$3, \$4)",
        [stream, group, consumer, count])
    return _group_entries(first(result)[1])
end

"""STREAM_XACK(stream, group, id_ms, id_seq) → Bool"""
# Takes the "<ms>-<seq>" string xadd! returns.
#
# This took id_ms and id_seq as separate integers, so the two ends of the same
# API did not compose: every caller had to split xadd!'s return value itself.
# The engine accepts the joined form since 2026-08-16 and all five SDKs now use
# it.
function xack!(m::StreamsModel, stream::String, group::String, entry_id::String)::Int64
    require_nucleus(m.features, "Streams")
    result = LibPQ.execute(m.conn,
        "SELECT STREAM_XACK(\$1, \$2, \$3)",
        [stream, group, entry_id])
    return _int(result)
end
