"""CDC model — Change Data Capture via CDC_* SQL functions."""

struct CDCModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

"""CDC_READ(after_sequence, limit) → String (JSON change events)

Sent ONE argument, and the engine requires two: every call failed with
"CDC_READ requires (after_sequence, limit)". The function had never worked, and
nothing noticed because nothing executed it against a live engine.
"""
function cdc_read(m::CDCModel, after_sequence::Int64, limit::Int64=100)
    require_nucleus(m.features, "CDC")
    result = LibPQ.execute(m.conn, "SELECT CDC_READ(\$1, \$2)", [after_sequence, limit])
    val = first(result)[1]
    return _cdc_events(val)
end

"""Decode the engine's event array into a Vector of Dicts.

Returned the raw JSON string, so a caller had to parse it and the cross-SDK case
asserting a list passed against a non-empty string — a non-empty string is
truthy. Go and Rust were fixed the same way on 2026-08-15.

An empty result is an empty vector, never an error: "no changes since that
sequence" is the common case. A malformed payload IS an error, because silently
reporting "no changes" for something unparseable is the bug class CDC exists to
detect.
"""
function _cdc_events(raw)
    (ismissing(raw) || raw === nothing || isempty(raw)) && return Dict{String, Any}[]
    return [Dict{String, Any}(String(k) => v for (k, v) in pairs(e)) for e in JSON3.read(raw)]
end

"""CDC_COUNT() → Int64"""
function cdc_count(m::CDCModel)::Int64
    require_nucleus(m.features, "CDC")
    result = LibPQ.execute(m.conn, "SELECT CDC_COUNT()")
    return _int(result)
end

"""CDC_TABLE_READ(table, offset) → String (JSON change events for table)"""
function cdc_table_read(m::CDCModel, table::String, offset::Int64)::String
    require_nucleus(m.features, "CDC")
    result = LibPQ.execute(m.conn, "SELECT CDC_TABLE_READ(\$1, \$2)", [table, offset])
    val = first(result)[1]
    return ismissing(val) ? "[]" : val
end
