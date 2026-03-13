"""CDC model — Change Data Capture via CDC_* SQL functions."""

struct CDCModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

"""CDC_READ(offset) → String (JSON change events)"""
function cdc_read(m::CDCModel, offset::Int64)::String
    require_nucleus(m.features, "CDC")
    result = LibPQ.execute(m.conn, "SELECT CDC_READ(\$1)", [offset])
    val = first(result)[1]
    return ismissing(val) ? "[]" : val
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
