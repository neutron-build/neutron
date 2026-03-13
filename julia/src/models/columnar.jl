"""Columnar model — analytical aggregations via COLUMNAR_* SQL functions."""

struct ColumnarModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

"""COLUMNAR_INSERT(table, values_json) → nothing"""
function insert!(m::ColumnarModel, table::String, values::Dict)
    require_nucleus(m.features, "Columnar")
    json_str = JSON3.write(values)
    LibPQ.execute(m.conn, "SELECT COLUMNAR_INSERT(\$1, \$2)", [table, json_str])
    return nothing
end

"""COLUMNAR_COUNT(table) → Int64"""
function columnar_count(m::ColumnarModel, table::String)::Int64
    require_nucleus(m.features, "Columnar")
    result = LibPQ.execute(m.conn, "SELECT COLUMNAR_COUNT(\$1)", [table])
    return _int(result)
end

"""COLUMNAR_SUM(table, column) → Float64"""
function columnar_sum(m::ColumnarModel, table::String, column::String)::Float64
    require_nucleus(m.features, "Columnar")
    result = LibPQ.execute(m.conn, "SELECT COLUMNAR_SUM(\$1, \$2)", [table, column])
    v = _float(result)
    return v === nothing ? 0.0 : v
end

"""COLUMNAR_AVG(table, column) → Float64"""
function columnar_avg(m::ColumnarModel, table::String, column::String)::Float64
    require_nucleus(m.features, "Columnar")
    result = LibPQ.execute(m.conn, "SELECT COLUMNAR_AVG(\$1, \$2)", [table, column])
    v = _float(result)
    return v === nothing ? 0.0 : v
end

"""COLUMNAR_MIN(table, column) → Any"""
function columnar_min(m::ColumnarModel, table::String, column::String)
    require_nucleus(m.features, "Columnar")
    result = LibPQ.execute(m.conn, "SELECT COLUMNAR_MIN(\$1, \$2)", [table, column])
    val = first(result)[1]
    return ismissing(val) ? nothing : val
end

"""COLUMNAR_MAX(table, column) → Any"""
function columnar_max(m::ColumnarModel, table::String, column::String)
    require_nucleus(m.features, "Columnar")
    result = LibPQ.execute(m.conn, "SELECT COLUMNAR_MAX(\$1, \$2)", [table, column])
    val = first(result)[1]
    return ismissing(val) ? nothing : val
end
