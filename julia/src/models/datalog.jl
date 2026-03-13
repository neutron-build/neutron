"""Datalog model — logic programming via DATALOG_* SQL functions."""

struct DatalogModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

"""DATALOG_ASSERT(fact) → Bool"""
function assert_fact!(m::DatalogModel, fact::String)::Bool
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_ASSERT(\$1)", [fact])
    return _bool(result)
end

"""DATALOG_RETRACT(fact) → Bool"""
function retract!(m::DatalogModel, fact::String)::Bool
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_RETRACT(\$1)", [fact])
    return _bool(result)
end

"""DATALOG_RULE(head, body) → Bool"""
function rule!(m::DatalogModel, head::String, body::String)::Bool
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_RULE(\$1, \$2)", [head, body])
    return _bool(result)
end

"""DATALOG_QUERY(query) → String (CSV results)"""
function datalog_query(m::DatalogModel, query::String)::String
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_QUERY(\$1)", [query])
    val = first(result)[1]
    return ismissing(val) ? "" : val
end

"""DATALOG_CLEAR() → Bool"""
function clear!(m::DatalogModel)::Bool
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_CLEAR()")
    return _bool(result)
end

"""DATALOG_IMPORT_GRAPH() → Int64 facts imported"""
function import_graph!(m::DatalogModel)::Int64
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_IMPORT_GRAPH()")
    return _int(result)
end
