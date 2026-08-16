"""Datalog model — logic programming via DATALOG_* SQL functions."""

struct DatalogModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

"""DATALOG_ASSERT(fact) → String status, e.g. "ASSERT parent/2"

Declared `::Bool` and called `_bool`, but the engine answers with a status
string, so every call raised `MethodError: Cannot convert an object of type
String`. The function had never worked. Rust and Go already returned the string.
"""
function assert_fact!(m::DatalogModel, fact::String)::String
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_ASSERT(\$1)", [fact])
    v = first(result)[1]
    return ismissing(v) ? "" : String(v)
end

"""DATALOG_RETRACT(fact) → String status (same shape as assert_fact!)."""
function retract!(m::DatalogModel, fact::String)::String
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_RETRACT(\$1)", [fact])
    v = first(result)[1]
    return ismissing(v) ? "" : String(v)
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
# Takes the predicate to clear.
#
# Sent ZERO arguments where DATALOG_CLEAR requires one, so every call failed
# with "requires 1 argument(s), got 0" — the function had never worked. This is
# the same defect TypeScript had (recorded 2026-08-13), in a second SDK, and
# both survived because nothing executed the call against a live engine.
function clear!(m::DatalogModel, predicate::String)::String
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_CLEAR(\$1)", [predicate])
    v = first(result)[1]
    return ismissing(v) ? "" : String(v)
end

"""DATALOG_IMPORT_GRAPH(predicate) → String status, e.g. "IMPORTED 30 edges into edge"

Sent ZERO arguments where the engine requires one, and declared `::Int64` where
it answers with a status string — two ways of never having worked, in the same
four lines as `clear!`.
"""
function import_graph!(m::DatalogModel, predicate::String)::String
    require_nucleus(m.features, "Datalog")
    result = LibPQ.execute(m.conn, "SELECT DATALOG_IMPORT_GRAPH(\$1)", [predicate])
    v = first(result)[1]
    return ismissing(v) ? "" : String(v)
end
