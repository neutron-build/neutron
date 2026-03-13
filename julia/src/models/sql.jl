"""Standard SQL model — arbitrary queries over plain PostgreSQL/Nucleus."""
struct SQLModel
    conn::LibPQ.Connection
end

"""
    query(m::SQLModel, sql, params...) -> NamedTuple of column vectors

Execute a SELECT and return results as a Tables.jl columntable (NamedTuple of
column vectors). With DataFrames.jl loaded, wrap in DataFrame(...) for a full
DataFrame.
"""
function query(m::SQLModel, sql_str::String, params...)
    result = LibPQ.execute(m.conn, sql_str, collect(params))
    return Tables.columntable(result)
end

"""
    query_one(m::SQLModel, sql, params...) -> NamedTuple row

Execute a SELECT and return the first row as a NamedTuple. Throws NucleusError
(404) if no rows are returned.
"""
function query_one(m::SQLModel, sql_str::String, params...)
    result = LibPQ.execute(m.conn, sql_str, collect(params))
    rows = Tables.rowtable(result)
    isempty(rows) && throw(NucleusError(
        "https://neutron.dev/errors/not-found",
        "Not Found",
        404,
        "Query returned no rows"))
    return first(rows)
end

"""
    execute!(m::SQLModel, sql, params...) -> Int64

Execute an INSERT/UPDATE/DELETE and return the number of affected rows.
"""
function execute!(m::SQLModel, sql_str::String, params...)::Int64
    result = LibPQ.execute(m.conn, sql_str, collect(params))
    return Int64(LibPQ.num_affected_rows(result))
end
