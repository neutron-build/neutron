"""
DataFrames.jl extension — loaded as NeutronJuliaDataFramesExt.
Bidirectional conversion between DataFrames and Nucleus SQL/Columnar results.

Usage:
```julia
using DataFrames, NeutronJulia
s = sql(client)
df = DataFrame(query(s, "SELECT id, name, score FROM users"))
```
"""

module NeutronJuliaDataFramesExt

using NeutronJulia
using DataFrames

"""
    DataFrame(result) → DataFrame

Convert a Tables.jl columntable (NamedTuple) returned by query() to a DataFrame.
"""
DataFrames.DataFrame(nt::NamedTuple) = DataFrames.DataFrame(nt)

"""
    query_df(m::SQLModel, sql, params...) → DataFrame

Execute a SQL query and return a DataFrame directly.
"""
function NeutronJulia.query_df(m::NeutronJulia.SQLModel, sql_str::String, params...)
    ct = NeutronJulia.query(m, sql_str, params...)
    return DataFrames.DataFrame(ct)
end

"""
    insert_dataframe!(m::ColumnarModel, table, df)

Bulk-insert a DataFrame into a Nucleus Columnar table row-by-row.
"""
function NeutronJulia.insert_dataframe!(m::NeutronJulia.ColumnarModel,
                                        table::String,
                                        df::DataFrames.AbstractDataFrame)
    for row in DataFrames.eachrow(df)
        NeutronJulia.insert!(m, table, Dict(pairs(row)...))
    end
    return nothing
end

end # module NeutronJuliaDataFramesExt
