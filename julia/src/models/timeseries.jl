"""TimeSeries model — time-stamped float series via TS_* SQL functions."""

struct TimeSeriesModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

struct TimeSeriesPoint
    timestamp_ms::Int64
    value::Float64
end

# ── Insertion ─────────────────────────────────────────────────────────────────

"""
    insert!(m, series, timestamp_ms, value)

TS_INSERT for a single (timestamp_ms, value) point.
"""
function insert!(m::TimeSeriesModel, series::String, timestamp_ms::Int64, value::Real)
    require_nucleus(m.features, "TimeSeries")
    LibPQ.execute(m.conn, "SELECT TS_INSERT(\$1, \$2, \$3)",
                  [series, timestamp_ms, Float64(value)])
    return nothing
end

"""
    insert!(m, series, points::Vector{TimeSeriesPoint})

Batch-insert multiple points wrapped in a transaction.
"""
function insert!(m::TimeSeriesModel, series::String, points::Vector{TimeSeriesPoint})
    require_nucleus(m.features, "TimeSeries")
    LibPQ.execute(m.conn, "BEGIN")
    try
        for pt in points
            LibPQ.execute(m.conn, "SELECT TS_INSERT(\$1, \$2, \$3)",
                          [series, pt.timestamp_ms, pt.value])
        end
        LibPQ.execute(m.conn, "COMMIT")
    catch e
        LibPQ.execute(m.conn, "ROLLBACK")
        rethrow(e)
    end
    return nothing
end

"""
    insert!(m, series, timestamps_ms, values)

Convenience: batch-insert from two parallel arrays.
"""
function insert!(m::TimeSeriesModel, series::String,
                 timestamps_ms::AbstractVector{Int64},
                 values::AbstractVector{<:Real})
    points = [TimeSeriesPoint(t, Float64(v)) for (t, v) in zip(timestamps_ms, values)]
    insert!(m, series, points)
end

# ── Queries ───────────────────────────────────────────────────────────────────

"""TS_LAST(series) → Float64 or nothing"""
function last_value(m::TimeSeriesModel, series::String)::Union{Float64, Nothing}
    require_nucleus(m.features, "TimeSeries")
    result = LibPQ.execute(m.conn, "SELECT TS_LAST(\$1)", [series])
    return _float(result)
end

"""TS_COUNT(series) → Int64"""
function ts_count(m::TimeSeriesModel, series::String)::Int64
    require_nucleus(m.features, "TimeSeries")
    result = LibPQ.execute(m.conn, "SELECT TS_COUNT(\$1)", [series])
    return _int(result)
end

"""TS_RANGE_COUNT(series, start_ms, end_ms) → Int64"""
function range_count(m::TimeSeriesModel, series::String,
                     start_ms::Int64, end_ms::Int64)::Int64
    require_nucleus(m.features, "TimeSeries")
    result = LibPQ.execute(m.conn, "SELECT TS_RANGE_COUNT(\$1, \$2, \$3)",
                           [series, start_ms, end_ms])
    return _int(result)
end

"""TS_RANGE_AVG(series, start_ms, end_ms) → Float64 or nothing"""
function range_avg(m::TimeSeriesModel, series::String,
                   start_ms::Int64, end_ms::Int64)::Union{Float64, Nothing}
    require_nucleus(m.features, "TimeSeries")
    result = LibPQ.execute(m.conn, "SELECT TS_RANGE_AVG(\$1, \$2, \$3)",
                           [series, start_ms, end_ms])
    return _float(result)
end

"""TS_RETENTION(series, days) → Bool"""
function set_retention!(m::TimeSeriesModel, series::String, days::Int)::Bool
    require_nucleus(m.features, "TimeSeries")
    result = LibPQ.execute(m.conn, "SELECT TS_RETENTION(\$1, \$2)", [series, days])
    return _bool(result)
end

"""TS_MATCH(series, pattern) → String"""
function match_pattern(m::TimeSeriesModel, series::String, pattern::String)::String
    require_nucleus(m.features, "TimeSeries")
    result = LibPQ.execute(m.conn, "SELECT TS_MATCH(\$1, \$2)", [series, pattern])
    val = first(result)[1]
    return ismissing(val) ? "" : val
end

"""
    time_bucket(m, interval, timestamp_ms) → Int64

TIME_BUCKET(interval, timestamp). Intervals: 'second','minute','hour','day','week','month'.
"""
function time_bucket(m::TimeSeriesModel, interval::String, timestamp_ms::Int64)::Int64
    require_nucleus(m.features, "TimeSeries")
    result = LibPQ.execute(m.conn, "SELECT TIME_BUCKET(\$1, \$2)", [interval, timestamp_ms])
    return _int(result)
end
