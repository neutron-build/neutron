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
# Takes the bucket size in MILLISECONDS.
#
# This took an interval NAME (`interval::String`) while the engine's TIME_BUCKET
# has always taken `(bucket_millis, ts)` as INT8, so every call bound text where
# an integer was required and the function had never once worked. This is the
# THIRD SDK with the identical defect — Python (L1) and Rust had it too, each
# found only when something finally executed the call against a live engine.
function time_bucket(m::TimeSeriesModel, bucket_ms::Int64, timestamp_ms::Int64)::Int64
    require_nucleus(m.features, "TimeSeries")
    result = LibPQ.execute(m.conn, "SELECT TIME_BUCKET(\$1, \$2)", [bucket_ms, timestamp_ms])
    return _int(result)
end

"""TS_RANGE(series, start_ms, end_ms) → Vector of (timestamp_ms, value).

The raw points, not an aggregate. There was no function for this and no SQL
surface either, so Python synthesised it from sixty bucketed TS_RANGE_AVG calls,
Go refused and TypeScript threw — three answers to one question. TS_RANGE was
added to the engine 2026-08-15 and every SDK now uses it.
"""
function ts_range(m::TimeSeriesModel, series::String, start_ms::Int64, end_ms::Int64)
    require_nucleus(m.features, "TimeSeries")
    end_ms <= start_ms && return Tuple{Int64, Float64}[]
    result = LibPQ.execute(m.conn, "SELECT TS_RANGE(\$1, \$2, \$3)", [series, start_ms, end_ms])
    raw = first(result)[1]
    (ismissing(raw) || raw === nothing || isempty(raw)) && return Tuple{Int64, Float64}[]
    return [(Int64(p["t"]), Float64(p["v"])) for p in JSON3.read(raw)]
end

"""Aggregate a series into fixed windows across a range.

One `(bucket_start_ms, value)` per `window_ms`-sized bucket, skipping empty
buckets. `fn` is `:avg` or `:count`: the engine ships TS_RANGE_AVG and
TS_RANGE_COUNT and nothing else.

Alignment is to `window_ms`, not to a calendar unit — aligning a five-minute
window to an hour boundary produces buckets that do not line up with the window
the caller asked for, which is a wrong answer rather than an error.
"""
function aggregate(m::TimeSeriesModel, series::String, start_ms::Int64, end_ms::Int64,
                   window_ms::Int64, fn::Symbol=:avg)
    require_nucleus(m.features, "TimeSeries")
    (window_ms <= 0 || end_ms <= start_ms) && return Tuple{Int64, Float64}[]
    sql_fn = fn === :avg ? "TS_RANGE_AVG" :
             fn === :count ? "TS_RANGE_COUNT" :
             throw(ArgumentError("unsupported aggregate $fn; use :avg or :count"))

    bucket = time_bucket(m, window_ms, start_ms)
    out = Tuple{Int64, Float64}[]
    while bucket < end_ms
        stop = min(bucket + window_ms, end_ms)
        r = LibPQ.execute(m.conn, "SELECT $sql_fn(\$1, \$2, \$3)", [series, bucket, stop])
        v = first(r)[1]
        (ismissing(v) || v === nothing) || push!(out, (Int64(bucket), Float64(v)))
        bucket += window_ms
    end
    return out
end

