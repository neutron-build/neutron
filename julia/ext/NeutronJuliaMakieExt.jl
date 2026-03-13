"""
Makie.jl extension — loaded as NeutronJuliaMakieExt.
Visualization directly from Nucleus data.

Usage:
```julia
using CairoMakie, NeutronJulia
ts = timeseries(client)
fig = Figure()
ax = Axis(fig[1,1])
plot_timeseries!(ax, ts, "sensor:temp"; start_ms=..., end_ms=...)
```
"""

module NeutronJuliaMakieExt

using NeutronJulia
using Makie

"""
    plot_timeseries!(ax, m::TimeSeriesModel, series; start_ms, end_ms, kwargs...)

Query Nucleus TimeSeries data for `series` and plot it on a Makie Axis.
"""
function NeutronJulia.plot_timeseries!(ax,
                                       m::NeutronJulia.TimeSeriesModel,
                                       series::String;
                                       start_ms::Union{Int64,Nothing}=nothing,
                                       end_ms::Union{Int64,Nothing}=nothing,
                                       kwargs...)
    NeutronJulia.require_nucleus(m.features, "TimeSeries")
    if start_ms !== nothing && end_ms !== nothing
        raw = LibPQ.execute(m.conn,
            "SELECT timestamp_ms, value FROM nucleus_timeseries
             WHERE series = \$1 AND timestamp_ms BETWEEN \$2 AND \$3
             ORDER BY timestamp_ms",
            [series, start_ms, end_ms])
    else
        raw = LibPQ.execute(m.conn,
            "SELECT timestamp_ms, value FROM nucleus_timeseries
             WHERE series = \$1 ORDER BY timestamp_ms",
            [series])
    end
    rows = collect(raw)
    isempty(rows) && return nothing

    t = [r[1] / 1000.0 for r in rows]
    v = [Float64(r[2]) for r in rows]
    Makie.lines!(ax, t, v; label=series, kwargs...)
    return nothing
end

end # module NeutronJuliaMakieExt
