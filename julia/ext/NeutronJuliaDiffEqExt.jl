"""
DifferentialEquations.jl extension — loaded as NeutronJuliaDiffEqExt.
Store ODE/SDE/DAE solutions in Nucleus TimeSeries.

Usage (with DifferentialEquations.jl loaded):
```julia
using DifferentialEquations, NeutronJulia
ts = timeseries(client)
sol = solve(prob, Tsit5(), saveat=0.01)
store!(ts, sol, "lorenz:run1"; variable_names=["x","y","z"])
t, u = load_solution(ts, "lorenz:run1", ["x","y","z"])
```
"""

module NeutronJuliaDiffEqExt

using NeutronJulia
using DifferentialEquations

"""
    store!(m::TimeSeriesModel, sol::ODESolution, prefix; variable_names, tags)

Store an ODE/SDE/DAE solution in Nucleus TimeSeries. Each state variable gets
its own series: `prefix:var_name`. Timestamps are converted from solver time
units to milliseconds (t * 1000).
"""
function NeutronJulia.store!(m::NeutronJulia.TimeSeriesModel,
                             sol::DifferentialEquations.ODESolution,
                             prefix::String;
                             variable_names::Union{Vector{String}, Nothing}=nothing,
                             tags::Dict{String,String}=Dict{String,String}())
    n_vars = length(sol.u[1])
    names = variable_names !== nothing ? variable_names :
            ["var$(i)" for i in 1:n_vars]
    length(names) == n_vars || throw(ArgumentError(
        "variable_names length $(length(names)) ≠ solution dimension $n_vars"))

    timestamps_ms = Int64.(round.(sol.t .* 1000))
    for (i, name) in enumerate(names)
        series = "$(prefix):$(name)"
        values = [Float64(sol.u[j][i]) for j in 1:length(sol.t)]
        NeutronJulia.insert!(m, series, timestamps_ms, values)
    end
    return nothing
end

"""
    load_solution(m::TimeSeriesModel, prefix, variable_names) -> (t, u)

Load a previously stored solution back from Nucleus TimeSeries.
Returns `t::Vector{Float64}` (times in seconds) and `u::Matrix{Float64}` (n_vars × n_points).
"""
function NeutronJulia.load_solution(m::NeutronJulia.TimeSeriesModel,
                                    prefix::String,
                                    variable_names::Vector{String})
    NeutronJulia.require_nucleus(m.features, "TimeSeries")
    # Query the first variable to get the time axis
    first_series = "$(prefix):$(variable_names[1])"
    raw = LibPQ.execute(m.conn,
        "SELECT timestamp_ms, value FROM nucleus_timeseries WHERE series = \$1 ORDER BY timestamp_ms",
        [first_series])
    rows = collect(raw)
    isempty(rows) && throw(NeutronJulia.NucleusError(
        "https://neutron.dev/errors/not-found", "Not Found", 404,
        "No data found for series prefix '$(prefix)'"))

    t = Float64[r[1] / 1000.0 for r in rows]
    n = length(t)
    u = Matrix{Float64}(undef, length(variable_names), n)

    for (i, name) in enumerate(variable_names)
        series = "$(prefix):$(name)"
        rr = LibPQ.execute(m.conn,
            "SELECT value FROM nucleus_timeseries WHERE series = \$1 ORDER BY timestamp_ms",
            [series])
        u[i, :] = [Float64(row[1]) for row in rr]
    end
    return t, u
end

end # module NeutronJuliaDiffEqExt
