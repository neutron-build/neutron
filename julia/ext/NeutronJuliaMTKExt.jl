"""
ModelingToolkit.jl extension — loaded as NeutronJuliaMTKExt.
Store MTK simulation results using symbolic variable names.

Usage:
```julia
using ModelingToolkit, DifferentialEquations, NeutronJulia
ts = timeseries(client)
store!(ts, sol, sys, "spring_mass:run1"; tags=Dict("k"=>"10.0"))
```
"""

module NeutronJuliaMTKExt

using NeutronJulia
using ModelingToolkit
using DifferentialEquations

"""
    store!(m, sol, sys, prefix; tags)

Store an MTK ODESolution using symbolic variable names extracted from the
ODESystem. Each variable gets a series `prefix:var_name`.
"""
function NeutronJulia.store!(m::NeutronJulia.TimeSeriesModel,
                             sol::DifferentialEquations.ODESolution,
                             sys::ModelingToolkit.ODESystem,
                             prefix::String;
                             tags::Dict{String,String}=Dict{String,String}())
    states = ModelingToolkit.unknowns(sys)
    names = [string(ModelingToolkit.getname(s)) for s in states]
    timestamps_ms = Int64.(round.(sol.t .* 1000))

    for (i, name) in enumerate(names)
        series = "$(prefix):$(name)"
        values = [Float64(sol[states[i]][j]) for j in 1:length(sol.t)]
        NeutronJulia.insert!(m, series, timestamps_ms, values)
    end
    return nothing
end

end # module NeutronJuliaMTKExt
