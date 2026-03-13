"""
Lorenz Attractor — store ODE solution in Nucleus TimeSeries.

Demonstrates:
- Solving a chaotic ODE with DifferentialEquations.jl
- Storing all state variables in Nucleus TimeSeries via the DiffEq extension
- Loading the solution back and computing basic statistics

Run:
    julia --project=. examples/lorenz.jl postgres://localhost:5432/mydb
"""

using NeutronJulia

const URL = get(ARGS, 1, get(ENV, "NUCLEUS_TEST_URL", ""))

isempty(URL) && error("""
Usage: julia --project=. examples/lorenz.jl <postgres-url>
       or set NUCLEUS_TEST_URL environment variable
""")

# ── Load DifferentialEquations.jl (triggers NeutronJuliaDiffEqExt) ─────────

try
    using DifferentialEquations
catch
    error("DifferentialEquations.jl not installed. Run: import Pkg; Pkg.add(\"DifferentialEquations\")")
end

println("Connecting to Nucleus...")
client = NeutronJulia.connect(URL)
ts = timeseries(client)

# ── Define the Lorenz system ─────────────────────────────────────────────────

function lorenz!(du, u, p, t)
    sigma, rho, beta = p
    du[1] = sigma * (u[2] - u[1])          # dx/dt
    du[2] = u[1] * (rho - u[3]) - u[2]     # dy/dt
    du[3] = u[1] * u[2] - beta * u[3]      # dz/dt
end

u0     = [1.0, 0.0, 0.0]
tspan  = (0.0, 100.0)
p      = (10.0, 28.0, 8/3)   # sigma, rho, beta (classic chaotic regime)

println("Solving Lorenz attractor (t=0..100, saveat=0.01)...")
prob = ODEProblem(lorenz!, u0, tspan, p)
sol  = solve(prob, Tsit5(), saveat=0.01)

println("  $(length(sol.t)) time points, $(length(sol.u[1])) state variables")

# ── Store in Nucleus TimeSeries ───────────────────────────────────────────────

run_id = "lorenz:run:$(round(Int, time()))"
println("Storing solution as series prefix '$(run_id)'...")

store!(ts, sol, run_id;
       variable_names=["x", "y", "z"],
       tags=Dict("solver"=>"Tsit5", "sigma"=>"10.0", "rho"=>"28.0", "beta"=>"2.667"))

println("  Stored $(ts_count(ts, "$(run_id):x")) points for :x")
println("  Last x value: $(last_value(ts, "$(run_id):x"))")

# ── Load back and compute statistics ─────────────────────────────────────────

println("\nLoading solution back from Nucleus...")
t_loaded, u_loaded = load_solution(ts, run_id, ["x", "y", "z"])

println("  Loaded $(length(t_loaded)) time points")
println("  x range: [$(minimum(u_loaded[1,:])), $(maximum(u_loaded[1,:]))]")
println("  y range: [$(minimum(u_loaded[2,:])), $(maximum(u_loaded[2,:]))]")
println("  z range: [$(minimum(u_loaded[3,:])), $(maximum(u_loaded[3,:]))]")

# Compare round-trip fidelity
max_err = maximum(abs.(sol[1,:] .- u_loaded[1,:]))
println("\n  Max round-trip error (x): $(max_err)")
@assert max_err < 1e-6 "Round-trip error too large: $max_err"

println("\nLorenz example complete.")
close(client)
