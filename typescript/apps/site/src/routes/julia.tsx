import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Julia - Neutron",
    description: "Scientific computing library for Julia with Nucleus integration. DifferentialEquations.jl, ModelingToolkit, CUDA, FMI 3.0 model exchange. Research-grade numerics with production plumbing.",
  };
}

export default function JuliaPage() {
  return (
    <ProductPage
      title="Neutron Julia"
      description="Scientific computing with DifferentialEquations.jl, ModelingToolkit, CUDA kernels, and FMI interop &mdash; wired to Nucleus so your simulations persist like real data."
      category="language"
      status="available"
      accent="var(--accent-julia)"
      heroAccentRgb="155, 92, 184"
      heroTagline="Where the equations live."
      stats={[
        { value: 'DiffEq', label: 'Solver Bindings' },
        { value: 'CUDA', label: 'GPU Kernels' },
        { value: 'FMI 3.0', label: 'Model Exchange' },
        { value: '14', label: 'Data Models' },
      ]}
    >
      <section>
        <h2>Science, with the plumbing already done.</h2>
        <p>Julia is the best language on the planet for numerical computing &mdash; C speed, LLVM codegen, multiple dispatch that makes solvers composable in a way static languages can't touch. What it doesn't have is a story for persistence, deployment, and integration with the rest of your stack. Neutron Julia is that story: a typed Nucleus client, first-class DifferentialEquations.jl bindings, ModelingToolkit helpers, and CUDA kernels that don't need a separate toolchain.</p>
      </section>

      <CodeBlock filename="src/lorenz.jl" annotation="Solve Lorenz ODEs, store the trajectory in Nucleus time-series.">
        <pre><code>{`using Neutron
using DifferentialEquations

function lorenz!(du, u, p, t)
    σ, ρ, β = p
    du[1] = σ*(u[2]-u[1])
    du[2] = u[1]*(ρ-u[3]) - u[2]
    du[3] = u[1]*u[2] - β*u[3]
end

u0 = [1.0, 0.0, 0.0]
prob = ODEProblem(lorenz!, u0, (0.0, 100.0), (10.0, 28.0, 8/3))
sol  = solve(prob, Tsit5(), saveat=0.01)

db = Neutron.connect(ENV["DATABASE_URL"])
series = Neutron.timeseries(db, "lorenz_runs")
for (t, u) in zip(sol.t, sol.u)
    push!(series, (t=t, x=u[1], y=u[2], z=u[3]))
end`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="155, 92, 184">
        <div class="feature-card">
          <div class="feature-card__title">Nucleus native</div>
          <div class="feature-card__desc">Typed client for all 14 data models over pgwire. Persist trajectories as time-series, vectors as vectors, graphs as graphs &mdash; no ORM adapter in between.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">DifferentialEquations.jl</div>
          <div class="feature-card__desc">First-class bindings to the best ODE/SDE/DAE/DDE/PDE solver ecosystem in any language. Adaptive timestepping, event handling, auto-differentiation.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">ModelingToolkit</div>
          <div class="feature-card__desc">Symbolic-numeric modeling with automatic simplification, index reduction, and code generation. Write physics, get fast solvers for free.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">CUDA kernels</div>
          <div class="feature-card__desc">CUDA.jl integration for GPU numerics. Write kernels in Julia, compile to PTX, no C++ required. Same code runs on CPU with Threads.jl.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">FMI 2.0 / 3.0</div>
          <div class="feature-card__desc">Import FMUs from Modelica, Simulink, or any FMI-compliant tool. Co-simulation and model exchange. Digital twins connect to real data.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Package extensions</div>
          <div class="feature-card__desc">Flux, Graphs, DataFrames, Makie, and CUDA are optional extensions. Import what you need; Julia's package extensions system loads them lazily.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>The scientific workflow, end to end.</h2>
        <p>The cycle is always the same: define the model, solve, persist, analyze, visualize. Neutron Julia keeps each step in one process with one database.</p>
      </section>

      <BenchmarkBars
        title="The pipeline"
        bars={[
          { label: 'Define', value: 'ModelingToolkit symbolic models', width: 100, color: '#9B5CB8' },
          { label: 'Solve', value: 'DifferentialEquations.jl, CUDA-accelerated', width: 92, color: '#B07ACC' },
          { label: 'Persist', value: 'Nucleus time-series + blob + SQL', width: 85, color: '#C49BDF' },
          { label: 'Analyze', value: 'DataFrames + statistics + graphs', width: 75, color: '#D9BDED' },
          { label: 'Visualize', value: 'Makie plots served via Neutron TypeScript', width: 68, color: '#E8D4F5' },
        ]}
      />

      <section>
        <h3>What it's for</h3>
        <p>Scientific simulations with persistent state. Climate models, financial models, population dynamics, digital twins backed by a real time-series database. ML-adjacent numerics that need CUDA but don't fit Python's asyncio. Research pipelines where Julia's speed meets Neutron's infrastructure.</p>

        <h3>Why Julia?</h3>
        <p>Because it compiles through LLVM to native code and hits C speed on numerical workloads. Because multiple dispatch makes solvers composable in a way monkey-patching and inheritance can't match. Because the same function runs on CPU, GPU, or distributed clusters with no rewrite. For the problems Julia solves, there's no substitute.</p>

        <h3>Part of a bigger system</h3>
        <p>Simulate in Neutron Julia. Serve interactive dashboards with Neutron TypeScript. Persist everything in Nucleus. Feed results into Neutron Mojo for ML inference. One source of truth across runtimes.</p>
      </section>
    </ProductPage>
  );
}
