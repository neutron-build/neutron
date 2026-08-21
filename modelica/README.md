# Neutron Modelica

Equation-based physics simulation. Python orchestrates; Julia computes.

Python handles what it is good at — FMI interoperability, SciPy ODE prototyping,
Nucleus TimeSeries storage, AI-agent integration, plotting. Heavy symbolic work
(DAE index reduction, large multi-domain systems) delegates to Julia's
ModelingToolkit.jl and DifferentialEquations.jl through an optional `juliacall`
bridge. This is a simulation component, not a framework SDK: it ships no HTTP
server and is not part of the SDK conformance matrix.

## Packages

Wheel name is `neutron-sim` (see `pyproject.toml`); there are two import packages.

| Package | What |
|---|---|
| `neutron_sim` | Core: acausal modeling API, solvers, domain libraries, FMI, Nucleus storage, AI tools, visualization |
| `neutron_modelica` | FMI Model Exchange runtime, Kirchhoff circuit solvers, Julia DE bridge |

### neutron_sim

| Module | What it does |
|---|---|
| `core/` | The acausal modeling API: `Variable`, `Parameter`, `Equation`, `der()`, `Connector`, `Component`, `System`, `connect` |
| `solvers/` | `simulate()` over SciPy `solve_ivp`, with stiff/non-stiff detection for automatic solver selection |
| `domains/` | Component libraries: electrical, fluid, mechanical translational, thermal |
| `fmi/` | FMI 2.0 and 3.0 `modelDescription.xml` export, FMU import, co-simulation orchestration |
| `nucleus/` | Store, load and compare simulation runs in Nucleus TimeSeries |
| `ai/` | Register simulations as MCP tools for Neutron Python AI agents; train surrogate models from run data |
| `julia/` | Optional `juliacall` bridge to ModelingToolkit.jl / DifferentialEquations.jl |
| `viz/` | matplotlib time-series plots and phase portraits |

### neutron_modelica

| Export | What it is |
|---|---|
| `FMU` | FMI 2.0/3.0 Model Exchange runtime wrapper, including the FMI 3.0 binary, clock and array variable types |
| `Circuit`, `Resistor`, `Capacitor`, `Inductor`, sources | Kirchhoff (KCL/KVL) solver with transient results |
| `JuliaDEBridge` | DifferentialEquations.jl bridge for heavy ODE/DAE work |

## Install and test

```bash
pip install -e ".[dev]"                # core + test deps
pip install -e ".[dev,fmi,julia,ai]"   # everything
pytest
```

Python 3.11+. Optional integrations are extras, not dependencies: `fmi`
(fmpy), `julia` (juliacall), `ai` (scikit-learn). Tests degrade honestly at
each boundary — FMI tests run without fmpy, a C compiler or a database;
Nucleus TimeSeries integration tests skip unless `NUCLEUS_TEST_URL` is set;
Julia bridge tests skip unless juliacall is installed; surrogate tests skip
without scikit-learn.

## CI

[`.github/workflows/modelica.yml`](../.github/workflows/modelica.yml), path-filtered
to `modelica/**`: pytest on Python 3.11 and 3.12 (including a step that fails
loudly if an optional extra silently failed to install), plus ruff lint over
`neutron_sim`, `neutron_modelica` and `tests`.

## License

MIT. See the repository root [LICENSE](../LICENSE).
