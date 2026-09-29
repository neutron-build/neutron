import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";

export function head() {
  return {
    title: "Neutron Modelica — Physical Simulation",
    description: "Physical simulation in the Neutron ecosystem: Python modeling and orchestration, numerical solvers, optional Julia and FMI integrations, and Nucleus result storage. Integration work is ongoing.",
  };
}

export default function ModelicaPage() {
  return (
    <ProductPage
      title="Neutron Modelica"
      description="Bring physical systems into your application. Model and simulate with Python, connect numerical tools through Julia and FMI, and work with simulation results through Nucleus."
      category="tool"
      status="in-progress"
      actions={[
        { label: "Simulation documentation", href: "/docs/modeling/modelica" },
        { label: "How the components fit", href: "/docs/modeling/architecture" },
      ]}
    >
      <section>
        <h2>Simulation belongs in the application.</h2>
        <p>A thermal model, electrical circuit, or mechanical system can inform a dashboard, an experiment, or a control service. Neutron Modelica is the simulation component that connects that work to the wider Neutron ecosystem.</p>
        <p>Today’s implementation is Python tooling in <code>neutron_sim</code>, with a modeling API, SciPy solvers, domain components, and optional integrations. Modelica-language models require an external compiler and an appropriate interchange path; Neutron does not ship a general Modelica compiler.</p>
      </section>
      <FeatureGrid columns={3} accentRgb="239, 68, 68">
        <div class="feature-card">
          <div class="feature-card__title">Model and solve</div>
          <div class="feature-card__desc">Variables, parameters, equations, components, and connections, with electrical, mechanical, thermal, and fluid libraries. Numerical simulation runs through SciPy.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Connect specialist tools</div>
          <div class="feature-card__desc">Optional FMI tooling and a juliacall bridge to Julia’s ModelingToolkit and DifferentialEquations packages. Availability depends on the selected integrations and their runtimes.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Work with results</div>
          <div class="feature-card__desc">Nucleus storage helpers, plots, MCP tool wrappers, and optional scikit-learn surrogate models. Compose these capabilities into your own application workflow.</div>
        </div>
      </FeatureGrid>
      <section>
        <h2>Clear boundaries make simulations portable.</h2>
        <p>The existing FMU export writes model-description metadata and a serialized Python model for Neutron round trips. It does not produce the native FMI implementation needed for a generally portable executable FMU.</p>
        <p>The source tree also contains <code>neutron_modelica</code> runtime and circuit helpers, but the current wheel configuration packages only <code>neutron_sim</code>. Packaging and external-tool interoperability need further work before the broader integration can be treated as complete.</p>
      </section>
      <section>
        <h2>Python orchestrates. Julia computes. Nucleus stores.</h2>
        <p>The intended workflow keeps each component focused: Python describes and coordinates an experiment; numerical backends solve it; Nucleus stores results; TypeScript presents them. Services in other Neutron languages can consume those results through explicit data contracts.</p>
        <p>A common project workflow, versioned simulation artifacts, and validated interoperability across these boundaries are the next steps. <a href="/docs/modeling/architecture">Read the proposed integration architecture</a>.</p>
      </section>
      <section>
        <h2>Simulation and verification work together.</h2>
        <p>Simulation estimates a model’s behavior numerically under chosen parameters and solver tolerances. <a href="/quint">Quint</a> explores protocol state transitions; <a href="/lean">Lean</a> checks deductive proofs. Combining their results requires explicit assumptions about the physical model and its connection to the software.</p>
      </section>
    </ProductPage>
  );
}
