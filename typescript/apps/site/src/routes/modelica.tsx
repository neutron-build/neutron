import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import ComparisonTable from "../components/ComparisonTable";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Modelica Physics Simulation - Neutron",
    description: "Equation-based physical system simulation with FMI 2.0/3.0 model exchange, Kirchhoff's-law solver via MNA, and a Julia bridge to DifferentialEquations.jl.",
  };
}

export default function ModelicaPage() {
  return (
    <ProductPage
      title="Neutron Modelica"
      description="Equation-based simulation of physical systems. FMI 2.0 and 3.0 model exchange, modified nodal analysis for electrical networks, and a Julia bridge for heavy numerics."
      category="language"
      status="available"
      accent="var(--accent-modelica)"
      heroAccentRgb="239, 68, 68"
      heroTagline="Describe physics. The solver does the rest."
      stats={[
        { value: 'FMI 3.0', label: 'Model Exchange' },
        { value: 'MNA', label: "Kirchhoff's Law Solver" },
        { value: 'Julia', label: 'Numerics Bridge' },
        { value: 'Open', label: 'Standard' },
      ]}
    >
      <section>
        <h2>Physics as code, not as a C++ project.</h2>
        <p>Modelica describes systems as equations. You write what a resistor, a motor, a heat exchanger <em>is</em> &mdash; and the compiler symbolically transforms your hierarchy of components into a solvable system. Neutron Modelica gives you a working pipeline: FMI import/export, a modified-nodal-analysis solver for electrical circuits, and a Julia bridge to DifferentialEquations.jl for when the numerics get heavy.</p>
      </section>

      <CodeBlock filename="models/rlc.mo" annotation="Series RLC circuit. Modelica compiles this into ODEs automatically.">
        <pre><code>{`model SeriesRLC
  import Modelica.Electrical.Analog.Basic.{Resistor, Inductor, Capacitor, Ground};
  import Modelica.Electrical.Analog.Sources.StepVoltage;

  Resistor  R(R=100);
  Inductor  L(L=0.01);
  Capacitor C(C=1e-6);
  StepVoltage V(V=5);
  Ground gnd;
equation
  connect(V.p, R.p);
  connect(R.n, L.p);
  connect(L.n, C.p);
  connect(C.n, V.n);
  connect(V.n, gnd.p);
end SeriesRLC;`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="239, 68, 68">
        <div class="feature-card">
          <div class="feature-card__title">FMI 2.0 &amp; 3.0</div>
          <div class="feature-card__desc">Functional Mockup Interface for model exchange and co-simulation. Import FMUs from Simulink, Dymola, OpenModelica; export FMUs for other tools.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Equation-based</div>
          <div class="feature-card__desc">Declare what the system <em>is</em>, not how to integrate it. The compiler flattens the hierarchy, simplifies symbolically, and hands a sparse system to the solver.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Multi-domain physics</div>
          <div class="feature-card__desc">Electrical, mechanical, thermal, hydraulic, chemical &mdash; connected in one model. Cross-domain conservation laws handled automatically.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">MNA solver</div>
          <div class="feature-card__desc">Modified nodal analysis for electrical circuits. Kirchhoff's laws become linear algebra; SPICE-level behavior with a real programming language on top.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Julia bridge</div>
          <div class="feature-card__desc">For stiff systems or GPU-accelerated integration, dispatch into Julia's DifferentialEquations.jl via Neutron Julia. Same process, shared memory.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Nucleus sink</div>
          <div class="feature-card__desc">Pipe simulation trajectories straight into Nucleus time-series. Persisted, queryable, ready to visualize with Neutron TypeScript or Studio.</div>
        </div>
      </FeatureGrid>

      <ComparisonTable
        headers={['Aspect', 'Hand-written ODE code', 'Modelica']}
        rows={[
          ['Workflow', 'Derive equations, pick solver, code it', 'Declare components, connect them, simulate'],
          ['Physics domains', 'One at a time', 'Electrical + mechanical + thermal in one'],
          ['Reuse', 'Copy and adapt', 'Hierarchical component libraries'],
          ['Symbolic simplification', 'Manual', 'Automatic (index reduction, causalization)'],
          ['Tool interoperability', 'None', 'FMI 2.0 / 3.0 across 50+ tools'],
        ]}
        highlightColumn={2}
        accentRgb="239, 68, 68"
      />

      <section>
        <h3>What it's for</h3>
        <p>Control-system design where the plant and the controller are modeled together. Digital twins of real assets pulling live sensor data from Nucleus. Battery pack, motor-drive, HVAC, and power-electronics simulations. Anywhere you'd reach for Simulink but would rather have an open standard and a real database.</p>

        <h3>Why Modelica?</h3>
        <p>Because thirty years of industrial engineering has already answered how to model physics correctly &mdash; aerospace, automotive, and energy companies built it. Because FMI means your model talks to every other simulation tool in the world. Because equation-based modeling lets you change physics by editing an equation, not rewriting a solver.</p>

        <h3>Part of a bigger system</h3>
        <p>Simulate in Neutron Modelica. Solve stiff systems in Neutron Julia. Persist trajectories in Nucleus time-series. Expose live dashboards from Neutron TypeScript. Train surrogate models in Neutron Mojo. The simulation is another process in the stack, not a silo.</p>
      </section>
    </ProductPage>
  );
}
