import LanguageOverview from "../components/LanguageOverview";

export function head() {
  return { title: "Julia - Neutron", description: "Neutron's Nucleus client for Julia." };
}

export default function JuliaPage() {
  return <LanguageOverview
    name="Julia"
    description="A typed Nucleus client designed to compose with Julia's data and scientific-computing ecosystem."
    accent="var(--accent-julia)"
    accentRgb="155, 92, 184"
    docsHref="/docs/julia/overview"
    facts={[
      { label: "Runtime", value: "Julia 1.9+" },
      { label: "Transport", value: "PostgreSQL wire protocol through LibPQ.jl" },
      { label: "Results", value: "Tables.jl-compatible column tables" },
      { label: "Extensions", value: "Optional integrations for DataFrames and scientific-computing packages" },
    ]}
  />;
}
