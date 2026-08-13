import LanguageOverview from "../components/LanguageOverview";

export function head() {
  return { title: "Elixir - Neutron", description: "Neutron's Elixir framework built on Plug, Bandit, and OTP." };
}

export default function ElixirPage() {
  return <LanguageOverview
    name="Elixir"
    description="A supervised HTTP framework built on Plug and Bandit, with Postgrex access to Nucleus."
    accent="var(--accent-elixir)"
    accentRgb="110, 74, 126"
    docsHref="/docs/elixir/index"
    quickstartHref="/docs/elixir/getting-started"
    facts={[
      { label: "Runtime", value: "Elixir 1.15+ and OTP" },
      { label: "HTTP", value: "Plug routing on Bandit" },
      { label: "Data", value: "Postgrex connection pooling and optional Nucleus model modules" },
      { label: "Package", value: "neutron_ex on Hex" },
    ]}
  />;
}
