import LanguageOverview from "../components/LanguageOverview";

export function head() {
  return { title: "Mojo - Neutron", description: "Preview tensor and inference libraries for Mojo." };
}

export default function MojoPage() {
  return <LanguageOverview
    name="Mojo"
    description="Preview tensor, quantization, and transformer-inference libraries for Mojo 1.0."
    status="in-progress"
    accent="var(--accent-mojo)"
    accentRgb="168, 85, 247"
    docsHref="/docs/mojo/overview"
    facts={[
      { label: "Status", value: "Preview; APIs still expected to move" },
      { label: "Compute", value: "CPU-first SIMD tensor and inference paths" },
      { label: "Models", value: "GGUF and SafeTensors loading with several quantization formats" },
      { label: "Toolchain", value: "Built on Mojo 1.0 (max >= 26.5)" },
    ]}
    note="A dedicated GPU backend and hardened HTTP serving runtime are not currently shipped."
  />;
}
