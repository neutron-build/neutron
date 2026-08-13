import LanguageOverview from "../components/LanguageOverview";

export function head() {
  return { title: "Zig - Neutron", description: "Neutron's allocation-conscious Nucleus client for Zig." };
}

export default function ZigPage() {
  return <LanguageOverview
    name="Zig"
    description="A Nucleus client for embedded and resource-constrained programs, using fixed buffers and compile-time query shapes."
    accent="var(--accent-zig)"
    accentRgb="247, 164, 29"
    docsHref="/docs/zig/overview"
    quickstartHref="/docs/zig/quickstart"
    facts={[
      { label: "Focus", value: "Embedded systems, firmware, and small command-line programs" },
      { label: "Memory", value: "Fixed-buffer APIs for allocation-conscious call paths" },
      { label: "Queries", value: "SQL, key-value, and time-series operations over the PostgreSQL wire protocol" },
      { label: "Build", value: "Zig build system with cross-compilation" },
    ]}
  />;
}
