import LanguageOverview from "../components/LanguageOverview";

export function head() {
  return { title: "Zig - Neutron", description: "Neutron’s layered Zig systems library, application framework, and Nucleus client." };
}

export default function ZigPage() {
  return <LanguageOverview
    name="Zig"
    description="A layered systems library that grows from wire codecs and networking into protocol servers, an application framework, and a Nucleus client. Choose the layers your program needs."
    accent="var(--accent-zig)"
    accentRgb="247, 164, 29"
    docsHref="/docs/zig/overview"
    quickstartHref="/docs/zig/quickstart"
    note="The source includes protocol and application layers. Support for a particular embedded target needs validation; the full client uses heap allocation."
    facts={[
      { label: "Layers", value: "Wire codecs, networking, protocol servers, and application framework" },
      { label: "Memory", value: "Allocation-free wire codecs; allocator-backed higher-level clients" },
      { label: "Application", value: "Router, middleware, lifecycle, and a multi-model Nucleus client" },
      { label: "Toolchain", value: "Zig 0.15.2; layers selected through build flags" },
    ]}
  />;
}
