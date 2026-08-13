import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "CLI - Neutron",
    description: "The Go command-line interface for Neutron projects and Nucleus.",
  };
}

export default function CliPage() {
  return <ProductOverview
    title="Neutron CLI"
    description="A Go command-line interface for project workflows, Nucleus operations, Studio, and machine-readable tooling."
    category="tool"
    accent="var(--accent-ts)"
    accentRgb="49, 120, 198"
    facts={[
      { label: "Projects", value: "new, init, dev, doctor, upgrade, and version commands" },
      { label: "Database", value: "db lifecycle, migrations, seeds, REPL, and typed schema generation" },
      { label: "Platforms", value: "Native and desktop command groups plus Studio launch" },
      { label: "Agents", value: "MCP over stdio or HTTP, with read-only SQL unless writes are explicitly enabled" },
    ]}
    note="Commands delegate to the relevant language toolchain where appropriate. Their availability still depends on that toolchain and the current project configuration."
    links={[
      { label: "Command reference", href: "/docs/cli/commands" },
      { label: "CLI documentation", href: "/docs/cli/overview" },
      { label: "Source", href: "https://github.com/neutron-build/neutron/tree/main/cli" },
    ]}
  />;
}
