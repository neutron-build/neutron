import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "Studio - Neutron",
    description: "A Preact interface for inspecting and working with Nucleus data.",
  };
}

export default function StudioPage() {
  return <ProductOverview
    title="Neutron Studio"
    description="A visual database workspace for SQL schemas and Nucleus data models."
    category="database"
    status="in-progress"
    accent="var(--accent-studio)"
    accentRgb="236, 72, 153"
    facts={[
      { label: "SQL", value: "Schema browser, CodeMirror query editor, data grid, and schema designer" },
      { label: "Models", value: "Dedicated views for KV, Vector, Document, Graph, FTS, Geo, Blob, TimeSeries, Streams, Columnar, Datalog, CDC, and PubSub" },
      { label: "Interface", value: "Preact application with tabs, command palette, connection management, and exports" },
      { label: "Launch", value: "Available through the neutron studio CLI command" },
    ]}
    note="Studio is private workspace software in this monorepo and remains under active development."
    links={[
      { label: "Documentation", href: "/docs/studio/overview" },
      { label: "Query editor", href: "/docs/studio/query-editor" },
      { label: "Source", href: "https://github.com/neutron-build/neutron/tree/main/studio" },
    ]}
  />;
}
