import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "Nucleus Client - Neutron",
    description: "The modular TypeScript client for Nucleus.",
  };
}

export default function ClientPage() {
  return <ProductOverview
    title="Nucleus Client"
    description="A modular TypeScript client that connects to Nucleus and adds only the data-model plugins an application uses."
    category="database"
    accent="var(--accent-client)"
    accentRgb="6, 182, 212"
    facts={[
      { label: "Package", value: "@neutron-build/nucleus" },
      { label: "Core", value: "Client builder, feature detection, transports, migrations, errors, and transaction retry" },
      { label: "Models", value: "SQL plus optional KV, Vector, Document, Graph, FTS, Geo, Blob, TimeSeries, Streams, Columnar, Datalog, CDC, and PubSub plugins" },
      { label: "Transports", value: "PostgreSQL, HTTP, mobile, and embedded transport interfaces" },
    ]}
    note="The language SDKs have their own idiomatic Nucleus clients. This page describes the current TypeScript package rather than claiming one identical API across every language."
    links={[
      { label: "Nucleus docs", href: "/docs/nucleus/overview" },
      { label: "TypeScript source", href: "https://github.com/neutron-build/neutron/tree/main/typescript/packages/neutron-nucleus" },
      { label: "Nucleus", href: "/nucleus" },
    ]}
  />;
}
