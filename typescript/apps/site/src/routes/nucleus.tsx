import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "Nucleus - Neutron",
    description: "A Rust multi-model database engine using the PostgreSQL wire protocol.",
  };
}

export default function NucleusPage() {
  return <ProductOverview
    title="Nucleus"
    description="A Rust database engine that exposes relational and non-relational models through one PostgreSQL-compatible server."
    category="database"
    accent="var(--accent-nucleus)"
    accentRgb="168, 85, 247"
    facts={[
      { label: "Protocol", value: "PostgreSQL wire protocol for standard clients and Neutron SDKs" },
      { label: "Models", value: "SQL, KV, Vector, Document, Graph, FTS, TimeSeries, Columnar, Blob, Datalog, Streams, Geo, CDC, and PubSub" },
      { label: "Engine", value: "Rust server with MVCC, WAL, indexing, security, and replication modules" },
      { label: "License", value: "Business Source License 1.1 with an Additional Use Grant and MIT change license" },
    ]}
    note="Model maturity varies. Consult the model-specific documentation and repository tests before selecting a feature for production use."
    links={[
      { label: "Quickstart", href: "/docs/nucleus/quickstart" },
      { label: "Documentation", href: "/docs/nucleus/overview" },
      { label: "License", href: "https://github.com/neutron-build/neutron/blob/main/nucleus/LICENSE" },
      { label: "Source", href: "https://github.com/neutron-build/neutron/tree/main/nucleus" },
    ]}
  />;
}
