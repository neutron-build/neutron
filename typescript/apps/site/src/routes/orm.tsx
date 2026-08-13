import ProductOverview from "../components/ProductOverview";

export function head() {
  return {
    title: "Data and Drizzle - Neutron",
    description: "Optional Drizzle database profiles and backend drivers for Neutron TypeScript.",
  };
}

export default function ORMPage() {
  return <ProductOverview
    title="Data and Drizzle"
    description="Optional TypeScript database profiles and backend drivers provided by @neutron-build/data."
    category="database"
    accent="var(--accent-orm)"
    accentRgb="245, 158, 11"
    facts={[
      { label: "SQL", value: "Drizzle profiles for SQLite, PostgreSQL, and Nucleus" },
      { label: "Drivers", value: "Cache, sessions, queues, storage, realtime, and rate-limit primitives" },
      { label: "Dependencies", value: "Third-party database and service clients are optional and loaded on demand" },
      { label: "Nucleus", value: "PostgreSQL transport for SQL with an optional multi-model client" },
    ]}
    note="This is an optional data package, not a separate multi-model ORM. Non-SQL Nucleus features use the Nucleus client plugins."
    links={[
      { label: "Data overview", href: "/docs/data-layer/overview" },
      { label: "Database setup", href: "/docs/data-layer/database" },
      { label: "Package source", href: "https://github.com/neutron-build/neutron/tree/main/typescript/packages/neutron-data" },
    ]}
  />;
}
