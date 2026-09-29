import LanguageOverview from "../components/LanguageOverview";

export function head() {
  return { title: "Rust - Neutron", description: "Neutron’s modular Rust framework for async services, background workers, and application backends." };
}

export default function RustPage() {
  return <LanguageOverview
    name="Rust"
    description="A modular framework for asynchronous services, background workers, and application backends. Build the parts of your system that need Rust’s control over resources and concurrency, with Hyper, Tokio, and optional Neutron crates."
    accent="var(--accent-rust)"
    accentRgb="255, 107, 53"
    docsHref="/docs/rust/overview"
    quickstartHref="/docs/rust/quickstart"
    note="Use Neutron’s modules where they fit your Rust application. The current core focuses on networked applications; performance depends on the workload and must be measured."
    facts={[
      { label: "Runtime", value: "Tokio for async execution; Hyper for HTTP services" },
      { label: "HTTP", value: "Trie routing, typed extractors, middleware, WebSocket, and SSE" },
      { label: "Modules", value: "Optional jobs, Nucleus, PostgreSQL, storage, cache, and telemetry crates" },
      { label: "Applications", value: "API services, real-time systems, workers, and Neutron Desktop backends" },
    ]}
  />;
}
