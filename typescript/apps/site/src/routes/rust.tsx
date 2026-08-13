import LanguageOverview from "../components/LanguageOverview";

export function head() {
  return { title: "Rust - Neutron", description: "Neutron's asynchronous Rust web framework built on Hyper." };
}

export default function RustPage() {
  return <LanguageOverview
    name="Rust"
    description="An asynchronous web framework built on Hyper and Tokio, with typed extractors and composable middleware."
    accent="var(--accent-rust)"
    accentRgb="255, 107, 53"
    docsHref="/docs/rust/overview"
    quickstartHref="/docs/rust/quickstart"
    facts={[
      { label: "Runtime", value: "Hyper and Tokio" },
      { label: "HTTP", value: "Trie routing, typed extractors, middleware, WebSocket, and SSE" },
      { label: "Data", value: "Optional Nucleus and PostgreSQL crates" },
      { label: "Deployment", value: "A compiled Rust binary" },
    ]}
  />;
}
