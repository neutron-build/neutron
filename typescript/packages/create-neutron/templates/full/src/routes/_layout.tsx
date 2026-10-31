import type { JsxChildren } from "@neutron-build/core/client";
export function head() {
  return { link: { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" } };
}

export default function Layout(props: { children?: JsxChildren }) {
  return (
    <div style={{ maxWidth: "900px", margin: "0 auto", padding: "2rem", fontFamily: "system-ui, sans-serif" }}>
      <header style={{ marginBottom: "1.5rem" }}>
        <h1 style={{ margin: "0" }}>__PROJECT_NAME__</h1>
        <p style={{ color: "#666" }}>Neutron full-stack template</p>
      </header>
      <nav style={{ display: "flex", gap: "0.75rem", marginBottom: "1.5rem" }}>
        <a href="/">Home</a>
        <a href="/pricing">Pricing</a>
        <a href="/app/dashboard">Dashboard</a>
        <a href="/app/settings">Settings</a>
      </nav>
      <main>{props.children}</main>
    </div>
  );
}
