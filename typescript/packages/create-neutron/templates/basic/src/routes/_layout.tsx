import type { JsxChildren } from "@neutron-build/core/client";
export function head() {
  return { link: { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" } };
}

export default function Layout(props: { children?: JsxChildren }) {
  return (
    <div style={{ maxWidth: "900px", margin: "0 auto", padding: "2rem", fontFamily: "system-ui, sans-serif" }}>
      <header style={{ marginBottom: "1.5rem" }}>
        <h1 style={{ margin: "0" }}>__PROJECT_NAME__</h1>
        <p style={{ color: "#666" }}>Neutron starter template</p>
      </header>
      <nav style={{ marginBottom: "1.5rem" }}>
        <a href="/">Home</a>
        {" | "}
        <a href="/users/1">User 1</a>
      </nav>
      <main>{props.children}</main>
    </div>
  );
}
