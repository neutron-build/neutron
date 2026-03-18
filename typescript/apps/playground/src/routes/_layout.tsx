import type { ErrorBoundaryProps } from "neutron";

export default function Layout({ children }: { children: preact.ComponentChildren }) {
  return (
    <div>
      <nav style={{ marginBottom: "1rem", paddingBottom: "1rem", borderBottom: "1px solid #333" }}>
        <a href="/">Home</a>
        {" | "}
        <a href="/about">About</a>
        {" | "}
        <a href="/dashboard">Dashboard</a>
        {" | "}
        <a href="/blog">Blog</a>
        {" | "}
        <a href="/islands">Islands</a>
        {" | "}
        <a href="/users">Users</a>
        {" | "}
        <a href="/todos">Todos</a>
        {" | "}
        <a href="/admin">Admin</a>
        {" | "}
        <a href="/protected">Protected</a>
        {" | "}
        <a href="/error-demo">Error Demo</a>
      </nav>
      <main>{children}</main>
    </div>
  );
}

export function ErrorBoundary({ error }: ErrorBoundaryProps) {
  return (
    <div style="padding: 2rem;">
      <h1 style="color: #FF4444;">Layout Error</h1>
      <p>An error occurred while rendering this page.</p>
      <p style="color: #888;">{error.message}</p>
      <p style="margin-top: 1rem;">
        <a href="/" style="color: #00E5A0;">Go home</a>
      </p>
    </div>
  );
}
