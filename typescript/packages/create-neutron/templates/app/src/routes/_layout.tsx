export default function Layout(props: { children?: unknown }) {
  return (
    <div style="max-width: 900px; margin: 0 auto; padding: 2rem; font-family: system-ui, sans-serif;">
      <header style="margin-bottom: 1.5rem;">
        <h1 style="margin: 0;">__PROJECT_NAME__</h1>
        <p style="color: #666;">Neutron SaaS template</p>
      </header>
      <nav style="display: flex; gap: 0.75rem; margin-bottom: 1.5rem;">
        <a href="/">Home</a>
        <a href="/app/dashboard">Dashboard</a>
        <a href="/app/settings">Settings</a>
        <a href="/login">Login</a>
        <a href="/protected?token=demo">Protected</a>
      </nav>
      <main>{props.children}</main>
    </div>
  );
}
