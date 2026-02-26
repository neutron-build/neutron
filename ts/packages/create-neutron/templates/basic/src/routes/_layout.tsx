export default function Layout(props: { children?: unknown }) {
  return (
    <div style="max-width: 900px; margin: 0 auto; padding: 2rem; font-family: system-ui, sans-serif;">
      <header style="margin-bottom: 1.5rem;">
        <h1 style="margin: 0;">__PROJECT_NAME__</h1>
        <p style="color: #666;">Neutron starter template</p>
      </header>
      <nav style="margin-bottom: 1.5rem;">
        <a href="/">Home</a>
        {" | "}
        <a href="/users/1">User 1</a>
      </nav>
      <main>{props.children}</main>
    </div>
  );
}
