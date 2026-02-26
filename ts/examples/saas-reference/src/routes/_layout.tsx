export default function SaasLayout({
  children,
}: {
  children: preact.ComponentChildren;
}) {
  return (
    <div style="max-width: 960px; margin: 0 auto; padding: 2rem 1rem;">
      <header style="display: flex; justify-content: space-between; margin-bottom: 1.5rem;">
        <strong>Neutron SaaS</strong>
        <nav style="display: flex; gap: 0.75rem;">
          <a href="/">Home</a>
          <a href="/dashboard">Dashboard</a>
          <a href="/tickets">Tickets</a>
        </nav>
      </header>
      <main>{children}</main>
    </div>
  );
}
