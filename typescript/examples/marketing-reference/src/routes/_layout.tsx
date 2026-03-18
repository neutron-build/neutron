export default function MarketingLayout({
  children,
}: {
  children: preact.ComponentChildren;
}) {
  return (
    <div style="max-width: 860px; margin: 0 auto; padding: 2rem 1rem;">
      <header style="display: flex; justify-content: space-between; margin-bottom: 1.5rem;">
        <strong>Neutron Marketing</strong>
        <nav style="display: flex; gap: 0.75rem;">
          <a href="/">Home</a>
          <a href="/about">About</a>
          <a href="/blog">Blog</a>
        </nav>
      </header>
      <main>{children}</main>
    </div>
  );
}
