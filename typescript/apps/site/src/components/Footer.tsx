export default function Footer() {
  const year = new Date().getFullYear();

  return (
    <footer class="footer">
      <div class="footer__inner container">
        <div class="footer__brand-block">
          <a class="footer__brand" href="/">neutron</a>
          <p>Frameworks and tools for Nucleus.</p>
        </div>
        <div class="footer__groups">
          <nav class="footer__group" aria-label="Frameworks">
            <span>Frameworks</span>
            <a href="/typescript">TypeScript</a>
            <a href="/rust">Rust</a>
            <a href="/go">Go</a>
            <a href="/python">Python</a>
            <a href="/elixir">Elixir</a>
            <a href="/docs#frameworks">All frameworks</a>
          </nav>
          <nav class="footer__group" aria-label="Platform">
            <span>Platform</span>
            <a href="/nucleus">Nucleus</a>
            <a href="/studio">Studio</a>
            <a href="/ai">AI</a>
            <a href="/agents">Agents</a>
            <a href="/workflow">Workflow</a>
            <a href="/native">Native</a>
          </nav>
          <nav class="footer__group" aria-label="Resources">
            <span>Resources</span>
            <a href="/docs">Documentation</a>
            <a href="/blog">Blog</a>
            <a href="/cli">CLI</a>
            <a href="https://github.com/neutron-build/neutron" target="_blank" rel="noopener noreferrer">GitHub</a>
          </nav>
        </div>
        <div class="footer__base">
          <div class="footer__legal">
            <span>&copy; {year} Neutron</span>
            <span>MIT License</span>
          </div>
          <p class="footer__note">
            Built on <a href="/typescript">Neutron</a>. Deployed with <a href="https://teploy.com" target="_blank" rel="noopener noreferrer">Teploy</a>.
          </p>
        </div>
      </div>
    </footer>
  );
}
