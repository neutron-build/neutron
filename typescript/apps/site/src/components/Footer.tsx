export default function Footer() {
  const year = new Date().getFullYear();

  return (
    <footer class="footer">
      <div class="footer__inner container">
        <div class="footer__left">
          <span class="footer__brand">neutron</span>
          <span class="footer__copyright">&copy; {year}</span>
          <span class="footer__license">MIT License</span>
        </div>
        <div class="footer__links">
          <a href="/typescript">TypeScript</a>
          <a href="/rust">Rust</a>
          <a href="/go">Go</a>
          <a href="/python">Python</a>
          <a href="/elixir">Elixir</a>
          <a href="/mojo">Mojo</a>
          <a href="/zig">Zig</a>
          <a href="/julia">Julia</a>
          <a href="/nucleus">Nucleus</a>
          <a href="/studio">Studio</a>
          <a href="/ai">AI</a>
          <a href="/agents">Agents</a>
          <a href="/workflow">Workflow</a>
          <a href="/native">Native</a>
          <a href="/docs">Docs</a>
          <a href="/blog">Blog</a>
          <a
            href="https://github.com/neutron-build/neutron"
            target="_blank"
            rel="noopener noreferrer"
          >
            GitHub
          </a>
        </div>
        <div class="footer__note">Built with Neutron.</div>
      </div>
    </footer>
  );
}
