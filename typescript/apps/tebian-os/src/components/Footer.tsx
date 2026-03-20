import { Link } from "neutron/client";

export function Footer() {
  return (
    <footer class="global-footer">
      <div class="footer-nav">
        <Link to="/">&larr; Home</Link>
        <span class="nav-div">|</span>
        <Link to="/manifesto">Manifesto</Link>
        <Link to="/reasoning">Reasoning</Link>
        <Link to="/honors">Honors</Link>
        <Link to="/source">Source</Link>
      </div>
      <div class="footer-extended">
        <Link to="/blog">Philosophy</Link>
        <span class="ext-sep">&middot;</span>
        <Link to="/docs">Guides</Link>
      </div>
    </footer>
  );
}
