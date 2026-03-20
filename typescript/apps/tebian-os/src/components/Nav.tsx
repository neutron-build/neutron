import { Link } from "neutron/client";

export function Nav() {
  return (
    <nav class="global-nav">
      <Link to="/">&larr; Home</Link>
      <span class="nav-div">|</span>
      <Link to="/manifesto">Manifesto</Link>
      <Link to="/reasoning">Reasoning</Link>
      <Link to="/honors">Honors</Link>
      <Link to="/source">Source</Link>
    </nav>
  );
}
