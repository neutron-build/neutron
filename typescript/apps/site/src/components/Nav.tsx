interface NavProps {
  activeProduct?: "typescript" | "rust" | "nucleus" | "mojo";
}

const languages = [
  { id: "typescript", label: "TypeScript", href: "/typescript" },
  { id: "rust", label: "Rust", href: "/rust" },
  { id: "go", label: "Go", href: "/go" },
  { id: "python", label: "Python", href: "/python" },
  { id: "elixir", label: "Elixir", href: "/elixir" },
  { id: "mojo", label: "Mojo", href: "/mojo" },
  { id: "zig", label: "Zig", href: "/zig" },
  { id: "julia", label: "Julia", href: "/julia" },
];

const verification = [
  { id: "lean", label: "Lean 4", desc: "Machine-checked proofs", href: "/lean" },
  { id: "quint", label: "Quint", desc: "Protocol verification", href: "/quint" },
  { id: "modelica", label: "Modelica", desc: "Physics simulation", href: "/modelica" },
];

const platforms = [
  { id: "web", label: "Web", desc: "Edge, Node, Serverless, Static", href: "/web" },
  { id: "native", label: "Mobile", desc: "Cross-platform native with Preact", href: "/native" },
  { id: "desktop", label: "Desktop", desc: "Lightweight apps with system WebView", href: "/desktop" },
];

const database = [
  { id: "nucleus", label: "Nucleus", desc: "14-in-1 database engine", href: "/nucleus" },
  { id: "client", label: "Client", desc: "Universal database SDK", href: "/client" },
  { id: "orm", label: "ORM", desc: "Type-safe multi-model queries", href: "/orm" },
  { id: "studio", label: "Studio", desc: "Visual database management", href: "/studio" },
];

const ai = [
  { id: "ai", label: "AI", desc: "Model calls, streaming, tools", href: "/ai" },
  { id: "agents", label: "Agents", desc: "Durable file-based agents", href: "/agents" },
  { id: "workflow", label: "Workflow", desc: "Event-sourced durable execution", href: "/workflow" },
];

export default function Nav({ activeProduct }: NavProps) {
  return (
    <>
      <nav class="nav" aria-label="Main navigation" id="main-nav">
        <div class="nav__inner container">
          <a href="/" class="nav__logo">neutron</a>

          {/* Full language bar — wide screens only */}
          <div class="nav__products" id="nav-products">
            <div class="nav__highlight" aria-hidden="true" id="nav-highlight"></div>
            <div class="nav__group">
              <span class="nav__label">Languages</span>
              <div class="nav__group-items">
                {languages.map((lang) => (
                  <a href={lang.href} class="nav__item" key={lang.id}>{lang.label}</a>
                ))}
              </div>
            </div>
            <span class="nav__divider"></span>
            <div class="nav__group">
              <span class="nav__label">Proof</span>
              <div class="nav__group-items">
                <a href="/lean" class="nav__item">Lean 4</a>
              </div>
            </div>
            <span class="nav__divider"></span>
            <div class="nav__group">
              <span class="nav__label">Modeling</span>
              <div class="nav__group-items">
                <a href="/quint" class="nav__item">Quint</a>
                <a href="/modelica" class="nav__item">Modelica</a>
              </div>
            </div>
          </div>

          {/* Dropdown triggers — all four use the same popover system */}
          <div class="nav__dropdowns">
            <button type="button" class="nav__item nav__trigger nav__trigger--compact" data-dropdown="languages">
              Languages <span class="nav__caret">&#9662;</span>
            </button>
            <button type="button" class="nav__item nav__trigger" data-dropdown="platforms">
              Platforms <span class="nav__caret">&#9662;</span>
            </button>
            <button type="button" class="nav__item nav__trigger" data-dropdown="database">
              Database <span class="nav__caret">&#9662;</span>
            </button>
            <button type="button" class="nav__item nav__trigger" data-dropdown="ai">
              AI <span class="nav__caret">&#9662;</span>
            </button>
          </div>

          <div class="nav__links">
            <a href="/cli" class="nav__link">CLI</a>
            <a href="/docs" class="nav__link">Docs</a>
            <a href="https://github.com/neutron-build/neutron" class="nav__link nav__link--icon" target="_blank" rel="noopener noreferrer" aria-label="GitHub">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z" />
              </svg>
            </a>
          </div>

          {/* Mobile hamburger — only visible below 768px */}
          <button type="button" class="nav__hamburger" id="nav-hamburger" aria-label="Open menu" aria-expanded="false" aria-controls="nav-drawer">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
              <line x1="4" y1="7" x2="20" y2="7" />
              <line x1="4" y1="12" x2="20" y2="12" />
              <line x1="4" y1="17" x2="20" y2="17" />
            </svg>
          </button>
        </div>
        <div class="nav__popover" aria-hidden="true" id="nav-popover">
          <div class="nav__popover-bg" id="nav-popover-bg"></div>
          <div class="nav__popover-content">
            <div class="nav__panel" data-panel="languages">
              <div class="nav__tile-grid nav__tile-grid--languages">
                {languages.map((l) => (
                  <a href={l.href} class="nav__tile nav__tile--compact" key={l.id}>
                    <span class="nav__tile-label">{l.label}</span>
                  </a>
                ))}
                <div class="nav__tile-divider"></div>
                {verification.map((v) => (
                  <a href={v.href} class="nav__tile nav__tile--compact" key={v.id}>
                    <span class="nav__tile-label">{v.label}</span>
                    <span class="nav__tile-desc">{v.desc}</span>
                  </a>
                ))}
              </div>
            </div>
            <div class="nav__panel" data-panel="platforms">
              <div class="nav__tile-grid">
                {platforms.map((p) => (
                  <a href={p.href} class="nav__tile" key={p.id}>
                    <span class="nav__tile-label">{p.label}</span>
                    <span class="nav__tile-desc">{p.desc}</span>
                  </a>
                ))}
              </div>
            </div>
            <div class="nav__panel" data-panel="database">
              <div class="nav__tile-grid nav__tile-grid--2x2">
                {database.map((d) => (
                  <a href={d.href} class="nav__tile" key={d.id}>
                    <span class="nav__tile-label">{d.label}</span>
                    <span class="nav__tile-desc">{d.desc}</span>
                  </a>
                ))}
              </div>
            </div>
            <div class="nav__panel" data-panel="ai">
              <div class="nav__tile-grid">
                {ai.map((a) => (
                  <a href={a.href} class="nav__tile" key={a.id}>
                    <span class="nav__tile-label">{a.label}</span>
                    <span class="nav__tile-desc">{a.desc}</span>
                  </a>
                ))}
              </div>
            </div>
          </div>
        </div>
      </nav>
      {/* Mobile drawer — deliberately OUTSIDE <nav>. .nav has backdrop-filter,
          which creates a new containing block for position:fixed descendants,
          so a fixed drawer nested inside it resolves inset:0 against the
          56px nav bar instead of the viewport. Keep it a sibling. */}
      <div class="nav__drawer" id="nav-drawer" aria-hidden="true">
        <div class="nav__drawer-backdrop" data-drawer-close></div>
        <div class="nav__drawer-panel" role="dialog" aria-modal="true" aria-label="Main navigation">
          <div class="nav__drawer-header">
            <a href="/" class="nav__logo">neutron</a>
            <button type="button" class="nav__drawer-close" data-drawer-close aria-label="Close menu">
              <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
                <line x1="6" y1="6" x2="18" y2="18" />
                <line x1="18" y1="6" x2="6" y2="18" />
              </svg>
            </button>
          </div>
          <nav class="nav__drawer-body" aria-label="Mobile menu">
            <div class="nav__drawer-section">
              <h3 class="nav__drawer-title">Languages</h3>
              {languages.map((l) => (
                <a href={l.href} class="nav__drawer-link" key={l.id}>{l.label}</a>
              ))}
            </div>
            <div class="nav__drawer-section">
              <h3 class="nav__drawer-title">Verification</h3>
              {verification.map((v) => (
                <a href={v.href} class="nav__drawer-link" key={v.id}>{v.label}</a>
              ))}
            </div>
            <div class="nav__drawer-section">
              <h3 class="nav__drawer-title">Platforms</h3>
              {platforms.map((p) => (
                <a href={p.href} class="nav__drawer-link" key={p.id}>{p.label}</a>
              ))}
            </div>
            <div class="nav__drawer-section">
              <h3 class="nav__drawer-title">Database</h3>
              {database.map((d) => (
                <a href={d.href} class="nav__drawer-link" key={d.id}>{d.label}</a>
              ))}
            </div>
            <div class="nav__drawer-section">
              <h3 class="nav__drawer-title">AI</h3>
              {ai.map((a) => (
                <a href={a.href} class="nav__drawer-link" key={a.id}>{a.label}</a>
              ))}
            </div>
            <div class="nav__drawer-section">
              <a href="/cli" class="nav__drawer-link nav__drawer-link--primary">CLI</a>
              <a href="/docs" class="nav__drawer-link nav__drawer-link--primary">Docs</a>
              <a href="https://github.com/neutron-build/neutron" class="nav__drawer-link nav__drawer-link--primary" target="_blank" rel="noopener noreferrer">GitHub</a>
            </div>
          </nav>
        </div>
      </div>
    </>
  );
}
