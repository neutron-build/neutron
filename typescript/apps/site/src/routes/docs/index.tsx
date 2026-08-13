export function head() {
  return {
    title: "Documentation - Neutron",
    description: "Documentation for Neutron frameworks, Nucleus, and the Neutron CLI.",
  };
}

interface DocsCardProps {
  title: string;
  description: string;
  href: string;
  badge?: string;
}

function DocsCard({ title, description, href, badge }: DocsCardProps) {
  return (
    <a class="docs-landing__card" href={href}>
      <div class="docs-landing__card-head">
        <span class="docs-landing__card-title">{title}</span>
        {badge && <span class="docs-landing__card-badge">{badge}</span>}
      </div>
      <p class="docs-landing__card-desc">{description}</p>
      <span class="docs-landing__card-link">Open &rarr;</span>
    </a>
  );
}

export default function DocsIndex() {
  return (
    <div class="docs-landing">
      <header class="docs-landing__hero">
        <p class="docs-landing__eyebrow">Documentation</p>
        <h1 class="docs-landing__title">Start with the part of Neutron you use.</h1>
        <p class="docs-landing__lead">
          Framework guides, Nucleus references, and deployment instructions.
        </p>
      </header>

      <section class="docs-landing__section">
        <h2 class="docs-landing__section-title">Start here</h2>
        <div class="docs-landing__grid">
          <DocsCard title="TypeScript quickstart" description="Create a Preact project and add your first route." href="/docs/getting-started/installation" />
          <DocsCard title="Nucleus quickstart" description="Connect over the PostgreSQL wire protocol and run a query." href="/docs/nucleus/quickstart" />
          <DocsCard title="CLI" description="Create, develop, build, and inspect Neutron projects." href="/docs/cli/overview" />
        </div>
      </section>

      <section class="docs-landing__section" id="frameworks">
        <h2 class="docs-landing__section-title">Frameworks and clients</h2>
        <p class="docs-landing__section-lead">Each implementation follows the shared framework contract but uses its language's own runtime and package conventions.</p>
        <div class="docs-landing__grid docs-landing__grid--compact">
          <DocsCard title="TypeScript" description="Routing, rendering, loaders, actions, and content." href="/docs/routing/file-conventions" />
          <DocsCard title="Rust" description="Hyper-based HTTP framework and optional crates." href="/docs/rust/overview" />
          <DocsCard title="Go" description="Standard-library HTTP framework and modular data client." href="/docs/go/overview" />
          <DocsCard title="Python" description="Starlette application framework and async data access." href="/docs/python/overview" />
          <DocsCard title="Elixir" description="Plug and Bandit under OTP supervision." href="/docs/elixir/index" />
          <DocsCard title="Zig" description="Allocation-conscious Nucleus client." href="/docs/zig/overview" />
          <DocsCard title="Julia" description="Nucleus access for data and scientific workflows." href="/docs/julia/overview" />
          <DocsCard title="Mojo" description="Tensor and inference libraries for Mojo." href="/docs/mojo/overview" badge="Preview" />
        </div>
      </section>

      <section class="docs-landing__section">
        <h2 class="docs-landing__section-title">Reference</h2>
        <div class="docs-landing__grid docs-landing__grid--compact">
          <DocsCard title="Nucleus models" description="SQL and optional multi-model capabilities." href="/docs/nucleus/overview" />
          <DocsCard title="TypeScript API" description="Route exports, components, hooks, and server utilities." href="/docs/api/route-exports" />
          <DocsCard title="Deployment" description="Adapters and supported deployment targets." href="/docs/deployment/adapters" />
          <DocsCard title="Framework contract" description="Shared behavior across the language SDKs." href="https://github.com/neutron-build/neutron/blob/main/FRAMEWORK_CONTRACT.md" />
        </div>
      </section>
    </div>
  );
}
