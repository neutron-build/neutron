export function head() {
  return {
    title: "Docs - Neutron",
    description: "Guides, API reference, and runbooks for the entire Neutron stack.",
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
      <span class="docs-landing__card-link">Read &rarr;</span>
    </a>
  );
}

export default function DocsIndex() {
  return (
    <div class="docs-landing">
      <header class="docs-landing__hero">
        <p class="docs-landing__eyebrow">Documentation</p>
        <h1 class="docs-landing__title">Everything you need to ship on Neutron.</h1>
        <p class="docs-landing__lead">
          Guides for the TypeScript meta-framework, per-language SDKs, and the Nucleus database engine.
          Jump in where you are &mdash; nothing here assumes you've read anything else first.
        </p>
      </header>

      <section class="docs-landing__section">
        <h2 class="docs-landing__section-title">Get started in minutes</h2>
        <div class="docs-landing__grid">
          <DocsCard
            title="Installation"
            description="Spin up a new Neutron project in one command. Covers Node, Bun, and Deno."
            href="/docs/getting-started/installation"
          />
          <DocsCard
            title="Your first route"
            description="File-based routing, loaders, and actions in under 20 lines of code."
            href="/docs/getting-started/your-first-route"
          />
          <DocsCard
            title="Project structure"
            description="What goes where and why. Runtime entry points, route modules, content collections."
            href="/docs/getting-started/project-structure"
          />
        </div>
      </section>

      <section class="docs-landing__section">
        <h2 class="docs-landing__section-title">TypeScript framework</h2>
        <div class="docs-landing__grid">
          <DocsCard
            title="Routing"
            description="Static, app, and dynamic routes. Nested layouts, error boundaries, file conventions."
            href="/docs/routing/file-conventions"
          />
          <DocsCard
            title="Data loading"
            description="Loaders, actions, forms, revalidation. End-to-end type safety from the handler to the component."
            href="/docs/data/loaders"
          />
          <DocsCard
            title="Rendering modes"
            description="Static, SSR, islands, view transitions. Pick the right mode per route."
            href="/docs/rendering/islands"
          />
          <DocsCard
            title="Content collections"
            description="Markdown + MDX with typed frontmatter, schemas, and first-class query helpers."
            href="/docs/content/content-collections"
          />
          <DocsCard
            title="Middleware"
            description="Global and per-route middleware. Request context, auth gates, rate limits."
            href="/docs/middleware/global-middleware"
          />
          <DocsCard
            title="Deployment"
            description="Node, Cloudflare, Vercel, Docker, static hosting. One build, many targets."
            href="/docs/deployment/adapters"
          />
        </div>
      </section>

      <section class="docs-landing__section">
        <h2 class="docs-landing__section-title">Nucleus database</h2>
        <p class="docs-landing__section-lead">
          One engine, fourteen data models. PostgreSQL wire compatible so any client just works.
        </p>
        <div class="docs-landing__grid">
          <DocsCard
            title="Overview"
            description="How Nucleus packs SQL, KV, Vector, Graph, Documents, TimeSeries, and eight more into a single process."
            href="/docs/nucleus/overview"
          />
          <DocsCard
            title="Quick start"
            description="Connect from psql, run a query across two data models in one transaction."
            href="/docs/nucleus/quickstart"
          />
          <DocsCard
            title="SQL reference"
            description="PostgreSQL dialect, extended with functions that reach into every other model."
            href="/docs/nucleus/sql"
          />
          <DocsCard
            title="Vector search"
            description="HNSW and IVFFlat indexes. SIMD distance functions. Hybrid SQL + vector queries."
            href="/docs/nucleus/vector"
          />
          <DocsCard
            title="Replication"
            description="Multi-Raft replication, read replicas, and disaster recovery runbook."
            href="/docs/nucleus/replication"
          />
          <DocsCard
            title="Security"
            description="Auth, roles, row-level security, audit logs. Production defaults, not checklists."
            href="/docs/nucleus/security"
          />
        </div>
      </section>

      <section class="docs-landing__section">
        <h2 class="docs-landing__section-title">Language SDKs</h2>
        <p class="docs-landing__section-lead">
          Seven SDKs, one contract. Same wire protocol, same error format, same health checks &mdash; in the idioms of each language.
        </p>
        <div class="docs-landing__grid docs-landing__grid--compact">
          <DocsCard title="Rust" description="Hyper + Tokio. Trie router, 19 crates, full auth." href="/docs/rust/overview" />
          <DocsCard title="Go" description="net/http, idiomatic router, OAuth + WebAuthn." href="/docs/go/overview" />
          <DocsCard title="Python" description="Starlette + Pydantic v2 + asyncpg, async-first." href="/docs/python/overview" />
          <DocsCard title="Elixir" description="Plug + Bandit, OTP supervisors, hot reload." href="/docs/elixir/index" />
          <DocsCard title="Zig" description="12 KB binary, zero heap, comptime SQL validation." href="/docs/zig/overview" />
          <DocsCard title="Julia" description="DifferentialEquations.jl, CUDA, FMI interop." href="/docs/julia/overview" />
          <DocsCard title="Mojo" description="GPU kernels, quantized inference, training stack." href="/docs/mojo/overview" badge="Preview" />
        </div>
      </section>

      <section class="docs-landing__section">
        <h2 class="docs-landing__section-title">Platforms &amp; tools</h2>
        <div class="docs-landing__grid docs-landing__grid--compact">
          <DocsCard title="Native (iOS/Android)" description="Preact components that render to native Fabric views." href="/docs/native/overview" />
          <DocsCard title="CLI" description="Create, dev, build, deploy, db, studio &mdash; one binary." href="/docs/cli/overview" />
          <DocsCard title="Studio" description="Visual browser for every Nucleus data model." href="/docs/studio/overview" />
          <DocsCard title="Verification" description="Machine-checked proofs across Lean 4, Quint, Verus." href="/docs/verification/overview" />
        </div>
      </section>

      <section class="docs-landing__section">
        <h2 class="docs-landing__section-title">API reference</h2>
        <div class="docs-landing__grid docs-landing__grid--compact">
          <DocsCard title="Route exports" description="loader, action, head, config, middleware." href="/docs/api/route-exports" />
          <DocsCard title="Components" description="Island, ViewTransitions, Outlet." href="/docs/api/components" />
          <DocsCard title="Hooks" description="useLoaderData, useActionData, useFormState." href="/docs/api/hooks" />
          <DocsCard title="Server utilities" description="redirect, json, error helpers, cookies, CSRF." href="/docs/api/server-utilities" />
          <DocsCard title="Content API" description="getCollection, getEntry, defineCollection, schemas." href="/docs/api/content-api" />
        </div>
      </section>
    </div>
  );
}
