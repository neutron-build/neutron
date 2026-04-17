import Terminal from "../components/Terminal";
import CodeBlock from "../components/CodeBlock";
import MetricCard from "../components/MetricCard";
import PerformanceComparison from "../components/PerformanceComparison";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import ComparisonTable from "../components/ComparisonTable";

export default function TypeScriptPage() {
  return (
    <>
      <main id="main-content">
        {/* HERO */}
        <section class="hero">
          <div class="container container--narrow">
            <p class="hero__flagship" data-animate>Flagship</p>
            <h1 class="hero__title" data-animate style={{ "--animate-delay": "0.05s" } as any}>The web framework with two modes.</h1>
            <div class="hero__lines" data-animate style={{ "--animate-delay": "0.1s" } as any}>
              <p>Static routes ship zero JavaScript.</p>
              <p>App routes ship the full interactive experience.</p>
              <p>Same project. Same router. You choose.</p>
            </div>
            <div class="hero__install" data-animate style={{ "--animate-delay": "0.2s" } as any}>
              <Terminal command="npm create neutron@latest" />
            </div>
          </div>
        </section>

        {/* TWO MODES */}
        <section class="modes">
          <div class="container container--code">
            <h2 class="section-label" data-animate>Two modes, one router</h2>
            <div class="modes__grid" data-animate style={{ "--animate-delay": "0.1s" } as any}>
              <div class="modes__col">
                <CodeBlock filename="routes/about.tsx" annotation="Output: 1.2 KB HTML. Zero JavaScript. 100 Lighthouse.">
                  <pre><code>{`export const config = { mode: "static" };

export default function About() {
  return (
    <main>
      <h1>About Us</h1>
      <p>We build tools for developers.</p>
    </main>
  );
}`}</code></pre>
                </CodeBlock>
                <span class="modes__label">Static route</span>
              </div>
              <div class="modes__divider"></div>
              <div class="modes__col">
                <CodeBlock filename="routes/app/dashboard.tsx" annotation="Output: SSR + 3 KB Preact hydration. Typed. Interactive.">
                  <pre><code>{`export const config = { mode: "app" };

export async function loader({ context }: LoaderArgs) {
  return {
    projects: await context.db.getProjects(),
  };
}

export default function Dashboard() {
  const { projects } = useLoaderData<typeof loader>();
  return (
    <main>
      <h1>Dashboard</h1>
      <ul>
        {projects.map(p => (
          <li key={p.id}>{p.name}</li>
        ))}
      </ul>
    </main>
  );
}`}</code></pre>
                </CodeBlock>
                <span class="modes__label">App route</span>
              </div>
            </div>
          </div>
        </section>

        {/* FEATURES */}
        <section class="features">
          <div class="container container--narrow">
            <h2 class="section-label" data-animate>What you get</h2>
          </div>
          <FeatureGrid columns={2} accentRgb="49, 120, 198">
            <div class="feature-card">
              <div class="feature-card__title">File-based nested routing</div>
              <div class="feature-card__desc">Parent layouts stay mounted during navigation. Dynamic params. Catch-all routes. Error boundaries per route.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Loaders</div>
              <div class="feature-card__desc">Server-only data functions. Run in parallel. Full TypeScript inference from loader return type to component props.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Actions + Forms</div>
              <div class="feature-card__desc">Mutations via &lt;Form&gt;. Works without JavaScript. After every action, all data automatically revalidates.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Islands</div>
              <div class="feature-card__desc">On static routes, opt into interactivity per-component with &lt;Island&gt;. No framework runtime shipped.</div>
            </div>
          </FeatureGrid>
        </section>

        {/* FULL EXAMPLE */}
        <section class="example">
          <div class="container container--code">
            <h2 class="section-label" data-animate>The complete picture</h2>
            <p class="example__desc" data-animate style={{ "--animate-delay": "0.1s" } as any}>One file. Loader fetches data. Action handles the form. Component renders it. Error boundary catches failures. TypeScript connects everything.</p>
            <CodeBlock filename="routes/app/projects/[id].tsx" annotation="typeof loader → useLoaderData → component. End-to-end type safety. No codegen. No runtime validation.">
              <pre><code>{`import type { LoaderArgs, ActionArgs } from "@neutron-build/core";

export const config = { mode: "app" };

export async function loader({ params, context }: LoaderArgs) {
  const project = await context.db.getProject(params.id);
  if (!project) throw new Response("Not found", { status: 404 });
  return { project };
}

export async function action({ request, params, context }: ActionArgs) {
  const form = await request.formData();
  await context.db.updateProject(params.id, {
    name: form.get("name") as string,
  });
  return { saved: true };
}

export default function ProjectPage() {
  const { project } = useLoaderData<typeof loader>();
  const action = useActionData<typeof action>();

  return (
    <div>
      <h1>{project.name}</h1>
      {action?.saved && <p>Saved.</p>}
      <Form method="post">
        <input name="name" defaultValue={project.name} />
        <button type="submit">Save</button>
      </Form>
    </div>
  );
}

export function ErrorBoundary() {
  const error = useRouteError();
  if (isRouteErrorResponse(error)) {
    return <p>{error.status}: Not found.</p>;
  }
  return <p>Something went wrong.</p>;
}`}</code></pre>
            </CodeBlock>
          </div>
        </section>

        {/* NESTED LAYOUTS */}
        <section class="layouts-section">
          <div class="container container--narrow">
            <h2 class="section-label" data-animate>Layouts that persist</h2>
            <p class="layouts-section__desc" data-animate style={{ "--animate-delay": "0.1s" } as any}>Navigate from /app/dashboard to /app/settings. The root layout stays. The app layout stays. Only the page swaps. No re-render. No flash. No lost state.</p>
            <div class="tree" data-animate style={{ "--animate-delay": "0.15s" } as any}>
              <pre><code>{`src/routes/
  _layout.tsx            \u2190 stays mounted
  app/
    _layout.tsx          \u2190 stays mounted
    dashboard.tsx        \u2190 unmounts
    settings.tsx         \u2190 mounts
    projects/
      _layout.tsx        \u2190 stays mounted between projects
      [id].tsx`}</code></pre>
            </div>
          </div>
        </section>

        {/* ISLANDS */}
        <section class="islands-section">
          <div class="container container--code">
            <h2 class="section-label" data-animate>Interactive islands in a sea of HTML</h2>
            <p class="islands-section__desc" data-animate style={{ "--animate-delay": "0.1s" } as any}>Static routes ship zero JavaScript. When you need interactivity, wrap a component in &lt;Island&gt;. Only that component hydrates. Everything else stays as HTML.</p>
            <CodeBlock filename="routes/index.tsx">
              <pre><code>{`import { Island } from "@neutron-build/core";
import Counter from "../components/Counter";

export default function Home() {
  return (
    <main>
      <h1>Welcome</h1>
      <p>This is HTML. No JavaScript.</p>

      <Island component={Counter} client="visible" start={0} />

      <footer>Also HTML. Also no JavaScript.</footer>
    </main>
  );
}`}</code></pre>
            </CodeBlock>
            <div class="islands-section__directives">
              <div class="directive">
                <code>client="load"</code>
                <span>Hydrate immediately.</span>
              </div>
              <div class="directive">
                <code>client="visible"</code>
                <span>Hydrate when scrolled into view.</span>
              </div>
              <div class="directive">
                <code>client="idle"</code>
                <span>Hydrate when the browser is idle.</span>
              </div>
              <div class="directive">
                <code>client="media"</code>
                <span>Hydrate when a media query matches.</span>
              </div>
            </div>
          </div>
        </section>

        {/* COMPARISON */}
        <section class="comparison">
          <div class="container">
            <h2 class="section-label" data-animate>How Neutron compares</h2>
            <ComparisonTable
              headers={['', 'Neutron', 'Next.js', 'Remix', 'Astro', 'SvelteKit', 'Nuxt', 'SolidStart']}
              rows={[
                ['Static routes', 'Zero JS', 'Requires React', 'Requires React', 'Zero JS', 'Svelte runtime', 'Requires Vue', 'Solid runtime'],
                ['App routes', 'Preact or React', 'Requires React', 'Requires React', 'Limited', 'Svelte', 'Vue', 'Solid'],
                ['Client runtime', '3 KB (Preact)', '~42 KB', '~42 KB', '0 KB', '~2 KB', '~30 KB', '~8 KB'],
                ['Data loading', 'Parallel loaders', 'Parallel (App Router)', 'Parallel loaders', 'Astro.glob', 'load()', 'useFetch', 'createResource'],
                ['Data layer', 'DB + Cache + Queue', 'DIY', 'DIY', 'DIY', 'DIY', 'DIY', 'DIY'],
                ['Mutations', 'Actions + Form', 'Server Actions', 'Actions + Form', 'Limited', 'Form actions', 'Server routes', 'Actions'],
                ['Nested layouts', 'Yes', 'Yes (App Router)', 'Yes', 'Yes', 'Yes', 'Yes', 'Yes'],
                ['Islands', 'Yes', 'No', 'No', 'Yes', 'No', 'No', 'No'],
                ['Rendering modes', '2 (explicit)', '5+ (implicit)', 'SSR + pre-render', 'SSG + SSR + hybrid', 'SSR + SSG + hybrid', 'SSR + SSG + hybrid', 'SSR + SSG'],
                ['Deploy targets', 'Any (adapters)', 'Vercel-biased', 'Any', 'Any (adapters)', 'Any (adapters)', 'Any (presets)', 'Any (adapters)'],
              ]}
              highlightColumn={1}
              accentRgb="49, 120, 198"
            />
            <p class="comparison__note">Neutron takes inspiration from the best ideas across these frameworks. Every framework makes trade-offs — Neutron makes different ones.</p>
          </div>
        </section>

        {/* SERVER PERFORMANCE */}
        <section id="server-performance" class="server-performance">
          <div class="container">
            <h2 class="section-label" data-animate>Server Performance</h2>
            <p class="server-performance__intro" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              Benchmarks across 8 scenarios. Production builds. Same hardware. autocannon with 80 concurrent connections.
            </p>

            <div class="server-performance__metrics">
              <MetricCard value="~3,500" label="Avg Requests/sec" description="Across 8 scenarios" variant="excellent" animateDelay={0.1} />
              <MetricCard value="8,262" label="Peak RPS" description="Static pages" variant="excellent" animateDelay={0.15} />
              <MetricCard value="~4x" label="Faster than Next.js" description="vs ~830 RPS average" variant="good" animateDelay={0.2} />
              <MetricCard value="~5x" label="Faster than Astro" description="vs ~634 RPS average" variant="good" animateDelay={0.25} />
            </div>

            <BenchmarkBars
              bars={[
                { label: 'Neutron', value: '~3,500 RPS avg', width: 100, color: 'var(--accent-ts)' },
                { label: 'Neutron (React)', value: '~2,870 RPS avg', width: 82, color: '#5A8EC6' },
                { label: 'Next.js', value: '~830 RPS avg', width: 24, color: '#666666' },
                { label: 'Astro', value: '~634 RPS avg', width: 18, color: '#FF5D01' },
                { label: 'Remix 3', value: '~277 RPS avg', width: 8, color: '#888888' },
              ]}
            />

            <div class="server-performance__framework-table" data-animate style={{ "--animate-delay": "0.3s" } as any}>
              <h3>Framework Comparison</h3>
              <p class="server-performance__desc">Average performance across 8 benchmark scenarios. Production builds, same hardware.</p>
              <PerformanceComparison />
            </div>

            <div class="server-performance__scenarios" data-animate style={{ "--animate-delay": "0.35s" } as any}>
              <h3>Why Neutron is Faster</h3>
              <p class="server-performance__desc">The performance gap comes from architecture, not tricks:</p>
              <div class="server-performance__breakdown">
                <p><strong>Static routes skip the render pipeline.</strong> No React, no virtual DOM, no component tree. A static route is string concatenation. This is why static pages hit 8,262 RPS while Next.js hits 2,756.</p>
                <p><strong>App routes use Preact instead of React.</strong> Preact's server render is lighter — 3 KB vs ~42 KB. That means faster SSR and smaller bundles. Your database will still be the bottleneck, but you'll have more headroom.</p>
              </div>
            </div>

            <div class="server-performance__cost" data-animate style={{ "--animate-delay": "0.4s" } as any}>
              <h3>What This Means in Practice</h3>
              <p class="server-performance__cost-desc">A ~4x throughput advantage means you can serve the same traffic with fewer servers. For most applications, the database is the real bottleneck — but when you do need to scale horizontally, that headroom matters.</p>
              <p class="server-performance__note">
                All benchmarks are production builds tested with autocannon (80 concurrent connections, 5s duration). Full methodology and raw data available in the <a href="/blog/neutron-vs-nextjs-benchmarks-2026">benchmark blog post</a>. We encourage you to run the benchmarks on your own hardware.
              </p>
            </div>
          </div>
        </section>

        {/* BENCHMARKS */}
        <section class="benchmarks">
          <div class="container">
            <h2 class="section-label" data-animate>Client Bundle Sizes</h2>
            <ComparisonTable
              headers={['Metric', 'Neutron', 'Next.js', 'Remix', 'Astro', 'SvelteKit', 'Nuxt', 'SolidStart']}
              rows={[
                ['Static page JS', '0 KB', '~85 KB', '~40 KB', '0 KB', '~22 KB', '~55 KB', '~18 KB'],
                ['App page JS', '~3 KB', '~90 KB', '~45 KB', 'N/A', '~24 KB', '~60 KB', '~22 KB'],
                ['Client runtime', '~3 KB (Preact)', '~42 KB (React)', '~42 KB (React)', '0 KB', '~2 KB', '~30 KB (Vue)', '~8 KB (Solid)'],
                ['Islands', 'Yes', 'No', 'No', 'Yes', 'No', 'No', 'No'],
                ['Server runtime', 'Bun or Node.js', 'Node.js', 'Node.js', 'Node.js', 'Node.js', 'Node.js / Nitro', 'Node.js'],
              ]}
              highlightColumn={1}
              accentRgb="49, 120, 198"
            />
            <p class="benchmarks__note">Bundle sizes are approximate and based on default configurations. Neutron ships Preact (~3 KB gzipped) by default. React-compat mode available for full React ecosystem compatibility.</p>
          </div>
        </section>

        {/* ECOSYSTEM */}
        <section class="ecosystem">
          <div class="container container--narrow">
            <h2 class="section-label" data-animate>The full stack</h2>
            <p class="ecosystem__intro" data-animate style={{ "--animate-delay": "0.1s" } as any}>Neutron is growing into an ecosystem. Four products, one set of patterns.</p>
            <div class="ecosystem__grid">
              <a href="/typescript" class="ecosystem__row ecosystem__row--active" data-animate style={{ "--row-accent": "var(--accent-ts)", "--animate-delay": "0.15s" } as any}>
                <div class="ecosystem__product">
                  <span class="ecosystem__name">TypeScript</span>
                  <span class="ecosystem__status ecosystem__status--available">Available</span>
                </div>
                <p class="ecosystem__role">The flagship framework. File-based routing, loaders, actions, islands, and two rendering modes.</p>
              </a>
              <a href="/rust" class="ecosystem__row" data-animate style={{ "--row-accent": "var(--accent-rust)", "--animate-delay": "0.2s" } as any}>
                <div class="ecosystem__product">
                  <span class="ecosystem__name">Rust</span>
                  <span class="ecosystem__status ecosystem__status--available">Available</span>
                </div>
                <p class="ecosystem__role">Systems-level performance. 1,161 tests, trie router, middleware, JWT, WebSocket, SSE.</p>
              </a>
              <a href="/mojo" class="ecosystem__row" data-animate style={{ "--row-accent": "var(--accent-mojo)", "--animate-delay": "0.25s" } as any}>
                <div class="ecosystem__product">
                  <span class="ecosystem__name">Mojo</span>
                  <span class="ecosystem__status ecosystem__status--available">Available</span>
                </div>
                <p class="ecosystem__role">ML tensor library. 110+ test suites, SIMD kernels, quantized inference, autograd.</p>
              </a>
              <a href="/nucleus" class="ecosystem__row" data-animate style={{ "--row-accent": "var(--accent-nucleus)", "--animate-delay": "0.3s" } as any}>
                <div class="ecosystem__product">
                  <span class="ecosystem__name">Nucleus</span>
                  <span class="ecosystem__status ecosystem__status--available">Available</span>
                </div>
                <p class="ecosystem__role">14-in-1 database. 3,724 tests, PostgreSQL compatible, MVCC, columnar OLAP.</p>
              </a>
            </div>
          </div>
        </section>
      </main>

      {/* GET STARTED */}
      <section class="cta">
        <div class="container container--narrow cta__inner">
          <h2 data-animate>Get started</h2>
          <div data-animate style={{ "--animate-delay": "0.1s" } as any}><Terminal command="npm create neutron@latest" /></div>
          <div class="cta__steps">
            <code>cd my-app</code>
            <code>npm run dev</code>
          </div>
          <a href="/docs" class="btn btn--primary">Read the docs &rarr;</a>
        </div>
      </section>
    </>
  );
}
