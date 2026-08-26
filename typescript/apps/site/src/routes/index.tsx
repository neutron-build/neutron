import Terminal from "../components/Terminal";
import NeutronAtom from "../components/NeutronAtom";

export function head() {
  return {
    title: "Neutron",
    description: "Build anything, no ceiling. TypeScript, Rust, Go, Python, Elixir, Mojo, Zig, and Julia — each at its peak. One database with 14 data models. Web, mobile, and desktop.",
    canonical: "https://neutron.build/",
    jsonLd: {
      "@context": "https://schema.org",
      "@type": "SoftwareApplication",
      name: "Neutron",
      applicationCategory: "DeveloperApplication",
      operatingSystem: "Cross-platform",
      description:
        "A multi-language full-stack framework ecosystem backed by Nucleus, a multi-model database engine.",
      url: "https://neutron.build",
      offers: { "@type": "Offer", price: "0", priceCurrency: "USD" },
      license: "https://opensource.org/licenses/MIT",
    },
  } as any;
}

export default function HomePage() {
  return (
    <main id="main-content">
        {/* HERO */}
        <section class="hero">
          <div class="hero__glow hero__glow--ts"></div>
          <div class="hero__glow hero__glow--rust"></div>
          <div class="hero__glow hero__glow--nucleus"></div>
          <div class="hero__glow hero__glow--mojo"></div>
          <div class="container hero__content">
            <div class="hero__text">
              <p class="hero__eyebrow" data-animate>
                The full-stack framework ecosystem
              </p>
              <h1
                class="hero__title"
                data-animate
                style="--animate-delay: 0.1s"
              >
                Build anything.
                <br />
                No ceiling.
              </h1>
              <p
                class="hero__sub"
                data-animate
                style="--animate-delay: 0.2s"
              >
                One system where every piece is built to its full potential. TypeScript for the web. Rust for raw performance. Go for services. Python for data. Mojo for ML. A database that handles any data model you throw at it. Web, mobile, and desktop from the same codebase.
              </p>
              <div
                class="hero__actions"
                data-animate
                style="--animate-delay: 0.3s"
              >
                <a href="/docs" class="btn btn--primary btn--lg">
                  Get started &rarr;
                </a>
                <a href="/nucleus" class="btn btn--ghost btn--lg">
                  Explore Nucleus
                </a>
              </div>
              <div
                class="hero__install"
                data-animate
                style="--animate-delay: 0.4s"
              >
                <Terminal command="npm create neutron@latest" />
              </div>
            </div>
            <NeutronAtom />
          </div>
        </section>

        {/* STATS */}
        <section class="stats">
          <div class="container">
            <div class="stats__grid">
              <div class="stats__item" data-animate>
                <span class="stats__number">8</span>
                <span class="stats__label">Languages</span>
              </div>
              <div
                class="stats__item"
                data-animate
                style="--animate-delay: 0.1s"
              >
                <span class="stats__number">14</span>
                <span class="stats__label">Data models</span>
              </div>
              <div
                class="stats__item"
                data-animate
                style="--animate-delay: 0.2s"
              >
                <span class="stats__number">3</span>
                <span class="stats__label">Platforms</span>
              </div>
              <div
                class="stats__item"
                data-animate
                style="--animate-delay: 0.3s"
              >
                <span class="stats__number">5,173</span>
                <span class="stats__label">Declared tests</span>
              </div>
            </div>
          </div>
        </section>

        {/* VISION */}
        <section class="vision">
          <div class="container">
            <div class="vision__intro">
              <h2 class="section-label" data-animate>
                Why Neutron
              </h2>
              <h3
                class="vision__headline"
                data-animate
                style="--animate-delay: 0.1s"
              >
                Every piece at its
                <br />
                full potential.
              </h3>
              <p
                class="vision__lead"
                data-animate
                style="--animate-delay: 0.15s"
              >
                Most projects hit walls. Your web framework can't do native mobile. Your database doesn't support vector search. Your ML pipeline lives in a completely separate world. Neutron is one integrated system where each component is purpose-built for its domain — and they all compose together through a shared database and clean integration points.
              </p>
            </div>
            <div class="vision__grid">
              <div
                class="vision__card"
                data-animate
                style="--animate-delay: 0.2s"
              >
                <h4>The right language for the job</h4>
                <p>
                  TypeScript for UI. Rust for performance-critical backends. Go for concurrent services. Python for data pipelines. Mojo for ML inference. Zig for embedded. Each language used where it excels — not forced into the same mold.
                </p>
              </div>
              <div
                class="vision__card"
                data-animate
                style="--animate-delay: 0.25s"
              >
                <h4>One database, every model</h4>
                <p>
                  Stop running Postgres, Redis, Elasticsearch, Neo4j, and
                  TimescaleDB as separate services. Nucleus is a single
                  database with 14 specialized data models — SQL, vector,
                  graph, timeseries, document, KV, and more.
                </p>
              </div>
              <div
                class="vision__card"
                data-animate
                style="--animate-delay: 0.3s"
              >
                <h4>Web, mobile, and desktop</h4>
                <p>
                  Build for the web with SSR and islands. Ship native mobile
                  apps with the same components. Create lightweight desktop
                  apps without bundling Chromium. One codebase across all
                  three platforms.
                </p>
              </div>
              <div
                class="vision__card"
                data-animate
                style="--animate-delay: 0.35s"
              >
                <h4>No walls, no rewrites</h4>
                <p>
                  Start with a TypeScript web app. Add a Rust API when you need throughput. Plug in Python for an ML feature. Need graph queries? Nucleus already has them. Your system grows with your ambitions — you never hit a wall where you need to start over.
                </p>
              </div>
            </div>
          </div>
        </section>

        {/* PRODUCTS */}
        <section class="products">
          <div class="container">
            <h2 class="section-label" data-animate>
              The ecosystem
            </h2>
            <div class="products__grid products__grid--main">
              <a
                href="/typescript"
                class="products__card products__card--ts"
                data-animate
                style="--animate-delay: 0.1s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">TypeScript</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    The flagship framework. File-based routing, loaders,
                    actions, islands, SSR. 3 KB Preact runtime. Zero JS on static routes.
                  </p>
                  <span class="products__link">Get started &rarr;</span>
                </div>
              </a>
              <a
                href="/rust"
                class="products__card products__card--rust"
                data-animate
                style="--animate-delay: 0.15s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Rust</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    High-performance backend. Trie router, middleware, JWT, WebSocket, SSE. 1,233 tests across 19 crates. Also powers Desktop via Tauri.
                  </p>
                  <span class="products__link">Learn more &rarr;</span>
                </div>
              </a>
              <a
                href="/mojo"
                class="products__card products__card--mojo"
                data-animate
                style="--animate-delay: 0.2s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Mojo</span>
                    <span class="products__badge">
                      Preview
                    </span>
                  </div>
                  <p class="products__desc">
                    ML tensor library. SIMD kernels, 8 quant formats,
                    inference pipeline, training stack. Preview, on Mojo 1.0.
                  </p>
                  <span class="products__link">Get started &rarr;</span>
                </div>
              </a>
              <a
                href="/nucleus"
                class="products__card products__card--nucleus"
                data-animate
                style="--animate-delay: 0.25s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Nucleus</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    14-in-1 database. SQL, Vector, Graph, Timeseries,
                    Document, KV, FTS, Geo, Blob, Streams, and more.
                    PostgreSQL wire compatible.
                  </p>
                  <span class="products__link">Get started &rarr;</span>
                </div>
              </a>
            </div>
            <div class="products__planned">
              <a
                href="/go"
                class="products__card products__card--go"
                data-animate
                style="--animate-delay: 0.3s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Go</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    Concurrent services, microservices, cloud infra.
                    Goroutine-native handlers, single binary deploy.
                  </p>
                  <span class="products__link">Learn more &rarr;</span>
                </div>
              </a>
              <a
                href="/python"
                class="products__card products__card--python"
                data-animate
                style="--animate-delay: 0.35s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Python</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    Full-stack web and data framework. Async-first, typed
                    loaders, Nucleus native, Mojo interop.
                  </p>
                  <span class="products__link">Learn more &rarr;</span>
                </div>
              </a>
              <a
                href="/zig"
                class="products__card products__card--zig"
                data-animate
                style="--animate-delay: 0.4s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Zig</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    Systems and embedded framework. Fixed-buffer APIs,
                    comptime typing, layer-wise build.
                  </p>
                  <span class="products__link">Learn more &rarr;</span>
                </div>
              </a>
              <a
                href="/elixir"
                class="products__card products__card--elixir"
                data-animate
                style="--animate-delay: 0.42s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Elixir</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    BEAM-native backend with OTP supervisors, Plug + Bandit,
                    channels, and full Nucleus client.
                  </p>
                  <span class="products__link">Learn more &rarr;</span>
                </div>
              </a>
              <a
                href="/julia"
                class="products__card products__card--julia"
                data-animate
                style="--animate-delay: 0.44s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Julia</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    Scientific computing with DifferentialEquations.jl,
                    ModelingToolkit, CUDA, and FMI interop.
                  </p>
                  <span class="products__link">Learn more &rarr;</span>
                </div>
              </a>
            </div>
            <div class="products__secondary">
              <a
                href="/client"
                class="products__card products__card--client"
                data-animate
                style="--animate-delay: 0.45s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Client</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    Type-safe queries for all 14 models. Schema-in-code ORM planned.
                  </p>
                </div>
              </a>
              <a
                href="/studio"
                class="products__card products__card--studio"
                data-animate
                style="--animate-delay: 0.5s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Studio</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    Visual database management.
                  </p>
                </div>
              </a>
              <a
                href="/ai"
                class="products__card products__card--ai"
                data-animate
                style="--animate-delay: 0.52s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">AI</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    Model calls, streaming, structured output, tools. Any provider.
                  </p>
                </div>
              </a>
              <a
                href="/agents"
                class="products__card products__card--agents"
                data-animate
                style="--animate-delay: 0.54s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Agents</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    File-based durable agents. Plan, act, survive restarts.
                  </p>
                </div>
              </a>
              <a
                href="/workflow"
                class="products__card products__card--workflow"
                data-animate
                style="--animate-delay: 0.56s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">Workflow</span>
                    <span class="products__badge products__badge--available">
                      Available
                    </span>
                  </div>
                  <p class="products__desc">
                    Durable event-sourced execution. Suspend for days, resume exactly.
                  </p>
                </div>
              </a>
              <div
                class="products__card products__card--platform"
                data-animate
                style="--animate-delay: 0.55s"
              >
                <div class="products__inner">
                  <div class="products__header">
                    <span class="products__name">
                      Web / Mobile / Desktop
                    </span>
                  </div>
                  <p class="products__desc">SSR, native views, Tauri.</p>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* TRACTION */}
        <section class="traction">
          <div class="container">
            <h2 class="section-label" data-animate>
              How it works
            </h2>
            <h3
              class="traction__headline"
              data-animate
              style="--animate-delay: 0.1s"
            >
              Start simple. Scale to anything.
            </h3>
            <div class="traction__grid">
              <div
                class="traction__item"
                data-animate
                style="--animate-delay: 0.15s"
              >
                <span class="traction__number">1</span>
                <span class="traction__label">
                  Pick your starting point
                </span>
                <span class="traction__detail">
                  A TypeScript web app. A Rust API. A Go microservice. Start with whatever fits your first problem. No upfront commitment to the full stack.
                </span>
              </div>
              <div
                class="traction__item"
                data-animate
                style="--animate-delay: 0.2s"
              >
                <span class="traction__number">2</span>
                <span class="traction__label">Add what you need</span>
                <span class="traction__detail">
                  Need ML inference? Add Mojo. Need a high-throughput API? Add Rust. Need vector search? It's already in your database. Each piece plugs in without rearchitecting.
                </span>
              </div>
              <div
                class="traction__item"
                data-animate
                style="--animate-delay: 0.25s"
              >
                <span class="traction__number">3</span>
                <span class="traction__label">
                  One database ties it together
                </span>
                <span class="traction__detail">
                  Nucleus gives you SQL, vector search, graph, timeseries,
                  document, KV, full-text search, and more — all through one
                  connection. Every part of your system reads from the same source of truth.
                </span>
              </div>
              <div
                class="traction__item"
                data-animate
                style="--animate-delay: 0.3s"
              >
                <span class="traction__number">4</span>
                <span class="traction__label">Deploy anywhere</span>
                <span class="traction__detail">
                  Ship to web, mobile, and desktop. Static sites, SSR,
                  native apps, lightweight desktop apps. Each deployment target is purpose-built, not a compromise.
                </span>
              </div>
            </div>
          </div>
        </section>

        {/* CTA */}
        <section class="cta">
          <div class="container container--narrow">
            <h2 class="cta__title" data-animate>
              Ready to build?
            </h2>
            <div data-animate style="--animate-delay: 0.1s">
              <Terminal command="npm create neutron@latest" />
            </div>
            <div
              class="cta__steps"
              data-animate
              style="--animate-delay: 0.15s"
            >
              <code>cd my-app</code>
              <code>npm run dev</code>
            </div>
            <div
              class="cta__buttons"
              data-animate
              style="--animate-delay: 0.2s"
            >
              <a href="/docs" class="btn btn--primary btn--lg">
                Explore the docs &rarr;
              </a>
              <a href="/nucleus" class="btn btn--ghost btn--lg">
                Learn about Nucleus
              </a>
            </div>
          </div>
        </section>
    </main>
  );
}
