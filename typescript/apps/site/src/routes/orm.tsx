import FeatureGrid from "../components/FeatureGrid";
import SectionBreak from "../components/SectionBreak";

export default function OrmPage() {
  return (
    <>
      <main id="main-content" class="orm-page">
        {/* HERO */}
        <header class="orm-hero">
          <div class="orm-hero__glow" aria-hidden="true"></div>
          <div class="container orm-hero__inner">
            <div class="orm-hero__badge" data-animate>ORM — Planned</div>
            <h1 class="orm-hero__title" data-animate style={{ "--animate-delay": "0.1s" } as any}>
              Type-safe queries for all 14 data models.
            </h1>
            <p class="orm-hero__desc" data-animate style={{ "--animate-delay": "0.15s" } as any}>
              Auto-generated types from schema, multi-model joins, and migration management. One ORM for SQL, vectors, graphs, timeseries, documents, and more.
            </p>
            <div class="orm-hero__pills" data-animate style={{ "--animate-delay": "0.2s" } as any}>
              <span class="orm-pill">Multi-Model</span>
              <span class="orm-pill">Type-Safe</span>
              <span class="orm-pill">6+ Languages</span>
              <span class="orm-pill">ACID</span>
              <span class="orm-pill">14 Data Models</span>
            </div>
          </div>
        </header>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* CORE FEATURES */}
        <section class="orm-section">
          <div class="container">
            <h2 class="orm-section__title" data-animate>Core Features</h2>
          </div>
          <FeatureGrid columns={2} accentRgb="16, 185, 129">
            <div class="feature-card">
              <div class="feature-card__title">Multi-Model Queries</div>
              <div class="feature-card__desc">Join SQL users with vector embeddings and graph relationships in one fluent query.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Type Generation</div>
              <div class="feature-card__desc">Auto-generate types from schema for TypeScript and Rust. Full compile-time safety.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Migration System</div>
              <div class="feature-card__desc">Version-controlled schema changes with rollback support across all 14 data models.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">ACID Transactions</div>
              <div class="feature-card__desc">Multi-model transactions with full rollback. All operations succeed together or fail together.</div>
            </div>
          </FeatureGrid>
        </section>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* MULTI-MODEL POWER */}
        <section class="orm-section orm-section--highlight">
          <div class="container container--code">
            <h2 class="orm-section__title" data-animate>Multi-Model Power</h2>
            <div class="orm-prose" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              <p>Search vector embeddings, join with SQL tables, and traverse graph relationships — all in a single fluent query. The ORM routes each part to the optimal storage engine automatically.</p>
              <p>Insert a SQL row, store its vector embedding, and create graph relationships in one atomic transaction. If any step fails, everything rolls back. No partial states, no data inconsistency across models.</p>
            </div>
          </div>
        </section>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* LANGUAGE SUPPORT */}
        <section class="orm-section">
          <div class="container">
            <h2 class="orm-section__title" data-animate>Language Support</h2>
            <p class="orm-section__desc" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              Native implementations for every supported language, each designed to feel idiomatic.
            </p>
          </div>
          <div class="lang-grid container" data-animate style={{ "--animate-delay": "0.1s" } as any}>
            <div class="lang-card" style={{ "--lc-color": "#3178C6" } as any}>
              <div class="lang-card__name">TypeScript</div>
              <div class="lang-card__detail">Full type inference with Zod schemas</div>
            </div>
            <div class="lang-card" style={{ "--lc-color": "#FF6B35" } as any}>
              <div class="lang-card__name">Rust</div>
              <div class="lang-card__detail">Compile-time type guarantees</div>
            </div>
            <div class="lang-card" style={{ "--lc-color": "#A855F7" } as any}>
              <div class="lang-card__name">Mojo</div>
              <div class="lang-card__detail">Native tensor data model support</div>
            </div>
            <div class="lang-card" style={{ "--lc-color": "#00ADD8" } as any}>
              <div class="lang-card__name">Go</div>
              <div class="lang-card__detail">pgx/v5 transport, goroutine-safe</div>
            </div>
            <div class="lang-card" style={{ "--lc-color": "#3776AB" } as any}>
              <div class="lang-card__name">Python</div>
              <div class="lang-card__detail">Async with asyncpg, Pydantic models</div>
            </div>
            <div class="lang-card" style={{ "--lc-color": "#F7A41D" } as any}>
              <div class="lang-card__name">Zig</div>
              <div class="lang-card__detail">Low-level embedded use cases</div>
            </div>
            <div class="lang-card" style={{ "--lc-color": "#9558B2" } as any}>
              <div class="lang-card__name">Julia</div>
              <div class="lang-card__detail">Scientific computing, GPU integration</div>
            </div>
          </div>
        </section>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* MIGRATION SYSTEM */}
        <section class="orm-section">
          <div class="container container--code">
            <h2 class="orm-section__title" data-animate>Migration System</h2>
            <div class="orm-prose" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              <p>Version-controlled schema changes across all 14 data models. The ORM detects diffs between your schema and database, generates type-safe migration files (not raw SQL), and applies them with zero-downtime online schema changes.</p>
              <p>Every migration includes a tested rollback function. Git-friendly files with conflict detection when multiple developers change schema simultaneously.</p>
            </div>
          </div>
        </section>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* CLI */}
        <section class="orm-section">
          <div class="container container--code">
            <h2 class="orm-section__title" data-animate>CLI Commands</h2>
            <div class="cli-commands" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              <div class="cli-row"><code>nucleus migrate generate &lt;name&gt;</code><span>Create migration from schema diff</span></div>
              <div class="cli-row"><code>nucleus migrate up</code><span>Apply pending migrations</span></div>
              <div class="cli-row"><code>nucleus migrate down</code><span>Rollback last migration</span></div>
              <div class="cli-row"><code>nucleus migrate status</code><span>Show applied/pending migrations</span></div>
              <div class="cli-row"><code>nucleus migrate validate</code><span>Check migrations for errors</span></div>
              <div class="cli-row"><code>nucleus migrate reset</code><span>Rollback all migrations (dev only)</span></div>
            </div>
          </div>
        </section>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* CTA */}
        <section class="orm-cta">
          <div class="container container--narrow">
            <h2 class="orm-cta__title" data-animate>Get Started</h2>
            <div class="orm-cta__terminal" data-animate style={{ "--animate-delay": "0.1s" } as any}>
              <div class="terminal-bar">
                <span class="terminal-dot"></span>
                <span class="terminal-dot"></span>
                <span class="terminal-dot"></span>
              </div>
              <pre class="terminal-body"><code><span class="terminal-prompt">#</span> Coming soon
<span class="terminal-prompt">#</span> Follow development at github.com/tystack</code></pre>
            </div>
          </div>
        </section>
      </main>
    </>
  );
}
