import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";
import ComparisonTable from "../components/ComparisonTable";

export function head() {
  return {
    title: "Rust - Neutron",
    description: "Async Rust web framework on Hyper and Tokio. Trie routing, 1,210 tests across 19 composable crates, built-in JWT, WebSocket, SSE, OAuth, and WebAuthn. Nucleus integration out of the box.",
  };
}

export default function RustPage() {
  return (
    <ProductPage
      title="Neutron Rust"
      description="Async web framework on Hyper and Tokio. Trie routing, 1,210 tests across 19 composable crates, and the auth, real-time, and database layers already wired up."
      category="language"
      status="available"
      accent="var(--accent-rust)"
      heroAccentRgb="255, 107, 53"
      heroTagline="The backend that compiles into a binary you're not afraid of."
      stats={[
        { value: '1,210', label: 'Tests Passing' },
        { value: '19', label: 'Composable Crates' },
        { value: '681ns', label: 'Plaintext Response' },
        { value: '277ns', label: 'Route Lookup' },
      ]}
    >
      <section>
        <h2>Stop wiring crates together.</h2>
        <p>Most Rust web projects start with Axum or Actix plus fifteen dependencies to reach feature parity with a normal framework. Neutron Rust ships with the full stack already composed &mdash; trie router, Tower-style middleware, JWT + OAuth + WebAuthn auth, Nucleus client, WebSocket and SSE, OTel tracing, and a Stripe integration &mdash; all tested across 1,210 tests and versioned together.</p>
        <p>You import the crates you need. You don't import the ones you don't. Feature-gate everything at the Cargo level, and the binary drops features that aren't in your build.</p>
      </section>

      <FeatureGrid columns={3} accentRgb="255, 107, 53">
        <div class="feature-card">
          <div class="feature-card__title">Trie router</div>
          <div class="feature-card__desc">277 ns lookup at 500 routes. O(segments) matching, so a 10,000-route app runs at the same speed as a five-route one.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Tower middleware</div>
          <div class="feature-card__desc">Zero-allocation chain building. Compose logger, CORS, JWT, rate limit, compression, and tracing at global or per-route scope.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Typed extractors</div>
          <div class="feature-card__desc">Path, Query, Json, Form, State, Extension &mdash; all checked at compile time. No runtime casting, no macro magic you can't read.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Full auth stack</div>
          <div class="feature-card__desc">JWT (HMAC, RSA, ECDSA), OAuth2 flows for GitHub / Google / Discord with PKCE, WebAuthn passkeys, sessions, CSRF.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">WebSocket &amp; SSE</div>
          <div class="feature-card__desc">Handle 100,000+ concurrent connections on one server. Built on Tokio with zero-copy framing. PubSub fans out across nodes through Nucleus.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Nucleus native</div>
          <div class="feature-card__desc">Typed client for all 14 data models: <code>.kv()</code>, <code>.vector()</code>, <code>.graph()</code>, <code>.stream()</code>. Pool lifecycle and prepared statements handled.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Real code, real handlers.</h2>
        <p>Here's a production-shaped handler with auth, validation, and a vector query in about thirty lines:</p>

        <CodeBlock filename="src/routes/recommend.rs" annotation="Compile-time validated extractors. No runtime JSON schema check.">
          <pre><code>{`use neutron::prelude::*;
use neutron_nucleus::NucleusClient;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Query { prompt: String, k: usize }

#[derive(Serialize)]
struct Hit { id: String, score: f32, title: String }

pub async fn recommend(
    _user: Authed,                  // JWT-validated, rejects unauthenticated
    State(db): State<NucleusClient>,
    Json(q): Json<Query>,
) -> Result<Json<Vec<Hit>>, ApiError> {
    let embedding = embed(&q.prompt).await?;

    let hits = db.vector()
        .search("articles", &embedding)
        .k(q.k.min(50))
        .execute()
        .await?;

    Ok(Json(hits.into_iter()
        .map(|h| Hit { id: h.id, score: h.score, title: h.title })
        .collect()))
}`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h2>The 19 crates, composable.</h2>
        <p>Everything is its own crate. Use what you need; the rest never touches your binary.</p>

        <ComparisonTable
          headers={["Crate", "What it does", "Depends on"]}
          rows={[
            ["neutron", "Core framework: router, extractors, middleware chain", "hyper, tokio"],
            ["neutron-cli", "`neutron new`, `dev`, `build`, `migrate`", "neutron"],
            ["neutron-nucleus", "Typed client for all 14 Nucleus data models", "postgres, pgx wire"],
            ["neutron-postgres", "Plain Postgres client when you don't want Nucleus", "postgres"],
            ["neutron-oauth", "OAuth2 (GitHub, Google, Discord) with PKCE", "reqwest, jsonwebtoken"],
            ["neutron-webauthn", "Passkey registration and assertion (P-256 ECDSA)", "ring, base64"],
            ["neutron-jobs", "Background jobs with retries, cron, and dead-letter", "neutron-nucleus"],
            ["neutron-cache", "In-memory + Nucleus KV tiered cache", "neutron-nucleus"],
            ["neutron-redis", "Drop-in Redis client for teams migrating", "redis"],
            ["neutron-smtp", "Outbound email with templates and dry-run mode", "lettre"],
            ["neutron-storage", "S3-compatible object storage", "aws-sdk-s3"],
            ["neutron-stripe", "Stripe webhooks, subscriptions, checkout", "stripe-rs"],
            ["neutron-graphql", "GraphQL server with Nucleus resolvers", "async-graphql"],
            ["neutron-grpc", "gRPC service hosting alongside HTTP", "tonic, prost"],
            ["neutron-rpc", "Typed client/server RPC (internal comms)", "serde"],
            ["neutron-otel", "OpenTelemetry traces, metrics, logs", "opentelemetry"],
            ["neutron-inference", "Model inference pipeline (CPU/GPU)", "candle, tch"],
            ["neutron-config", "Layered config: env → file → CLI flags", "serde, toml"],
          ]}
        />
      </section>

      <BenchmarkBars
        title="What you're buying"
        bars={[
          { label: 'Router', value: '277ns lookup at 500 routes', width: 100, color: '#FF6B35' },
          { label: 'Auth', value: 'JWT + OAuth + WebAuthn built in', width: 92, color: '#FF8C5A' },
          { label: 'Real-time', value: 'WebSocket + SSE + PubSub', width: 85, color: '#FFA07A' },
          { label: 'Nucleus', value: 'Typed client, all 14 models', width: 80, color: '#FFB899' },
          { label: 'Ops', value: 'OTel, Stripe, jobs, cache, storage', width: 72, color: '#FFCEB3' },
          { label: 'Tests', value: '1,210 across 19 crates', width: 65, color: '#FFDDC8' },
        ]}
      />

      <section>
        <h2>Deploy a binary. That's it.</h2>
        <p>Compile once, ship once. Cross-compile to any target; the binary is self-contained. No runtime to install, no Dockerfile to maintain, no dependency tree to audit at 3am.</p>

        <CodeBlock filename="Dockerfile (optional)" annotation="If you want a container. Otherwise just SCP the binary.">
          <pre><code>{`FROM scratch
COPY ./target/release/myapp /myapp
EXPOSE 8080
ENTRYPOINT ["/myapp"]`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h3>What it's for</h3>
        <p>High-throughput APIs where latency predictability matters. Real-time fan-out to tens of thousands of clients. Edge-compiled WASM services. Tauri desktop backends (the same framework powers Neutron Desktop). Anything where you want Rust's memory safety and throughput without building half a web framework first.</p>

        <h3>Production-grade by default</h3>
        <p>1,210 tests across 19 crates &mdash; unit, integration, property-based, and async. Every crate's public API is checked by <code>cargo-semver-checks</code>. Memory safety guaranteed by the borrow checker; no segfaults, no data races, no null dereferences. Release profile with LTO + codegen-units=1 lands production binaries at ~8&ndash;12 MB.</p>

        <h3>Part of a bigger system</h3>
        <p>Use Neutron Rust for the performance-sensitive services. Use Neutron TypeScript for the web, Go for concurrent microservices, Python for ML and data pipelines. Every SDK speaks the same contract and reads the same Nucleus database. You never hit a wall where the framework says no.</p>
      </section>
    </ProductPage>
  );
}
