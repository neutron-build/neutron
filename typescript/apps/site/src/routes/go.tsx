import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Go - Neutron",
    description: "Go backend framework with native goroutines, OAuth2 and WebAuthn, pgx/v5 transport, all 14 Nucleus data models, and single-binary deploy. Built on Go 1.22+ ServeMux.",
  };
}

export default function GoPage() {
  return (
    <ProductPage
      title="Neutron Go"
      description="A Go framework for concurrent services with auth, real-time, and database already finished. Goroutine-native handlers, pgx/v5 transport, typed Nucleus client. One binary, zero runtime."
      category="language"
      status="available"
      accent="var(--accent-go)"
      heroAccentRgb="0, 173, 216"
      heroTagline="Go's concurrency, without bringing your own auth."
      stats={[
        { value: 'OAuth2', label: '+ WebAuthn Passkeys' },
        { value: 'pgx/v5', label: 'Native Transport' },
        { value: '14', label: 'Data Models' },
        { value: '1', label: 'Static Binary' },
      ]}
    >
      <section>
        <h2>Auth is already done.</h2>
        <p>The reason Go backends get abandoned halfway isn't the language &mdash; it's the hour on day three when you realize you need OAuth and there's no batteries-included option. Neutron Go ships <code>neutronauth</code> with OAuth2 for GitHub, Google, and Discord (PKCE-first), WebAuthn passkeys (P-256 ECDSA), JWT signing and verification, session handling, and CSRF. You wire a provider, register a callback, and you're done.</p>
      </section>

      <CodeBlock filename="main.go" annotation="Real OAuth + WebAuthn wiring. 20 lines.">
        <pre><code>{`package main

import (
    "github.com/neutron-dev/neutron-go/neutron"
    "github.com/neutron-dev/neutron-go/neutronauth"
    "github.com/neutron-dev/neutron-go/nucleus"
)

func main() {
    db := nucleus.Connect("postgres://localhost/app")
    auth := neutronauth.New(neutronauth.Config{
        GitHub:    neutronauth.OAuth{ClientID: env("GH_ID"), Secret: env("GH_SECRET")},
        Google:    neutronauth.OAuth{ClientID: env("GG_ID"), Secret: env("GG_SECRET")},
        WebAuthn:  neutronauth.WebAuthnConfig{RPID: "example.com", Origin: "https://example.com"},
        SessionDB: db.KV("sessions"),
    })

    app := neutron.New()
    app.Mount("/auth", auth.Routes())
    app.GET("/me", auth.Required(), func(c *neutron.Ctx) error {
        return c.JSON(200, auth.User(c))
    })
    app.Listen(":8080")
}`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="0, 173, 216">
        <div class="feature-card">
          <div class="feature-card__title">Goroutine-native handlers</div>
          <div class="feature-card__desc">Every request gets its own goroutine. 100K+ concurrent connections on one server, with Go's work-stealing scheduler doing the scheduling.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Idiomatic routing</div>
          <div class="feature-card__desc">Built on Go 1.22+ ServeMux with pattern matching. Composable groups, OpenAPI 3.1 generation, type-safe path params.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">OAuth2 + WebAuthn</div>
          <div class="feature-card__desc">Full social-auth flows with PKCE. Passkey registration and assertion. Session stores backed by Nucleus KV or in-memory.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">pgx/v5 native</div>
          <div class="feature-card__desc">The fastest Postgres driver in Go, wired straight to Nucleus. Pool lifecycle, prepared statements, context cancellation &mdash; all defaulted right.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Typed middleware</div>
          <div class="feature-card__desc">Auth, CORS, rate limiting, compression, logging, tracing. Chain at global or route level; Go's type system enforces contracts.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">gRPC + WebSocket</div>
          <div class="feature-card__desc">First-class gRPC service hosting alongside REST. WebSocket with goroutine-per-connection. Real-time without complexity.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>All 14 data models, one connection.</h2>
        <p>The <code>nucleus</code> package gives you typed accessors for every model Nucleus supports &mdash; no separate library per storage engine, no version skew.</p>

        <CodeBlock filename="recommend.go" annotation="Vector search + SQL join in one transaction.">
          <pre><code>{`func recommend(c *neutron.Ctx) error {
    var q struct {
        Prompt string \`\`json:"prompt"\`\`
        K      int    \`\`json:"k"\`\`
    }
    if err := c.Bind(&q); err != nil { return err }

    emb, err := embed(c.Context(), q.Prompt)
    if err != nil { return err }

    tx, _ := db.Begin(c.Context())
    defer tx.Rollback(c.Context())

    hits, _ := tx.Vector("articles").Search(emb).K(q.K).Do(c.Context())
    enriched, _ := tx.SQL().Query(
        "SELECT id, title, author FROM articles WHERE id = ANY($1)",
        hitIDs(hits),
    ).All(c.Context())

    tx.Commit(c.Context())
    return c.JSON(200, enriched)
}`}</code></pre>
        </CodeBlock>
      </section>

      <BenchmarkBars
        title="What's in the box"
        bars={[
          { label: 'HTTP', value: 'Go 1.22 ServeMux + typed middleware', width: 100, color: '#00ADD8' },
          { label: 'Auth', value: 'OAuth2 + WebAuthn + JWT + CSRF', width: 95, color: '#29BFE6' },
          { label: 'Nucleus', value: 'pgx/v5, all 14 models, pool-aware', width: 88, color: '#5CD1F4' },
          { label: 'gRPC', value: 'First-class alongside REST', width: 75, color: '#8FE3FF' },
          { label: 'Deploy', value: 'Single static binary, scratch image', width: 70, color: '#B5EDFF' },
        ]}
      />

      <section>
        <h2>Deploy. Done.</h2>
        <p>Compile to one static binary with <code>go build</code>. No runtime to install, no dependency tree to vendor, no <code>golang:1.21-alpine</code> base image. The binary is the artifact.</p>
        <CodeBlock filename="deploy.sh">
          <pre><code>{`# Build for Linux, copy to server, restart systemd unit.
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o app .
scp app prod:/usr/local/bin/myapp
ssh prod systemctl restart myapp`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h3>What it's for</h3>
        <p>API gateways handling millions of requests. Microservice architectures with gRPC between services. Cloud infrastructure tooling &mdash; CLIs, controllers, Kubernetes operators. Real-time services with WebSocket fan-out. Background job workers. Anywhere you want Go's concurrency story and would rather not reimplement a login screen.</p>

        <h3>Why Go for services?</h3>
        <p>Goroutines give you concurrency without callbacks or async runtimes. The type system catches errors at compile time without generics-heavy ceremony. Single-binary deployment means no runtime install, no dependency hell, no container layer audit. And it compiles in seconds.</p>

        <h3>Part of a bigger system</h3>
        <p>Neutron TypeScript on the edge. Neutron Go for the services. Neutron Rust where performance wins. All three talk to the same Nucleus database through the same contract &mdash; same wire protocol, same RFC 7807 errors, same health endpoints. Add what you need without rearchitecting.</p>
      </section>
    </ProductPage>
  );
}
