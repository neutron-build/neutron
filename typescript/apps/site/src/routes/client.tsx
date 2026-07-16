import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Nucleus Client - Neutron",
    description: "Typed client for every Nucleus data model, in every SDK language. Same surface in TypeScript, Rust, Go, Python, Elixir, Zig, and Julia &mdash; idiomatic in each.",
  };
}

export default function ClientPage() {
  return (
    <ProductPage
      title="Nucleus Client"
      description="The typed database client that lives in every Neutron SDK. All 14 data models through one API &mdash; SQL, KV, Vector, Graph, Time-series, and eight more &mdash; idiomatic in whichever language you reach for."
      category="tool"
      status="available"
      accent="var(--accent-client)"
      heroAccentRgb="6, 182, 212"
      heroTagline="All 14 models. Every language. One API shape."
      stats={[
        { value: '14', label: 'Data Models' },
        { value: '7', label: 'Language Clients' },
        { value: 'pgwire', label: 'Over Postgres Protocol' },
        { value: 'ACID', label: 'Multi-Model Tx' },
      ]}
    >
      <section>
        <h2>One mental model, seven runtimes.</h2>
        <p>Nucleus speaks the PostgreSQL wire protocol, which means you can connect with any Postgres client ever written. The Nucleus Client goes further: a hand-crafted, typed surface for each SDK language that gives you accessors for every non-SQL model too &mdash; <code>client.kv()</code>, <code>client.vector()</code>, <code>client.graph()</code>, <code>client.stream()</code>, <code>client.cdc()</code> &mdash; idiomatic to the language you're in.</p>
      </section>

      <CodeBlock filename="TypeScript">
        <pre><code>{`import { nucleus } from "@neutron-build/data";
const db = nucleus(process.env.DATABASE_URL!);

const hits = await db.vector("docs").search(embedding).k(10).execute();
await db.kv("sessions").set(sid, user, { ttl: 3600 });
await db.graph().shortestPath(a, b).limit(5).execute();`}</code></pre>
      </CodeBlock>

      <CodeBlock filename="Rust">
        <pre><code>{`use neutron_nucleus::NucleusClient;
let db = NucleusClient::connect(&env::var("DATABASE_URL")?).await?;

let hits = db.vector().search("docs", &embedding).k(10).execute().await?;
db.kv().set("sessions", &sid, &user, Some(3600)).await?;
db.graph().shortest_path(a, b).limit(5).execute().await?;`}</code></pre>
      </CodeBlock>

      <CodeBlock filename="Go">
        <pre><code>{`import "github.com/neutron-dev/neutron-go/nucleus"
db := nucleus.Connect(os.Getenv("DATABASE_URL"))

hits, _ := db.Vector("docs").Search(emb).K(10).Do(ctx)
db.KV("sessions").Set(ctx, sid, user, nucleus.TTL(3600))
db.Graph().ShortestPath(a, b).Limit(5).Do(ctx)`}</code></pre>
      </CodeBlock>

      <CodeBlock filename="Python">
        <pre><code>{`from neutron_py import nucleus
db = await nucleus.connect(os.environ["DATABASE_URL"])

hits = await db.vector("docs").search(embedding).k(10).execute()
await db.kv("sessions").set(sid, user, ttl=3600)
await db.graph().shortest_path(a, b).limit(5).execute()`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="6, 182, 212">
        <div class="feature-card">
          <div class="feature-card__title">Typed per language</div>
          <div class="feature-card__desc">Full type inference in TypeScript and Rust. Generics in Go 1.22+. Pydantic models in Python. Comptime shape checks in Zig. Parametric types in Julia.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">All 14 models</div>
          <div class="feature-card__desc">SQL, Columnar, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, Datalog, CDC, PubSub &mdash; one connection, one client.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Multi-model transactions</div>
          <div class="feature-card__desc">ACID across models. Insert a SQL row, store its embedding, append a CDC event, publish to a topic &mdash; commit or rollback the whole thing.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Pool-aware</div>
          <div class="feature-card__desc">Connection pooling, prepared statements, context cancellation &mdash; handled by the client. Your code doesn't open or close sockets.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Just Postgres if you want</div>
          <div class="feature-card__desc">Any PostgreSQL client (psql, pgx, node-postgres, psycopg) works. The typed clients are better, but the wire protocol is the contract.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">OpenTelemetry</div>
          <div class="feature-card__desc">Traces propagate automatically. Each query is a span with parameter count, model, rows returned, duration. Works with any OTel collector.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Multi-model atomically.</h2>
        <p>This is the thing a row-only ORM can't do. Start a transaction, touch four different storage engines inside it, commit &mdash; or roll it all back. No two-phase-commit dance between separate services.</p>

        <CodeBlock filename="Rust">
          <pre><code>{`let mut tx = db.begin().await?;

let id: i64 = tx.sql()
    .query_one("INSERT INTO articles (title) VALUES ($1) RETURNING id", &[&title])
    .await?;

tx.vector().insert("articles", id, &embed(&body)).await?;
tx.fts().index("articles", id, &body).await?;
tx.cdc().emit("articles.created", &json!({ "id": id })).await?;

tx.commit().await?; // all four writes land, or none`}</code></pre>
        </CodeBlock>
      </section>

      <section>
        <h3>What it's for</h3>
        <p>Any app that reads or writes to Nucleus. In practice that's every Neutron app, because the SDKs are designed around this client. You can also pull it in standalone &mdash; drop <code>@neutron-build/data</code> into a non-Neutron Node app and get typed access to a Nucleus cluster without buying into the framework.</p>

        <h3>Client vs. ORM</h3>
        <p>The Client gives you typed accessors and query builders &mdash; you're still writing queries by hand, just safely. The <a href="/orm">ORM</a> (planned) sits on top and generates schema types, handles migrations, and lets you join across models with a fluent surface. Use the Client today; drop in the ORM when it ships.</p>

        <h3>Part of a bigger system</h3>
        <p>The Client is Nucleus's front door. Every Neutron SDK wraps it. Every MCP tool in Studio speaks through it. Every migration the CLI runs lands through it. If you're touching a Neutron app, you're touching this API.</p>
      </section>
    </ProductPage>
  );
}
