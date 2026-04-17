import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Python - Neutron",
    description: "Async-first Python framework on Starlette, Pydantic v2, and asyncpg. Typed loaders, Nucleus native, MCP server included. Built for AI apps and data pipelines.",
  };
}

export default function PythonPage() {
  return (
    <ProductPage
      title="Neutron Python"
      description="Starlette speed, Pydantic sanity, Nucleus for state. Async-first from the handler to the database, with MCP server support so your LLM apps ship with tools defined, not glued on."
      category="language"
      status="available"
      accent="var(--accent-python)"
      heroAccentRgb="55, 118, 171"
      heroTagline="Starlette speed. Pydantic sanity. Nucleus for state."
      stats={[
        { value: 'Async', label: 'Top to Bottom' },
        { value: 'Pydantic v2', label: 'Validation' },
        { value: 'asyncpg', label: 'Nucleus Transport' },
        { value: 'MCP', label: 'Tools Built In' },
      ]}
    >
      <section>
        <h2>The Python server built for AI apps.</h2>
        <p>Most Python frameworks are either too bare (raw ASGI) or too opinionated about being an ORM-first monolith. Neutron Python lands in the middle &mdash; Starlette for routing, Pydantic v2 for validation, asyncpg for Nucleus, and a built-in MCP server so your tools are first-class citizens. Plus 30-second graceful shutdown, CSRF, per-IP rate limiting, and the same middleware order as every other Neutron SDK.</p>
      </section>

      <CodeBlock filename="app/routes/chat.py" annotation="Pydantic validation + streaming response + MCP tool call.">
        <pre><code>{`from neutron import route, LoaderArgs, stream
from neutron.mcp import tool
from pydantic import BaseModel
import anthropic

class ChatReq(BaseModel):
    message: str
    thread_id: str | None = None

@tool("search_docs")
async def search_docs(query: str, k: int = 5) -> list[dict]:
    hits = await ctx.db.vector("docs").search(query).k(k).execute()
    return [{"id": h.id, "snippet": h.text[:200]} for h in hits]

@route.post("/chat")
async def chat(args: LoaderArgs[ChatReq]):
    async def token_stream():
        async with anthropic.AsyncClient().messages.stream(
            model="claude-opus-4-6",
            messages=[{"role": "user", "content": args.body.message}],
            tools=[search_docs.schema],
        ) as s:
            async for tok in s.text_stream:
                yield tok
    return stream(token_stream())`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="55, 118, 171">
        <div class="feature-card">
          <div class="feature-card__title">Starlette + Pydantic</div>
          <div class="feature-card__desc">Async ASGI with typed loaders and actions. Pydantic v2 validates request bodies, query params, and headers at the edge.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">asyncpg to Nucleus</div>
          <div class="feature-card__desc">The fastest Python Postgres client, wired to all 14 Nucleus data models. Connection pool and prepared statements by default.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">MCP tools built in</div>
          <div class="feature-card__desc">Decorate a function with <code>@tool</code> and it's a Model Context Protocol tool &mdash; discoverable by Claude, ChatGPT, or any MCP client.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Streaming first</div>
          <div class="feature-card__desc">Server-sent events and chunked streaming are one line. Perfect for LLM token streams, real-time data feeds, and long reports.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Graceful shutdown</div>
          <div class="feature-card__desc">30-second drain on SIGTERM. In-flight requests finish, new ones 503 with <code>Retry-After</code>. Zero dropped traffic on deploys.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Security defaults</div>
          <div class="feature-card__desc">CSRF middleware, per-IP rate limiting, CORS, OAuth2. Not a pile of middlewares you wire up &mdash; an opinionated stack that boots correct.</div>
        </div>
      </FeatureGrid>

      <section>
        <h2>Data pipelines that aren't a separate process.</h2>
        <p>You don't need to run Airflow next to your API server anymore. Typed Nucleus queries, async everywhere, streaming responses, and MCP tools live in the same app &mdash; so your LLM can call your pipeline and your pipeline can write to the same database that serves the UI.</p>

        <CodeBlock filename="app/routes/index.py" annotation="Typed loader with vector search. Edge-rendered HTML.">
          <pre><code>{`from neutron import loader, page

async def load(args):
    db = args.ctx.db
    trending = await db.sql().query(
        "SELECT id, title FROM articles "
        "ORDER BY views_24h DESC LIMIT 10"
    ).all()
    similar = await db.vector("articles").search(
        embed("python frameworks")
    ).k(5).execute()
    return {"trending": trending, "similar": similar}

@page("/")
async def home(data):
    return render("home.html", data)`}</code></pre>
        </CodeBlock>
      </section>

      <BenchmarkBars
        title="What's in the box"
        bars={[
          { label: 'HTTP', value: 'Starlette + Pydantic v2, async ASGI', width: 100, color: '#3776AB' },
          { label: 'Nucleus', value: 'asyncpg, all 14 models, pooled', width: 92, color: '#5891C8' },
          { label: 'MCP', value: 'Tools + prompts + resources built in', width: 88, color: '#7AACD9' },
          { label: 'Security', value: 'CSRF + rate limit + OAuth + CORS', width: 80, color: '#9BC7E9' },
          { label: 'Ops', value: 'Graceful shutdown, OTel, health probes', width: 72, color: '#BCDAEF' },
        ]}
      />

      <section>
        <h3>What it's for</h3>
        <p>LLM applications where your tools, prompts, and memory live in one app. Data pipelines that need a typed schema and a real database. Async APIs that don't want Django's assumptions. Anywhere you'd reach for FastAPI but also want a real batteries-included framework around it.</p>

        <h3>Why Python?</h3>
        <p>Because the AI ecosystem lives here. Because <code>asyncpg</code> hits C-like throughput against Nucleus. Because Pydantic v2 compiled in Rust is as fast as validation gets. You don't pick Python for speed; you pick it for the libraries, and then you pick a framework that doesn't waste them.</p>

        <h3>Part of a bigger system</h3>
        <p>Train a model in Neutron Mojo, expose it through Neutron Python's MCP server, consume it from Neutron TypeScript on the edge. All three backed by the same Nucleus database. One contract, many runtimes.</p>
      </section>
    </ProductPage>
  );
}
