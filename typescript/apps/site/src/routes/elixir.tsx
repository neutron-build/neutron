import ProductPage from "../components/ProductPage";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import CodeBlock from "../components/CodeBlock";

export function head() {
  return {
    title: "Elixir - Neutron",
    description: "BEAM-native backend framework with OTP supervision, Plug + Bandit HTTP, Phoenix-style channels, tiered ETS/Nucleus cache, and the full 14-model Nucleus client via Postgrex.",
  };
}

export default function ElixirPage() {
  return (
    <ProductPage
      title="Neutron Elixir"
      description="A BEAM-native backend framework for systems that must stay up. OTP supervision, Plug + Bandit, Phoenix-style channels, tiered ETS cache, and a typed Nucleus client for all 14 data models."
      category="language"
      status="available"
      accent="var(--accent-elixir)"
      heroAccentRgb="110, 74, 126"
      heroTagline="Let it crash. The supervisor handles the rest."
      stats={[
        { value: 'OTP', label: 'Supervision Trees' },
        { value: 'Bandit', label: 'HTTP/1 + HTTP/2' },
        { value: 'ETS', label: 'Microsecond Cache' },
        { value: 'Hot', label: 'Code Reload' },
      ]}
    >
      <section>
        <h2>Fault tolerance isn't a library &mdash; it's the runtime.</h2>
        <p>Every other language treats a crashed request as an outage. On the BEAM it's just a restart. Neutron Elixir uses OTP supervisors, isolated processes, and message passing so a broken handler can't take the server down. Pair that with a Plug-based router, Bandit (pure-Elixir HTTP/2), a Phoenix-style channel behaviour, and a full Nucleus client &mdash; and you get a backend that stays up while you ship.</p>
      </section>

      <CodeBlock filename="lib/my_app/router.ex" annotation="Plug router + middleware + channel. Mounted under a supervisor.">
        <pre><code>{`defmodule MyApp.Router do
  use Neutron.Router

  plug Neutron.Middleware.RequestID
  plug Neutron.Middleware.Logger
  plug Neutron.Middleware.Recovery
  plug Neutron.Middleware.CORS
  plug Neutron.Middleware.RateLimit, per_ip: 100
  plug Neutron.Middleware.Auth

  get "/health", do: send_resp(conn, 200, ~s({"status":"ok"}))

  post "/messages" do
    %{"text" => text} = conn.body_params
    case MyApp.Messages.create(text) do
      {:ok, msg}     -> json(conn, 201, msg)
      {:error, errs} -> problem(conn, 422, errs)
    end
  end

  channel "room:*", MyApp.RoomChannel
end`}</code></pre>
      </CodeBlock>

      <FeatureGrid columns={3} accentRgb="110, 74, 126">
        <div class="feature-card">
          <div class="feature-card__title">OTP supervisors</div>
          <div class="feature-card__desc">Process trees with automatic restart strategies. A crashing handler takes down one process, not the server. Children restart in microseconds.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Plug + Bandit</div>
          <div class="feature-card__desc">Composable middleware on Bandit, pure-Elixir HTTP/1 and HTTP/2. Ten-layer middleware stack matching the Neutron contract.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Channels + presence</div>
          <div class="feature-card__desc">Phoenix-style channel behaviour with CRDT-based presence tracking. Fan out WebSocket messages to thousands of clients per node.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Full Nucleus client</div>
          <div class="feature-card__desc">All 14 data models through Postgrex. SQL, KV, Vector, Graph, TimeSeries, Document, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, PubSub.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Jobs + tiered cache</div>
          <div class="feature-card__desc">GenServer-backed job queue with retries. ETS L1 (microseconds) + Nucleus KV L2 tiered cache. Session storage in either tier.</div>
        </div>
        <div class="feature-card">
          <div class="feature-card__title">Cluster &amp; hot reload</div>
          <div class="feature-card__desc">Distributed Erlang for multi-node clustering. Hot code reload for zero-downtime deploys. <code>libcluster</code> discovery included.</div>
        </div>
      </FeatureGrid>

      <BenchmarkBars
        title="The BEAM advantage"
        bars={[
          { label: 'Processes', value: 'Millions of lightweight processes per node', width: 100, color: '#6E4A7E' },
          { label: 'Supervisors', value: 'Automatic restart, no downtime', width: 95, color: '#8B6699' },
          { label: 'Channels', value: 'Thousands of WebSockets per node', width: 88, color: '#A583B4' },
          { label: 'Cluster', value: 'Distributed Erlang + Phoenix.PubSub', width: 80, color: '#BFA1CD' },
          { label: 'Hot reload', value: 'Ship code without restarts', width: 72, color: '#D3BADD' },
        ]}
      />

      <section>
        <h3>What it's for</h3>
        <p>High-concurrency APIs that absolutely cannot go down. Real-time systems with tens of thousands of WebSocket connections. Multi-node clusters where nodes fail and traffic keeps flowing. Chat, notifications, presence, telemetry collection &mdash; anywhere the cost of a restart is higher than the cost of a crash.</p>

        <h3>Why the BEAM?</h3>
        <p>Because it was designed for concurrency, distribution, and fault tolerance at telecom scale. Isolated processes with preemptive scheduling, message passing instead of shared memory, supervisor hierarchies that restart the broken parts automatically. It's the only runtime where "nine nines" is a real engineering target instead of marketing.</p>

        <h3>Part of a bigger system</h3>
        <p>Run Neutron Elixir for the services that must stay up. Pair it with Neutron TypeScript on the edge, Rust for performance-critical paths, Go for microservices &mdash; all reading the same Nucleus database. Each piece at its peak, one source of truth.</p>
      </section>
    </ProductPage>
  );
}
