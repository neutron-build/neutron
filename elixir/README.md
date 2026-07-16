# Neutron Elixir

Fault-tolerant, distributed web framework for the BEAM — Plug + Bandit HTTP, OTP supervision, real-time channels, and a Nucleus multi-model database client. Part of the Neutron multi-language ecosystem.

## What It Is

A backend framework that leans on everything the BEAM already does well: OTP supervisors keep the HTTP server, database pool, real-time channels, and job workers isolated and self-healing. The HTTP layer is plain Plug on the [Bandit](https://hex.pm/packages/bandit) server, so it composes with the wider Plug ecosystem. Data goes through Nucleus over the PostgreSQL wire protocol (pgwire) via [Postgrex](https://hex.pm/packages/postgrex) — any Postgres client works, and when the server is Nucleus, all 14 data models light up.

## Philosophy

Light core, modular data models. SQL works out of the box through the Nucleus client. Every other model — KV, Vector, Graph, FTS, and the rest — is a dedicated module you call only when you need it, because Nucleus exposes each one as SQL functions over a single pgwire connection. Nothing you don't import runs.

## Install

Published to Hex as `neutron_ex` (the bare `neutron` name is taken). The OTP app atom stays `:neutron` and the module namespace stays `Neutron`:

```elixir
# mix.exs
def deps do
  [
    {:neutron, "~> 0.1", hex: :neutron_ex}
  ]
end
```

Requires Elixir `~> 1.15`.

## Quick Start

Define a router with the macro DSL, then start the server as a child of your supervision tree:

```elixir
defmodule MyApp.Router do
  use Neutron.Router

  get "/" do
    json(conn, 200, %{message: "Hello, Neutron!"})
  end

  get "/users/:id" do
    json(conn, 200, %{id: conn.path_params["id"]})
  end

  post "/users" do
    {:ok, body} = read_json(conn)
    json(conn, 201, %{created: body["name"]})
  end

  scope "/api/v1" do
    get "/items" do
      json(conn, 200, %{items: []})
    end
  end
end

# In your application supervisor:
children = [
  {Neutron, router: MyApp.Router, port: 4000}
]
Supervisor.start_link(children, strategy: :one_for_one)
```

Router helpers available inside route blocks: `json/3`, `text/3`, `read_json/1`, `read_json!/1`, `send_error/2`, `path_param/2`, `query_param/3`, `get_header/3`. Routes support `get`, `post`, `put`, `patch`, `delete`, `options`, plus `scope/2` for prefix grouping. Unmatched routes return an RFC 7807 404 automatically.

### Handlers

For logic separated from routing, implement the `Neutron.Handler` behaviour. `handle/2` receives the conn and a merged param map (path, query, and JSON body) and returns `{:ok, response}` or `{:error, reason}`:

```elixir
defmodule MyApp.Handlers.GetUser do
  use Neutron.Handler

  @impl true
  def handle(_conn, params) do
    case MyApp.Users.get(params["id"]) do
      {:ok, user} -> {:ok, %{status: 200, body: user}}
      {:error, :not_found} -> {:error, Neutron.Error.not_found("User not found")}
    end
  end
end

# In the router: get "/users/:id", do: Neutron.Handler.call(conn, MyApp.Handlers.GetUser)
```

## Middleware

Starting a server assembles the standard Framework Contract pipeline as a `Plug.Builder` stack. Wired layers: **Request ID** (`X-Request-Id`), **structured logging** with per-request timing, **exception recovery** (returns RFC 7807 500s), **CORS** (configurable origins), **rate limiting** (ETS sliding window per IP), **request timeout** (30s default, kills the handler), and **OpenTelemetry** `:telemetry` spans. Authentication is opt-in — add `Neutron.Auth.Plug` where you need it.

## Errors — RFC 7807

`Neutron.Error` builds Problem Details responses sent as `application/problem+json`. Constructors: `bad_request/1`, `unauthorized/1`, `forbidden/1`, `not_found/1`, `conflict/1`, `validation/2`, `rate_limited/1`, `internal/1`, and `nucleus_required/1` (501 when a Nucleus-only feature is used against plain PostgreSQL).

```elixir
Neutron.Error.not_found("User 42 not found")
|> Neutron.Error.send_error(conn)
```

## Health Check

`GET /health` returns Nucleus detection per the Framework Contract:

```json
{ "status": "ok", "nucleus": true, "version": "0.1.0" }
```

Add it with `forward "/health", to: Neutron.Health` or as a plug (`plug Neutron.Health.Plug`).

## Nucleus — Database Client

`Nucleus.Client` runs a Postgrex pool and detects capabilities on connect via `SELECT VERSION()`. It is started automatically when `NEUTRON_DATABASE_URL` is set. SQL runs directly; the other 13 models are dedicated modules that guard themselves against plain PostgreSQL:

```elixir
# Raw SQL — always available
{:ok, result} = Nucleus.Client.query(Nucleus.Client, "SELECT * FROM users WHERE id = $1", [42])

# Feature detection
Nucleus.Client.is_nucleus?(Nucleus.Client)   # => true / false

# KV (Redis-compatible: base, lists, hashes, sets, sorted sets, HyperLogLog)
Nucleus.Models.KV.set(Nucleus.Client, "session:abc", "data", ttl: 3600)
{:ok, "data"} = Nucleus.Models.KV.get(Nucleus.Client, "session:abc")

# Vector search
Nucleus.Models.Vector.create_collection(Nucleus.Client, "embeddings", 384, :cosine)
Nucleus.Models.Vector.insert(Nucleus.Client, "embeddings", "doc1", vec, %{title: "Hello"})
{:ok, results} = Nucleus.Models.Vector.search(Nucleus.Client, "embeddings", query, limit: 10)
```

All 14 models are reachable: **SQL** directly via the client, plus dedicated modules for **KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, Columnar, Datalog, CDC, and PubSub**. Non-SQL calls return a `nucleus_required` error when connected to plain PostgreSQL. Schema changes run through `Nucleus.Migration` with `up/1` / `down/1` callbacks and an applied-migrations table.

## Real-Time — Channels & Presence

`Neutron.Realtime.Channel` is a Phoenix Channel-style API. Each topic subscription is a GenServer registered in a `Registry`, so a crashing channel never takes down its neighbors:

```elixir
defmodule MyApp.RoomChannel do
  use Neutron.Realtime.Channel

  @impl true
  def join("room:" <> room_id, _params, socket) do
    {:ok, assign(socket, :room_id, room_id)}
  end

  @impl true
  def handle_in("new_message", payload, socket) do
    broadcast(socket, "new_message", payload)
    {:noreply, socket}
  end
end
```

Wire it to a WebSocket route with `Neutron.Realtime.Socket` (built on the `WebSock` behaviour, wildcard topic matching like `"room:*"`). `Neutron.Realtime.Presence` tracks who is in each topic with `track/3`, `untrack/2`, `update/3`, `list/1`, and `count/1` — presences are process-monitored and swept on a heartbeat, and joins/leaves broadcast a `presence_diff`.

## Background Jobs

`Neutron.Jobs.Queue` is an Oban-style queue. Workers implement `Neutron.Jobs.Worker`; jobs run in supervised tasks with retries (exponential backoff), priorities, and scheduling, and are persisted to Nucleus KV when a client is connected:

```elixir
defmodule MyApp.Workers.SendEmail do
  use Neutron.Jobs.Worker
  @impl true
  def perform(%{"to" => to}), do: MyApp.Mailer.send(to)
end

Neutron.Jobs.Queue.enqueue(MyApp.Workers.SendEmail, %{"to" => "a@b.com"}, schedule_in: 300)
```

## Caching, Auth & Validation

- **`Neutron.Cache`** — tiered cache: ETS L1 (microsecond reads) backfilled from Nucleus KV L2. `get/1`, `put/3`, `delete/1`, `fetch/3` (get-or-compute), with lazy TTL expiry and a background sweep.
- **`Neutron.Auth.JWT`** — HS256 sign/verify/peek/ttl via JOSE, keyed by `NEUTRON_SECRET_KEY`. **`Neutron.Auth.Plug`** guards routes (`:bearer` or `:session`, `required: false` for optional auth) and exposes `require_auth/2` and `require_role/2`.
- **`Neutron.Validate`** — an Ecto.Changeset-inspired pipeline with no schemas: `new/2`, `required/2`, `format/3`, `length/3`, `number/3`, `inclusion/3`, `custom/4`, then `run/1`, producing RFC 7807 field errors.
- **`Neutron.OpenAPI`** — generates an OpenAPI 3.1 spec from your router routes.

## Configuration

Configuration loads from `NEUTRON_`-prefixed environment variables (`Neutron.Config`):

| Variable | Description | Default |
|---|---|---|
| `NEUTRON_HOST` | Bind address | `0.0.0.0` |
| `NEUTRON_PORT` | Listen port | `4000` |
| `NEUTRON_DATABASE_URL` | PostgreSQL/Nucleus URL | `nil` |
| `NEUTRON_LOG_LEVEL` | Log level | `info` |
| `NEUTRON_LOG_FORMAT` | `json` or `text` | `json` |
| `NEUTRON_SECRET_KEY` | JWT/session secret | generated |
| `NEUTRON_CORS_ORIGINS` | Comma-separated origins | `*` |
| `NEUTRON_RATE_LIMIT_RPM` | Requests per minute per IP | `100` |
| `NEUTRON_REQUEST_TIMEOUT_MS` | Request deadline | `30000` |
| `NEUTRON_SHUTDOWN_TIMEOUT` | Graceful shutdown ms | `30000` |

## Supervision Tree

`Neutron.App` boots the ETS table manager, tiered cache, session sweeper, real-time channel registry and supervisor, the job supervisor, and — when a database URL is set — the Nucleus client, all under a `:one_for_one` strategy.

## Testing

```bash
mix deps.get
mix test
```

The suite ships 481 tests across the framework, Nucleus client, all 14 model modules, real-time channels/presence/socket, auth, jobs, cache, middleware, and validation.

## License

MIT.
