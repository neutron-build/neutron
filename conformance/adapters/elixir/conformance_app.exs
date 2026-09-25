# Canonical Neutron conformance app (Elixir SDK).
#
# Boots a Neutron Elixir (Plug + Bandit) server with NO database so the
# cross-SDK conformance runner can assert FRAMEWORK_CONTRACT.md against it.
# Mirrors the Go/Rust/Python conformance apps endpoint-for-endpoint:
#
#     GET  /health                  §7 health shape {status, nucleus, version}
#     GET  /openapi.json            §4 OpenAPI 3.1 document
#     GET  /api/items               200 list (compression / request-id probe)
#     POST /api/items               422 validation error (RFC 7807 + errors[])
#     GET  /errors/{bad-request,…}  forced standard §2 errors
#     GET  /slow                    200 after 1.5s (in-flight request for the §8 drain probe)
#
# No DATABASE_URL is set, so `Nucleus.Client` never starts and /health reports
# `"nucleus": "unconfigured"` — which §7 calls out as "not an error".
#
# The listen address is the SDK's own: `{Neutron, router: ...}` without
# `:port`/`:host` reads NEUTRON_HOST / NEUTRON_PORT via `Neutron.Config`
# (contract §6). The adapter reads no variable of its own — the runner's
# `config.env` dimension exists to catch an SDK that ignores them, and an
# adapter-specific PORT would hide exactly that.
#
#     NEUTRON_PORT=8085 elixir conformance_app.exs

# The SDK is a path dependency: this app must test the tree, not a published
# release. Mix.install compiles it once and caches by lockfile hash.
here = __DIR__
repo = Path.expand("../../..", here)

Mix.install([{:neutron, path: Path.join(repo, "elixir")}])

defmodule ConformanceRouter do
  use Neutron.Router

  alias Neutron.Error

  # `Neutron.Middleware`'s pipeline does NOT include the health or OpenAPI
  # plugs — it runs RequestId, Logger, Recovery, Cors, Plug.Head, RateLimit,
  # Timeout, OTel, Dispatch. So an app mounts them itself, which is what the
  # Health moduledoc tells you to do. Mounting the FRAMEWORK's plugs rather
  # than hand-rolling the responses is the point: the conformance run has to
  # exercise Neutron's implementation, not the adapter's.
  get "/health" do
    Neutron.Health.call(conn, [])
  end

  get "/openapi.json" do
    Neutron.OpenAPI.JsonPlug.call(conn, router: __MODULE__, version: "9.9.9")
  end

  # A body big enough and compressible enough for the gzip probe, and a plain
  # 200 for the request-id probe.
  get "/api/items" do
    items =
      Enum.map(1..50, fn i ->
        %{id: i, name: "conformance-item-#{i}", price: i * 1.0}
      end)

    json(conn, 200, items)
  end

  # §2 validation: a missing/blank name must produce RFC 7807 with `errors[]`.
  post "/api/items" do
    case read_json(conn) do
      {:ok, body} -> validate_item(conn, body)
      _ -> send_error(conn, Error.bad_request("body must be JSON"))
    end
  end

  # §8 drain probe: a request that is still in flight when SIGTERM arrives.
  get "/slow" do
    Process.sleep(1500)
    json(conn, 200, %{ok: true})
  end

  get "/errors/bad-request" do
    send_error(conn, Error.bad_request("forced bad request"))
  end

  get "/errors/unauthorized" do
    send_error(conn, Error.unauthorized("forced unauthorized"))
  end

  get "/errors/forbidden" do
    send_error(conn, Error.forbidden("forced forbidden"))
  end

  get "/errors/not-found" do
    send_error(conn, Error.not_found("forced not found"))
  end

  get "/errors/conflict" do
    send_error(conn, Error.conflict("forced conflict"))
  end

  get "/errors/rate-limited" do
    send_error(conn, Error.rate_limited("forced rate limited"))
  end

  get "/errors/internal" do
    send_error(conn, Error.internal("forced internal error"))
  end

  defp validate_item(conn, body) do
    errors =
      []
      |> check(body["name"], "name", &(is_binary(&1) and String.trim(&1) != ""), "must not be blank")
      |> check(body["price"], "price", &(is_number(&1) and &1 >= 0), "must be a number >= 0")

    if errors == [] do
      json(conn, 201, %{id: 1, name: body["name"], price: body["price"]})
    else
      send_error(conn, Error.validation("Request body failed validation", errors))
    end
  end

  defp check(acc, value, field, ok?, message) do
    if ok?.(value), do: acc, else: acc ++ [%{field: field, message: message}]
  end
end

# The server runs the way the SDK documents it: `{Neutron, ...}` as a child in
# the HOST APPLICATION's supervision tree (Neutron.App moduledoc). That is
# load-bearing for §8, not ceremony. SIGTERM → `:init.stop/0`, which stops
# APPLICATIONS in reverse start order — so the tree drains only if it belongs
# to one. A bare `Supervisor.start_link` in this script (what the adapter used
# to do) is linked to the script process, which `:init.stop/0` simply kills
# after the applications are down: the in-flight request is dropped, the
# process still exits 0, and the shutdown dimension would be measuring the
# adapter instead of the SDK. A `mix release` app never runs that way.
defmodule ConformanceApp do
  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link([{Neutron, router: ConformanceRouter}], strategy: :one_for_one)
  end
end

:ok =
  :application.load(
    {:application, :conformance_app,
     [
       description: ~c"Neutron conformance app",
       vsn: ~c"0.0.0",
       modules: [ConformanceApp, ConformanceRouter],
       registered: [],
       applications: [:kernel, :stdlib, :elixir, :logger, :neutron],
       mod: {ConformanceApp, []}
     ]}
  )

{:ok, _} = Application.ensure_all_started(:conformance_app)

# `elixir file.exs` exits when the script ends, so block forever; SIGTERM
# stops the VM through `:init.stop/0`.
Process.sleep(:infinity)
