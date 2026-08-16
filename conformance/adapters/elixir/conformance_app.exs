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
#
# No DATABASE_URL is set, so `Nucleus.Client` never starts and /health reports
# `"nucleus": "unconfigured"` — which §7 calls out as "not an error".
#
# Listen port comes from PORT (HOST optional), so the runner can pin an
# ephemeral port.
#
#     PORT=8085 elixir conformance_app.exs

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

port = String.to_integer(System.get_env("PORT") || "8085")
host = System.get_env("HOST") || "127.0.0.1"

{:ok, _} =
  Supervisor.start_link(
    [{Neutron, router: ConformanceRouter, port: port, host: host}],
    strategy: :one_for_one
  )

# `elixir file.exs` exits when the script ends; the server is a child of a
# supervisor in this process, so block forever and let the runner kill us.
Process.sleep(:infinity)
