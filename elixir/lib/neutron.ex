defmodule Neutron do
  @moduledoc """
  Neutron — Fault-tolerant, distributed web framework for the BEAM.

  Part of the Neutron multi-language ecosystem. Provides:

  - Plug-based HTTP pipeline with Bandit server
  - Macro-based router DSL
  - RFC 7807 error handling
  - Nucleus multi-model database client (14 models)
  - Phoenix Channel-style real-time pub/sub
  - OTP supervision trees for fault tolerance
  - ETS-backed tiered caching
  - Oban-style job queues backed by Nucleus

  ## Quick Start

      defmodule MyApp.Router do
        use Neutron.Router

        get "/hello" do
          json(conn, 200, %{message: "Hello, Neutron!"})
        end

        get "/users/:id" do
          user_id = conn.path_params["id"]
          json(conn, 200, %{id: user_id})
        end
      end

      # In your application supervisor:
      children = [
        {Neutron, router: MyApp.Router, port: 4000}
      ]

  ## Configuration

  Set environment variables with the `NEUTRON_` prefix:

  - `NEUTRON_HOST` — bind address (default: `0.0.0.0`)
  - `NEUTRON_PORT` — listen port (default: `4000`)
  - `NEUTRON_DATABASE_URL` — PostgreSQL/Nucleus connection string
  - `NEUTRON_LOG_LEVEL` — log level (default: `info`)
  - `NEUTRON_LOG_FORMAT` — `json` or `text` (default: `json`)
  """

  @version "0.1.0"

  @doc """
  Returns the Neutron framework version.
  """
  @spec version() :: String.t()
  def version, do: @version

  @doc """
  Starts the Neutron HTTP server as a child spec for a supervisor.

  ## Options

    * `:router` — the router module (required)
    * `:port` — listen port (default: from config or 4000)
    * `:host` — bind address (default: from config or "0.0.0.0")
    * `:nucleus` — Nucleus client pid or name (optional)

  ## Example

      children = [
        {Neutron, router: MyApp.Router, port: 4000}
      ]
      Supervisor.start_link(children, strategy: :one_for_one)
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    config = Neutron.Config.load()
    port = Keyword.get(opts, :port, config.port)
    host = Keyword.get(opts, :host, config.host)
    router = Keyword.fetch!(opts, :router)

    plug =
      {Neutron.Middleware,
       router: router,
       nucleus: Keyword.get(opts, :nucleus)}

    %{
      id: __MODULE__,
      start:
        {Bandit, :start_link,
         [
           [
             plug: plug,
             port: port,
             ip: parse_host(host),
             scheme: :http
           ]
         ]},
      type: :supervisor
    }
  end

  @doc false
  defp parse_host(host) when is_binary(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, ip} -> ip
      _ -> {0, 0, 0, 0}
    end
  end

  defp parse_host(host) when is_tuple(host), do: host
end
