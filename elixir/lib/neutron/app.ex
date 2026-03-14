defmodule Neutron.App do
  @moduledoc """
  Neutron OTP Application.

  Starts the supervision tree with:
  - ETS table manager for caching and rate limiting
  - Nucleus connection pool (if DATABASE_URL configured)
  - PubSub registry for real-time channels
  - Job queue supervisor

  ## Supervision Tree

      Neutron.App (Application)
      ├── Neutron.ETS.Manager (GenServer — creates ETS tables)
      ├── Neutron.Cache (GenServer — tiered cache with sweep timer)
      ├── Neutron.Auth.SessionSweeper (GenServer — periodic session cleanup)
      ├── Nucleus.Client (GenServer — Postgrex pool + feature detection)
      ├── Neutron.Realtime.Registry (Registry — channel processes)
      ├── Neutron.Realtime.Supervisor (DynamicSupervisor — channel workers)
      ├── Neutron.Jobs.Supervisor (DynamicSupervisor — job workers)
      └── Bandit (HTTP server — added by user via child_spec)
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    config = Neutron.Config.load()

    children =
      [
        # ETS table manager — creates tables for cache, rate limiting, sessions
        Neutron.ETS.Manager,
        # Tiered cache (L1 ETS + L2 Nucleus KV) with periodic expired-entry sweep
        Neutron.Cache,
        # Session sweeper — periodically removes expired sessions from ETS
        Neutron.Auth.SessionSweeper,
        # Real-time channel registry
        {Registry, keys: :duplicate, name: Neutron.Realtime.Registry},
        # Dynamic supervisor for channel processes
        {DynamicSupervisor, name: Neutron.Realtime.Supervisor, strategy: :one_for_one},
        # Dynamic supervisor for background jobs
        {DynamicSupervisor, name: Neutron.Jobs.Supervisor, strategy: :one_for_one}
      ]
      |> maybe_add_nucleus(config)

    opts = [strategy: :one_for_one, name: Neutron.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        Logger.info("[Neutron] Application started (v#{Neutron.version()})")
        {:ok, pid}

      error ->
        error
    end
  end

  @impl true
  def stop(_state) do
    Logger.info("[Neutron] Application stopping, draining connections...")
    # Graceful shutdown — OTP handles supervisor tree teardown in reverse order
    :ok
  end

  defp maybe_add_nucleus(children, config) do
    if config.database_url do
      children ++ [{Nucleus.Client, url: config.database_url, name: Nucleus.Client}]
    else
      children
    end
  end
end
