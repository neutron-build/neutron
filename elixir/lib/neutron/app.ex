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

  ## Graceful shutdown (FRAMEWORK_CONTRACT §8)

  SIGTERM is graceful on a plain BEAM: `:erl_signal_server`'s default handler
  calls `:init.stop/0`, which stops applications in reverse start order.
  SIGINT is not by default (break handler, no drain) — this application
  installs `Neutron.Signals` at start to put SIGINT on the same
  `:init.stop/0` path (skipped under `mix test`, where tests install their own
  observer).

  `:init.stop/0` terminates this supervision tree in reverse order, which is
  OTP's mapping of the contract's OnStop lifecycle hooks: each child's
  `terminate/2` runs before its dependants stop — e.g.
  `Nucleus.Client.terminate/2` closes the Postgrex pool. The HTTP server
  (started separately via `Neutron.child_spec/1` in the host application's
  tree) drains in-flight requests for `NEUTRON_SHUTDOWN_TIMEOUT` ms (default
  30 000) as the tree tears down.
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    config = Neutron.Config.load()
    maybe_install_signal_handler()

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

  # Under `mix test` the suite owns the VM: installing the handler would
  # change SIGINT's disposition for the whole test run. Mix is not available
  # in releases, where the handler is always installed.
  defp maybe_install_signal_handler do
    if mix_test_env?() do
      :ok
    else
      :ok = Neutron.Signals.attach()
      :ok
    end
  end

  defp mix_test_env? do
    Code.ensure_loaded?(Mix) and function_exported?(Mix, :env, 0) and Mix.env() == :test
  end
end
