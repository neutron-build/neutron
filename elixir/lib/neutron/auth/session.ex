defmodule Neutron.Auth.Session do
  @moduledoc """
  Session management using ETS (L1) with optional Nucleus KV (L2) backing.

  Sessions are stored in ETS for fast local access. When Nucleus is available,
  sessions are also persisted to KV for cross-node session sharing.

  ## Example

      # Create a session
      {:ok, session_id} = Neutron.Auth.Session.create(%{user_id: 42})

      # Get session data
      {:ok, data} = Neutron.Auth.Session.get(session_id)

      # Update session
      :ok = Neutron.Auth.Session.put(session_id, %{user_id: 42, last_seen: now})

      # Delete session
      :ok = Neutron.Auth.Session.delete(session_id)
  """

  @table :neutron_sessions
  @default_ttl 86_400
  @kv_prefix "neutron:session:"

  @type session_id :: String.t()
  @type session_data :: map()

  @doc """
  Creates a new session with the given data.

  Returns the generated session ID.

  ## Options

    * `:ttl` — time-to-live in seconds (default: 86400 = 24h)
  """
  @spec create(session_data(), keyword()) :: {:ok, session_id()}
  def create(data, opts \\ []) do
    session_id = generate_session_id()
    ttl = Keyword.get(opts, :ttl, @default_ttl)
    expires_at = System.system_time(:second) + ttl

    record = %{
      data: data,
      created_at: System.system_time(:second),
      expires_at: expires_at
    }

    # Write to ETS
    :ets.insert(@table, {session_id, record})

    # Write to Nucleus KV if available
    persist_to_nucleus(session_id, record, ttl)

    {:ok, session_id}
  end

  @doc """
  Retrieves session data by session ID.

  Checks ETS first (L1), then Nucleus KV (L2) if not found locally.
  """
  @spec get(session_id()) :: {:ok, session_data()} | {:error, :not_found | :expired}
  def get(session_id) do
    case :ets.lookup(@table, session_id) do
      [{^session_id, record}] ->
        if expired?(record) do
          delete(session_id)
          {:error, :expired}
        else
          {:ok, record.data}
        end

      [] ->
        # Try Nucleus KV fallback
        case load_from_nucleus(session_id) do
          {:ok, record} ->
            # Cache back to ETS
            :ets.insert(@table, {session_id, record})
            {:ok, record.data}

          error ->
            error
        end
    end
  rescue
    ArgumentError -> {:error, :not_found}
  end

  @doc """
  Updates session data.
  """
  @spec put(session_id(), session_data()) :: :ok | {:error, :not_found}
  def put(session_id, data) do
    case :ets.lookup(@table, session_id) do
      [{^session_id, record}] ->
        updated = %{record | data: data}
        :ets.insert(@table, {session_id, updated})
        remaining_ttl = max(0, record.expires_at - System.system_time(:second))
        persist_to_nucleus(session_id, updated, remaining_ttl)
        :ok

      [] ->
        {:error, :not_found}
    end
  rescue
    ArgumentError -> {:error, :not_found}
  end

  @doc """
  Deletes a session.
  """
  @spec delete(session_id()) :: :ok
  def delete(session_id) do
    :ets.delete(@table, session_id)
    delete_from_nucleus(session_id)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Touches a session, extending its TTL.
  """
  @spec touch(session_id(), non_neg_integer()) :: :ok | {:error, :not_found}
  def touch(session_id, ttl \\ @default_ttl) do
    case :ets.lookup(@table, session_id) do
      [{^session_id, record}] ->
        updated = %{record | expires_at: System.system_time(:second) + ttl}
        :ets.insert(@table, {session_id, updated})
        persist_to_nucleus(session_id, updated, ttl)
        :ok

      [] ->
        {:error, :not_found}
    end
  rescue
    ArgumentError -> {:error, :not_found}
  end

  @doc """
  Removes all expired sessions from ETS.
  """
  @spec cleanup() :: non_neg_integer()
  def cleanup do
    now = System.system_time(:second)

    :ets.foldl(
      fn {session_id, record}, count ->
        if record.expires_at < now do
          :ets.delete(@table, session_id)
          count + 1
        else
          count
        end
      end,
      0,
      @table
    )
  rescue
    ArgumentError -> 0
  end

  # --- Internal ---

  defp expired?(record) do
    System.system_time(:second) > record.expires_at
  end

  defp generate_session_id do
    :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)
  end

  defp persist_to_nucleus(session_id, record, ttl) do
    case Process.whereis(Nucleus.Client) do
      nil ->
        :ok

      _pid ->
        try do
          Nucleus.Models.KV.set(
            Nucleus.Client,
            "#{@kv_prefix}#{session_id}",
            Jason.encode!(record),
            ttl: ttl
          )
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
    end
  end

  defp load_from_nucleus(session_id) do
    case Process.whereis(Nucleus.Client) do
      nil ->
        {:error, :not_found}

      _pid ->
        try do
          case Nucleus.Models.KV.get(Nucleus.Client, "#{@kv_prefix}#{session_id}") do
            {:ok, nil} ->
              {:error, :not_found}

            {:ok, json} ->
              case Jason.decode(json) do
                {:ok, map} ->
                  record = %{
                    data: map["data"],
                    created_at: map["created_at"],
                    expires_at: map["expires_at"]
                  }

                  {:ok, record}

                _ ->
                  {:error, :not_found}
              end

            _ ->
              {:error, :not_found}
          end
        rescue
          _ -> {:error, :not_found}
        catch
          :exit, _ -> {:error, :not_found}
        end
    end
  end

  defp delete_from_nucleus(session_id) do
    case Process.whereis(Nucleus.Client) do
      nil ->
        :ok

      _pid ->
        try do
          Nucleus.Models.KV.del(Nucleus.Client, "#{@kv_prefix}#{session_id}")
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
    end
  end
end

defmodule Neutron.Auth.SessionSweeper do
  @moduledoc """
  Periodic cleanup of expired sessions from ETS.

  Runs `Neutron.Auth.Session.cleanup/0` every 60 seconds (configurable via
  `NEUTRON_SESSION_SWEEP_INTERVAL_MS`). Started automatically in the
  Neutron supervision tree.
  """
  use GenServer
  require Logger

  @default_interval_ms 60_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    interval = get_interval()
    Process.send_after(self(), :sweep, interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:sweep, %{interval: interval} = state) do
    removed = Neutron.Auth.Session.cleanup()

    if removed > 0 do
      Logger.debug("[SessionSweeper] Cleaned up #{removed} expired session(s)")
    end

    Process.send_after(self(), :sweep, interval)
    {:noreply, state}
  end

  defp get_interval do
    case System.get_env("NEUTRON_SESSION_SWEEP_INTERVAL_MS") do
      nil -> @default_interval_ms
      val -> String.to_integer(val)
    end
  end
end
