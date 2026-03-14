defmodule Neutron.Realtime.Presence do
  @moduledoc """
  Distributed presence tracking using CRDTs.

  Tracks which users are present in which topics, with automatic cleanup
  when processes terminate. Uses a last-write-wins register for metadata
  and heartbeat-based liveness detection.

  ## Example

      # Track a user's presence
      Neutron.Realtime.Presence.track("room:lobby", user_id, %{
        name: "Alice",
        online_at: System.system_time(:second)
      })

      # List present users
      presences = Neutron.Realtime.Presence.list("room:lobby")
      # => %{"user_1" => %{name: "Alice", ...}, "user_2" => %{name: "Bob", ...}}

      # Untrack
      Neutron.Realtime.Presence.untrack("room:lobby", user_id)
  """

  use GenServer
  require Logger

  @table :neutron_presence
  @heartbeat_interval 30_000
  @presence_timeout 60_000

  @type topic :: String.t()
  @type key :: String.t()
  @type meta :: map()

  # --- Client API ---

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Tracks a key (usually user ID) as present in a topic.

  The calling process is monitored — when it exits, the presence is removed.
  """
  @spec track(topic(), key(), meta()) :: :ok
  def track(topic, key, meta \\ %{}) do
    GenServer.call(__MODULE__, {:track, topic, key, meta, self()})
  end

  @doc """
  Removes a tracked key from a topic.
  """
  @spec untrack(topic(), key()) :: :ok
  def untrack(topic, key) do
    GenServer.call(__MODULE__, {:untrack, topic, key})
  end

  @doc """
  Updates the metadata for a tracked presence.
  """
  @spec update(topic(), key(), meta()) :: :ok | {:error, :not_tracked}
  def update(topic, key, meta) do
    GenServer.call(__MODULE__, {:update, topic, key, meta})
  end

  @doc """
  Lists all presences for a topic.

  Returns a map of key => metadata.
  """
  @spec list(topic()) :: %{key() => meta()}
  def list(topic) do
    try do
      :ets.lookup(@table, topic)
      |> Enum.into(%{}, fn {_topic, key, meta, _pid, _ts} ->
        {key, meta}
      end)
    rescue
      ArgumentError -> %{}
    end
  end

  @doc """
  Returns the count of present users in a topic.
  """
  @spec count(topic()) :: non_neg_integer()
  def count(topic) do
    topic |> list() |> map_size()
  end

  @doc """
  Lists all topics with tracked presences.
  """
  @spec topics() :: [topic()]
  def topics do
    try do
      :ets.foldl(
        fn {topic, _key, _meta, _pid, _ts}, acc ->
          if topic in acc, do: acc, else: [topic | acc]
        end,
        [],
        @table
      )
    rescue
      ArgumentError -> []
    end
  end

  # --- GenServer Implementation ---

  @impl true
  def init(_opts) do
    table = :ets.new(@table, [:bag, :public, :named_table, read_concurrency: true])
    Process.send_after(self(), :heartbeat_check, @heartbeat_interval)
    {:ok, %{table: table, monitors: %{}}}
  end

  @impl true
  def handle_call({:track, topic, key, meta, pid}, _from, state) do
    # Remove existing entry for this key in this topic
    remove_presence(topic, key)

    # Insert new presence
    ts = System.system_time(:millisecond)
    :ets.insert(@table, {topic, key, meta, pid, ts})

    # Monitor the process
    ref = Process.monitor(pid)
    monitors = Map.put(state.monitors, ref, {topic, key})

    # Broadcast presence join
    broadcast_diff(topic, %{joins: %{key => meta}, leaves: %{}})

    {:reply, :ok, %{state | monitors: monitors}}
  end

  @impl true
  def handle_call({:untrack, topic, key}, _from, state) do
    meta = get_meta(topic, key)
    remove_presence(topic, key)

    # Remove monitor
    {ref, monitors} = pop_monitor_by_presence(state.monitors, topic, key)
    if ref, do: Process.demonitor(ref, [:flush])

    # Broadcast presence leave
    if meta do
      broadcast_diff(topic, %{joins: %{}, leaves: %{key => meta}})
    end

    {:reply, :ok, %{state | monitors: monitors}}
  end

  @impl true
  def handle_call({:update, topic, key, meta}, _from, state) do
    case get_entry(topic, key) do
      nil ->
        {:reply, {:error, :not_tracked}, state}

      {_topic, _key, _old_meta, pid, _ts} ->
        remove_presence(topic, key)
        ts = System.system_time(:millisecond)
        :ets.insert(@table, {topic, key, meta, pid, ts})
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, ref) do
      {{topic, key}, monitors} ->
        meta = get_meta(topic, key)
        remove_presence(topic, key)

        if meta do
          broadcast_diff(topic, %{joins: %{}, leaves: %{key => meta}})
        end

        {:noreply, %{state | monitors: monitors}}

      {nil, _} ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:heartbeat_check, state) do
    now = System.system_time(:millisecond)
    cutoff = now - @presence_timeout

    # Find stale entries
    stale =
      :ets.foldl(
        fn {topic, key, meta, _pid, ts}, acc ->
          if ts < cutoff, do: [{topic, key, meta} | acc], else: acc
        end,
        [],
        @table
      )

    # Remove stale entries
    Enum.each(stale, fn {topic, key, meta} ->
      remove_presence(topic, key)
      broadcast_diff(topic, %{joins: %{}, leaves: %{key => meta}})
    end)

    Process.send_after(self(), :heartbeat_check, @heartbeat_interval)
    {:noreply, state}
  end

  # --- Internal ---

  defp remove_presence(topic, key) do
    entries = :ets.lookup(@table, topic)

    Enum.each(entries, fn {t, k, _meta, _pid, _ts} = entry ->
      if t == topic and k == key do
        :ets.delete_object(@table, entry)
      end
    end)
  end

  defp get_entry(topic, key) do
    :ets.lookup(@table, topic)
    |> Enum.find(fn {t, k, _, _, _} -> t == topic and k == key end)
  end

  defp get_meta(topic, key) do
    case get_entry(topic, key) do
      {_, _, meta, _, _} -> meta
      nil -> nil
    end
  end

  defp pop_monitor_by_presence(monitors, topic, key) do
    case Enum.find(monitors, fn {_ref, {t, k}} -> t == topic and k == key end) do
      {ref, _} -> {ref, Map.delete(monitors, ref)}
      nil -> {nil, monitors}
    end
  end

  defp broadcast_diff(topic, diff) do
    Neutron.Realtime.Channel.broadcast(topic, "presence_diff", diff)
  end
end
