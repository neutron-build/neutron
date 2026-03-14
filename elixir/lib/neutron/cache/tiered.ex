defmodule Neutron.Cache do
  @moduledoc """
  Tiered cache: ETS L1 (microsecond reads) + Nucleus KV L2 (distributed).

  Provides a simple get/set/delete API with automatic tiering. Reads check
  ETS first, then fall back to Nucleus KV. Writes go to both tiers.

  ## Example

      # Set a value (writes to both L1 and L2)
      Neutron.Cache.put("user:42", %{name: "Alice"}, ttl: 300)

      # Get a value (checks ETS first, then Nucleus)
      {:ok, user} = Neutron.Cache.get("user:42")

      # Delete
      Neutron.Cache.delete("user:42")

      # Get-or-set pattern
      user = Neutron.Cache.fetch("user:42", fn ->
        {:ok, MyApp.Users.get(42)}
      end, ttl: 300)

  ## TTL

  Entries in ETS are lazily expired on read. A background sweep runs
  every 60 seconds to clean up stale entries.
  """

  use GenServer
  require Logger

  @ets_table :neutron_cache
  @kv_prefix "neutron:cache:"
  @sweep_interval 60_000

  @type key :: String.t()
  @type value :: term()

  # --- Client API ---

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Retrieves a value from cache.

  Checks ETS (L1) first, then Nucleus KV (L2). If found in L2 but not L1,
  the value is backfilled into L1.
  """
  @spec get(key()) :: {:ok, value()} | {:error, :not_found}
  def get(key) do
    case ets_get(key) do
      {:ok, value} ->
        {:ok, value}

      :miss ->
        case nucleus_get(key) do
          {:ok, value} ->
            # Backfill L1
            ets_put(key, value, nil)
            {:ok, value}

          :miss ->
            {:error, :not_found}
        end
    end
  end

  @doc """
  Stores a value in both cache tiers.

  ## Options

    * `:ttl` — time-to-live in seconds (default: no expiry)
  """
  @spec put(key(), value(), keyword()) :: :ok
  def put(key, value, opts \\ []) do
    ttl = Keyword.get(opts, :ttl)
    ets_put(key, value, ttl)
    nucleus_put(key, value, ttl)
    :ok
  end

  @doc """
  Deletes a value from both cache tiers.
  """
  @spec delete(key()) :: :ok
  def delete(key) do
    ets_delete(key)
    nucleus_delete(key)
    :ok
  end

  @doc """
  Gets a value or computes and caches it.

  If the key is not in cache, calls the function to compute the value,
  stores it, and returns it.

  ## Example

      user = Neutron.Cache.fetch("user:42", fn ->
        {:ok, expensive_lookup(42)}
      end, ttl: 300)
  """
  @spec fetch(key(), (-> {:ok, value()} | {:error, term()}), keyword()) ::
          {:ok, value()} | {:error, term()}
  def fetch(key, compute_fn, opts \\ []) do
    case get(key) do
      {:ok, value} ->
        {:ok, value}

      {:error, :not_found} ->
        case compute_fn.() do
          {:ok, value} ->
            put(key, value, opts)
            {:ok, value}

          {:error, _} = error ->
            error
        end
    end
  end

  @doc """
  Checks if a key exists in cache (either tier).
  """
  @spec exists?(key()) :: boolean()
  def exists?(key) do
    case get(key) do
      {:ok, _} -> true
      _ -> false
    end
  end

  @doc """
  Clears all entries from L1 cache.
  """
  @spec clear() :: :ok
  def clear do
    try do
      :ets.delete_all_objects(@ets_table)
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  @doc """
  Returns L1 cache stats.
  """
  @spec stats() :: %{size: non_neg_integer(), memory_bytes: non_neg_integer()}
  def stats do
    try do
      info = :ets.info(@ets_table)

      %{
        size: Keyword.get(info, :size, 0),
        memory_bytes: Keyword.get(info, :memory, 0) * :erlang.system_info(:wordsize)
      }
    rescue
      ArgumentError -> %{size: 0, memory_bytes: 0}
    end
  end

  # --- GenServer (sweep timer) ---

  @impl true
  def init(_opts) do
    Process.send_after(self(), :sweep, @sweep_interval)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:sweep, state) do
    sweep_expired()
    Process.send_after(self(), :sweep, @sweep_interval)
    {:noreply, state}
  end

  # --- Internal: ETS (L1) ---

  defp ets_get(key) do
    case :ets.lookup(@ets_table, key) do
      [{^key, value, nil}] ->
        {:ok, value}

      [{^key, value, expires_at}] ->
        if System.system_time(:second) < expires_at do
          {:ok, value}
        else
          :ets.delete(@ets_table, key)
          :miss
        end

      [] ->
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  defp ets_put(key, value, nil) do
    :ets.insert(@ets_table, {key, value, nil})
  rescue
    ArgumentError -> :ok
  end

  defp ets_put(key, value, ttl) when is_integer(ttl) do
    expires_at = System.system_time(:second) + ttl
    :ets.insert(@ets_table, {key, value, expires_at})
  rescue
    ArgumentError -> :ok
  end

  defp ets_delete(key) do
    :ets.delete(@ets_table, key)
  rescue
    ArgumentError -> :ok
  end

  defp sweep_expired do
    now = System.system_time(:second)

    :ets.foldl(
      fn
        {key, _value, expires_at}, count when is_integer(expires_at) and expires_at < now ->
          :ets.delete(@ets_table, key)
          count + 1

        _, count ->
          count
      end,
      0,
      @ets_table
    )
  rescue
    ArgumentError -> 0
  end

  # --- Internal: Nucleus KV (L2) ---

  defp nucleus_get(key) do
    case Process.whereis(Nucleus.Client) do
      nil ->
        :miss

      _pid ->
        try do
          case Nucleus.Models.KV.get(Nucleus.Client, "#{@kv_prefix}#{key}") do
            {:ok, nil} -> :miss
            {:ok, json} ->
              case Jason.decode(json) do
                {:ok, value} -> {:ok, value}
                _ -> :miss
              end
            _ -> :miss
          end
        rescue
          _ -> :miss
        catch
          :exit, _ -> :miss
        end
    end
  end

  defp nucleus_put(key, value, ttl) do
    case Process.whereis(Nucleus.Client) do
      nil ->
        :ok

      _pid ->
        try do
          opts = if ttl, do: [ttl: ttl], else: []
          Nucleus.Models.KV.set(Nucleus.Client, "#{@kv_prefix}#{key}", Jason.encode!(value), opts)
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
    end
  end

  defp nucleus_delete(key) do
    case Process.whereis(Nucleus.Client) do
      nil ->
        :ok

      _pid ->
        try do
          Nucleus.Models.KV.del(Nucleus.Client, "#{@kv_prefix}#{key}")
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
    end
  end
end
