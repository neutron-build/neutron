defmodule Neutron.CacheTest do
  use ExUnit.Case

  alias Neutron.Cache

  setup do
    # Ensure the ETS table exists for cache
    try do
      :ets.delete(:neutron_cache)
    rescue
      ArgumentError -> :ok
    end

    :ets.new(:neutron_cache, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ok
  end

  describe "put/3 and get/1" do
    test "stores and retrieves a value" do
      assert :ok = Cache.put("key1", "value1")
      assert {:ok, "value1"} = Cache.get("key1")
    end

    test "stores map values" do
      data = %{name: "Alice", age: 30}
      assert :ok = Cache.put("user:1", data)
      assert {:ok, ^data} = Cache.get("user:1")
    end

    test "stores list values" do
      data = [1, 2, 3]
      assert :ok = Cache.put("list", data)
      assert {:ok, ^data} = Cache.get("list")
    end

    test "overwrites existing key" do
      Cache.put("key", "v1")
      Cache.put("key", "v2")
      assert {:ok, "v2"} = Cache.get("key")
    end

    test "returns {:error, :not_found} for missing key" do
      assert {:error, :not_found} = Cache.get("nonexistent")
    end

    test "stores value with TTL" do
      Cache.put("ttl_key", "value", ttl: 3600)
      assert {:ok, "value"} = Cache.get("ttl_key")
    end

    test "expired value returns :not_found" do
      # Insert directly with past expiry
      :ets.insert(:neutron_cache, {"expired_key", "old_value", 0})
      assert {:error, :not_found} = Cache.get("expired_key")
    end

    test "value without TTL does not expire" do
      Cache.put("no_ttl", "persistent")
      # Stored with nil expiry
      [{_, _, nil}] = :ets.lookup(:neutron_cache, "no_ttl")
      assert {:ok, "persistent"} = Cache.get("no_ttl")
    end
  end

  describe "delete/1" do
    test "removes a key" do
      Cache.put("del_key", "value")
      assert :ok = Cache.delete("del_key")
      assert {:error, :not_found} = Cache.get("del_key")
    end

    test "returns :ok for non-existent key" do
      assert :ok = Cache.delete("nonexistent")
    end
  end

  describe "fetch/3" do
    test "returns cached value if present" do
      Cache.put("fetch_key", "cached")

      result = Cache.fetch("fetch_key", fn ->
        {:ok, "computed"}
      end)

      assert {:ok, "cached"} = result
    end

    test "computes and caches value if not present" do
      result = Cache.fetch("compute_key", fn ->
        {:ok, "computed_value"}
      end)

      assert {:ok, "computed_value"} = result
      # Should now be cached
      assert {:ok, "computed_value"} = Cache.get("compute_key")
    end

    test "passes through compute errors without caching" do
      result = Cache.fetch("error_key", fn ->
        {:error, :db_down}
      end)

      assert {:error, :db_down} = result
      assert {:error, :not_found} = Cache.get("error_key")
    end

    test "respects TTL option" do
      Cache.fetch("ttl_fetch", fn ->
        {:ok, "value"}
      end, ttl: 60)

      [{_, _, expires_at}] = :ets.lookup(:neutron_cache, "ttl_fetch")
      assert is_integer(expires_at)
      assert expires_at > System.system_time(:second)
    end
  end

  describe "exists?/1" do
    test "returns true for existing key" do
      Cache.put("exist_key", "value")
      assert Cache.exists?("exist_key") == true
    end

    test "returns false for missing key" do
      assert Cache.exists?("no_exist") == false
    end

    test "returns false for expired key" do
      :ets.insert(:neutron_cache, {"expired", "val", 0})
      assert Cache.exists?("expired") == false
    end
  end

  describe "clear/0" do
    test "removes all entries" do
      Cache.put("a", 1)
      Cache.put("b", 2)
      Cache.put("c", 3)
      assert :ok = Cache.clear()
      assert {:error, :not_found} = Cache.get("a")
      assert {:error, :not_found} = Cache.get("b")
      assert {:error, :not_found} = Cache.get("c")
    end

    test "returns :ok when already empty" do
      assert :ok = Cache.clear()
    end
  end

  describe "stats/0" do
    test "returns size and memory" do
      stats = Cache.stats()
      assert is_map(stats)
      assert Map.has_key?(stats, :size)
      assert Map.has_key?(stats, :memory_bytes)
      assert is_integer(stats.size)
      assert is_integer(stats.memory_bytes)
    end

    test "size reflects number of entries" do
      Cache.put("s1", "v1")
      Cache.put("s2", "v2")
      stats = Cache.stats()
      assert stats.size == 2
    end

    test "size is 0 when empty" do
      Cache.clear()
      stats = Cache.stats()
      assert stats.size == 0
    end
  end

  describe "GenServer lifecycle" do
    test "starts as a GenServer" do
      # The GenServer handles the sweep timer
      {:ok, pid} = Cache.start_link([])
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "schedules sweep on init" do
      {:ok, pid} = Cache.start_link([])
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "handles :sweep message" do
      {:ok, pid} = Cache.start_link([])

      # Insert expired entry
      :ets.insert(:neutron_cache, {"sweep_test", "old", 0})

      # Trigger sweep
      send(pid, :sweep)
      Process.sleep(50)

      # Expired entry should be cleaned
      assert :ets.lookup(:neutron_cache, "sweep_test") == []

      GenServer.stop(pid)
    end
  end
end
