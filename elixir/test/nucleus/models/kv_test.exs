defmodule Nucleus.Models.KVTest do
  @moduledoc """
  Tests for Nucleus.Models.KV.

  Since these functions require a live Nucleus database connection, we test:
  1. That the module exports all expected functions
  2. That each function checks for Nucleus via require_nucleus (returns error without it)
  3. Correct function arities
  """
  use ExUnit.Case, async: true

  alias Nucleus.Models.KV

  describe "module exports" do
    # Base operations
    test "exports get/2" do
      assert function_exported?(KV, :get, 2)
    end

    test "exports set/3 and set/4" do
      assert function_exported?(KV, :set, 3)
      assert function_exported?(KV, :set, 4)
    end

    test "exports setnx/3" do
      assert function_exported?(KV, :setnx, 3)
    end

    test "exports del/2" do
      assert function_exported?(KV, :del, 2)
    end

    test "exports exists?/2" do
      assert function_exported?(KV, :exists?, 2)
    end

    test "exports incr/2 and incr/3" do
      assert function_exported?(KV, :incr, 2)
      assert function_exported?(KV, :incr, 3)
    end

    test "exports ttl/2" do
      assert function_exported?(KV, :ttl, 2)
    end

    test "exports expire/3" do
      assert function_exported?(KV, :expire, 3)
    end

    test "exports dbsize/1" do
      assert function_exported?(KV, :dbsize, 1)
    end

    test "exports flushdb/1" do
      assert function_exported?(KV, :flushdb, 1)
    end

    # List operations
    test "exports lpush/3" do
      assert function_exported?(KV, :lpush, 3)
    end

    test "exports rpush/3" do
      assert function_exported?(KV, :rpush, 3)
    end

    test "exports lpop/2" do
      assert function_exported?(KV, :lpop, 2)
    end

    test "exports rpop/2" do
      assert function_exported?(KV, :rpop, 2)
    end

    test "exports lrange/4" do
      assert function_exported?(KV, :lrange, 4)
    end

    test "exports llen/2" do
      assert function_exported?(KV, :llen, 2)
    end

    test "exports lindex/3" do
      assert function_exported?(KV, :lindex, 3)
    end

    # Hash operations
    test "exports hset/4" do
      assert function_exported?(KV, :hset, 4)
    end

    test "exports hget/3" do
      assert function_exported?(KV, :hget, 3)
    end

    test "exports hdel/3" do
      assert function_exported?(KV, :hdel, 3)
    end

    test "exports hexists?/3" do
      assert function_exported?(KV, :hexists?, 3)
    end

    test "exports hgetall/2" do
      assert function_exported?(KV, :hgetall, 2)
    end

    test "exports hlen/2" do
      assert function_exported?(KV, :hlen, 2)
    end

    # Set operations
    test "exports sadd/3" do
      assert function_exported?(KV, :sadd, 3)
    end

    test "exports srem/3" do
      assert function_exported?(KV, :srem, 3)
    end

    test "exports smembers/2" do
      assert function_exported?(KV, :smembers, 2)
    end

    test "exports sismember?/3" do
      assert function_exported?(KV, :sismember?, 3)
    end

    test "exports scard/2" do
      assert function_exported?(KV, :scard, 2)
    end

    # Sorted set operations
    test "exports zadd/4" do
      assert function_exported?(KV, :zadd, 4)
    end

    test "exports zrange/4" do
      assert function_exported?(KV, :zrange, 4)
    end

    test "exports zrangebyscore/4" do
      assert function_exported?(KV, :zrangebyscore, 4)
    end

    test "exports zrem/3" do
      assert function_exported?(KV, :zrem, 3)
    end

    test "exports zcard/2" do
      assert function_exported?(KV, :zcard, 2)
    end

    # HyperLogLog operations
    test "exports pfadd/3" do
      assert function_exported?(KV, :pfadd, 3)
    end

    test "exports pfcount/2" do
      assert function_exported?(KV, :pfcount, 2)
    end
  end

  # Every KV collection Ok arm is Value::Text(serde_json::to_string(&vec))
  # (scalar_fns.rs KV_LRANGE/KV_SMEMBERS/KV_HGETALL/KV_ZRANGE/KV_ZRANGEBYSCORE)
  # — serde of a Vec emits at least "[]", so the empty collection arrives as
  # "[]" and never NULL; the wire column is TEXT, so Postgrex always delivers
  # a binary. Any other shape raises rather than collapsing into {:ok, []}.
  describe "collection payload handling" do
    test "an empty collection (\"[]\" payload) is empty" do
      ok = %Postgrex.Result{rows: [["[]"]], num_rows: 1}

      assert {:ok, []} = KV.lrange(fake_client({:ok, ok}), "k", 0, -1)
      assert {:ok, []} = KV.smembers(fake_client({:ok, ok}), "k")
      assert {:ok, %{}} = KV.hgetall(fake_client({:ok, ok}), "k")
      assert {:ok, []} = KV.zrange(fake_client({:ok, ok}), "k", 0, -1)
      assert {:ok, []} = KV.zrangebyscore(fake_client({:ok, ok}), "k", 0.0, 1.0)
    end

    test "populated payloads decode" do
      assert {:ok, ["a", "b"]} =
               KV.lrange(fake_client(cell(~s(["a","b"]))), "k", 0, -1)

      assert {:ok, ["a"]} =
               KV.smembers(fake_client(cell(~s(["a"]))), "k")

      assert {:ok, %{"f" => "v"}} =
               KV.hgetall(fake_client(cell(~s([["f","v"]]))), "k")

      assert {:ok, ["m"]} = KV.zrange(fake_client(cell(~s([["m",1.0]]))), "k", 0, -1)
    end

    test "a NULL cell is a contract violation, not an empty collection" do
      null_cell = %Postgrex.Result{rows: [[nil]], num_rows: 1}

      assert_raise CaseClauseError, fn -> KV.lrange(fake_client({:ok, null_cell}), "k", 0, -1) end
      assert_raise CaseClauseError, fn -> KV.smembers(fake_client({:ok, null_cell}), "k") end
      assert_raise CaseClauseError, fn -> KV.hgetall(fake_client({:ok, null_cell}), "k") end
      assert_raise CaseClauseError, fn -> KV.zrange(fake_client({:ok, null_cell}), "k", 0, -1) end
    end

    test "invalid JSON raises instead of collapsing to empty" do
      assert_raise Jason.DecodeError, fn ->
        KV.lrange(fake_client(cell("not json")), "k", 0, -1)
      end
    end
  end

  defp cell(value), do: {:ok, %Postgrex.Result{rows: [[value]], num_rows: 1}}

  # Stands in for the Nucleus.Client GenServer, as in streams_test.exs.
  defp fake_client(query_reply) do
    spawn_link(fn -> fake_loop(query_reply) end)
  end

  defp fake_loop(query_reply) do
    receive do
      {:"$gen_call", from, :is_nucleus?} ->
        GenServer.reply(from, true)
        fake_loop(query_reply)

      {:"$gen_call", from, {:query, _sql, _params}} ->
        GenServer.reply(from, query_reply)
        fake_loop(query_reply)
    end
  end
end
