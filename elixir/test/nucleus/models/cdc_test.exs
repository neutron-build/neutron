defmodule Nucleus.Models.CDCTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.CDC

  describe "module exports" do
    test "exports read/2 and read/3" do
      assert function_exported?(CDC, :read, 2)
      assert function_exported?(CDC, :read, 3)
    end

    test "exports count/1" do
      assert function_exported?(CDC, :count, 1)
    end

    test "exports table_read/3 and table_read/4" do
      assert function_exported?(CDC, :table_read, 3)
      assert function_exported?(CDC, :table_read, 4)
    end
  end

  # CDC_READ / CDC_TABLE_READ have a single Ok arm (scalar_fns.rs):
  # Value::Text("[...]") — an empty log answers "[]", never NULL — and the
  # wire column is TEXT, so Postgrex always delivers a binary; any other
  # shape raises rather than collapsing into {:ok, []}.
  describe "payload handling" do
    test "an empty log (\"[]\" payload) is an empty list" do
      ok = %Postgrex.Result{rows: [["[]"]], num_rows: 1}

      assert {:ok, []} = CDC.read(fake_client({:ok, ok}), 0)
      assert {:ok, []} = CDC.table_read(fake_client({:ok, ok}), "users", 0)
    end

    test "a populated payload decodes to events" do
      json = ~s([{"seq":1,"table":"users","change":"INSERT","ts":1700000000}])
      ok = %Postgrex.Result{rows: [[json]], num_rows: 1}

      assert {:ok, [%{"seq" => 1, "table" => "users", "change" => "INSERT"}]} =
               CDC.read(fake_client({:ok, ok}), 0)
    end

    test "a NULL cell is a contract violation, not an empty result" do
      null_cell = %Postgrex.Result{rows: [[nil]], num_rows: 1}

      assert_raise CaseClauseError, fn -> CDC.read(fake_client({:ok, null_cell}), 0) end

      assert_raise CaseClauseError, fn ->
        CDC.table_read(fake_client({:ok, null_cell}), "users", 0)
      end
    end

    test "invalid JSON raises instead of collapsing to empty" do
      assert_raise Jason.DecodeError, fn ->
        CDC.read(fake_client(cell("not json")), 0)
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
