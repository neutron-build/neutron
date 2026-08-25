defmodule Nucleus.Models.DatalogTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Datalog

  describe "module exports" do
    test "exports assert/2" do
      assert function_exported?(Datalog, :assert, 2)
    end

    test "exports retract/2" do
      assert function_exported?(Datalog, :retract, 2)
    end

    test "exports rule/3" do
      assert function_exported?(Datalog, :rule, 3)
    end

    test "exports query/2" do
      assert function_exported?(Datalog, :query, 2)
    end

    test "exports clear/2" do
      assert function_exported?(Datalog, :clear, 2)
    end

    test "exports import_graph/2" do
      assert function_exported?(Datalog, :import_graph, 2)
    end
  end

  # DATALOG_QUERY's only Ok arm is Value::Text(json) (scalar_fns.rs), and
  # sql_query (datalog/mod.rs) always formats a "[...]" array — an empty
  # result answers "[]", never NULL — and the wire column is TEXT, so Postgrex
  # always delivers a binary; any other shape raises rather than collapsing
  # into {:ok, []}.
  describe "query/2 payload handling" do
    test "no matches (\"[]\" payload) is an empty list" do
      ok = %Postgrex.Result{rows: [["[]"]], num_rows: 1}

      assert {:ok, []} = Datalog.query(fake_client({:ok, ok}), "parent(alice, X)")
    end

    test "a populated payload decodes to result tuples" do
      ok = %Postgrex.Result{rows: [[~s([["alice","bob"],["alice","charlie"]])]], num_rows: 1}

      assert {:ok, [["alice", "bob"], ["alice", "charlie"]]} =
               Datalog.query(fake_client({:ok, ok}), "parent(alice, X)")
    end

    test "a NULL cell is a contract violation, not an empty result" do
      null_cell = %Postgrex.Result{rows: [[nil]], num_rows: 1}

      assert_raise CaseClauseError, fn ->
        Datalog.query(fake_client({:ok, null_cell}), "parent(alice, X)")
      end
    end

    test "a list cell cannot arrive from the TEXT column and raises" do
      list_cell = %Postgrex.Result{rows: [[["alice", "bob"]]], num_rows: 1}

      assert_raise CaseClauseError, fn ->
        Datalog.query(fake_client({:ok, list_cell}), "parent(alice, X)")
      end
    end

    test "invalid JSON raises instead of collapsing to empty" do
      assert_raise Jason.DecodeError, fn ->
        Datalog.query(fake_client(cell("not json")), "parent(alice, X)")
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
