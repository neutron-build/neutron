defmodule Nucleus.Models.GraphTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Graph

  describe "module exports" do
    test "exports add_node/2 and add_node/3" do
      assert function_exported?(Graph, :add_node, 2)
      assert function_exported?(Graph, :add_node, 3)
    end

    test "exports add_edge/4 and add_edge/5" do
      assert function_exported?(Graph, :add_edge, 4)
      assert function_exported?(Graph, :add_edge, 5)
    end

    test "exports delete_node/2" do
      assert function_exported?(Graph, :delete_node, 2)
    end

    test "exports delete_edge/2" do
      assert function_exported?(Graph, :delete_edge, 2)
    end

    test "exports query/2" do
      assert function_exported?(Graph, :query, 2)
    end

    test "exports neighbors/2 and neighbors/3" do
      assert function_exported?(Graph, :neighbors, 2)
      assert function_exported?(Graph, :neighbors, 3)
    end

    test "exports shortest_path/3" do
      assert function_exported?(Graph, :shortest_path, 3)
    end

    test "exports node_count/1" do
      assert function_exported?(Graph, :node_count, 1)
    end

    test "exports edge_count/1" do
      assert function_exported?(Graph, :edge_count, 1)
    end
  end

  # GRAPH_NEIGHBORS's only Ok arm is Value::Text("[...]") (scalar_fns.rs) — an
  # unknown node answers "[]", never NULL — and the wire column is TEXT, so
  # Postgrex always delivers a binary; any other shape raises.
  describe "neighbors/3 payload handling" do
    test "no neighbors (\"[]\" payload) is an empty list" do
      ok = %Postgrex.Result{rows: [["[]"]], num_rows: 1}

      assert {:ok, []} = Graph.neighbors(fake_client({:ok, ok}), 1)
    end

    test "a populated payload decodes to neighbors" do
      json = ~s([{"neighbor_id":2,"edge_id":10,"edge_type":"KNOWS"}])

      assert {:ok, [%{"neighbor_id" => 2, "edge_id" => 10, "edge_type" => "KNOWS"}]} =
               Graph.neighbors(
                 fake_client({:ok, %Postgrex.Result{rows: [[json]], num_rows: 1}}),
                 1
               )
    end

    test "a NULL cell is a contract violation, not an empty result" do
      null_cell = %Postgrex.Result{rows: [[nil]], num_rows: 1}

      assert_raise CaseClauseError, fn ->
        Graph.neighbors(fake_client({:ok, null_cell}), 1)
      end
    end

    test "a list cell cannot arrive from the TEXT column and raises" do
      list_cell = %Postgrex.Result{rows: [[[1, 2]]], num_rows: 1}

      assert_raise CaseClauseError, fn ->
        Graph.neighbors(fake_client({:ok, list_cell}), 1)
      end
    end
  end

  # GRAPH_SHORTEST_PATH has two Ok arms (scalar_fns.rs): Text "[...]" when a
  # path exists and Value::Null when none does — the wire column is declared
  # TEXT, but NULL is reachable at runtime (the LINDEX precedent), so the
  # model must map the NULL cell to "no path" explicitly.
  describe "shortest_path/3 payload handling" do
    test "no path (NULL cell) is an empty list" do
      null_cell = %Postgrex.Result{rows: [[nil]], num_rows: 1}

      assert {:ok, []} = Graph.shortest_path(fake_client({:ok, null_cell}), 1, 2)
    end

    test "a populated payload decodes to node ids" do
      ok = %Postgrex.Result{rows: [[~s([1, 4, 2])]], num_rows: 1}

      assert {:ok, [1, 4, 2]} = Graph.shortest_path(fake_client({:ok, ok}), 1, 2)
    end

    test "a list cell cannot arrive from the TEXT column and raises" do
      list_cell = %Postgrex.Result{rows: [[[1, 2]]], num_rows: 1}

      assert_raise CaseClauseError, fn ->
        Graph.shortest_path(fake_client({:ok, list_cell}), 1, 2)
      end
    end
  end

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
