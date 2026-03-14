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
end
