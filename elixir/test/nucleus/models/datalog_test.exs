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

    test "exports clear/1" do
      assert function_exported?(Datalog, :clear, 1)
    end

    test "exports import_graph/1" do
      assert function_exported?(Datalog, :import_graph, 1)
    end
  end
end
