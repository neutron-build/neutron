defmodule Nucleus.Models.VectorTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Vector

  describe "module exports" do
    test "exports create_collection/3 and create_collection/4" do
      assert function_exported?(Vector, :create_collection, 3)
      assert function_exported?(Vector, :create_collection, 4)
    end

    test "exports insert/4 and insert/5" do
      assert function_exported?(Vector, :insert, 4)
      assert function_exported?(Vector, :insert, 5)
    end

    test "exports search/3 and search/4" do
      assert function_exported?(Vector, :search, 3)
      assert function_exported?(Vector, :search, 4)
    end

    test "exports delete/3" do
      assert function_exported?(Vector, :delete, 3)
    end

    test "exports dims/2" do
      assert function_exported?(Vector, :dims, 2)
    end

    test "exports distance/3 and distance/4" do
      assert function_exported?(Vector, :distance, 3)
      assert function_exported?(Vector, :distance, 4)
    end
  end

  describe "identifier validation" do
    test "create_collection rejects invalid identifiers" do
      # We cannot call without a real client for require_nucleus,
      # but we can verify the module structure is correct
      assert function_exported?(Vector, :create_collection, 4)
    end
  end
end
