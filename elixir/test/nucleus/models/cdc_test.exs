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
end
