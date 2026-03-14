defmodule Nucleus.Models.CDCTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.CDC

  describe "module exports" do
    test "exports read/2" do
      assert function_exported?(CDC, :read, 2)
    end

    test "exports count/1" do
      assert function_exported?(CDC, :count, 1)
    end

    test "exports table_read/3" do
      assert function_exported?(CDC, :table_read, 3)
    end
  end
end
