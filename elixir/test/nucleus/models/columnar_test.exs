defmodule Nucleus.Models.ColumnarTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Columnar

  describe "module exports" do
    test "exports insert/3" do
      assert function_exported?(Columnar, :insert, 3)
    end

    test "exports count/2" do
      assert function_exported?(Columnar, :count, 2)
    end

    test "exports sum/3" do
      assert function_exported?(Columnar, :sum, 3)
    end

    test "exports avg/3" do
      assert function_exported?(Columnar, :avg, 3)
    end

    test "exports min/3" do
      assert function_exported?(Columnar, :min, 3)
    end

    test "exports max/3" do
      assert function_exported?(Columnar, :max, 3)
    end
  end
end
