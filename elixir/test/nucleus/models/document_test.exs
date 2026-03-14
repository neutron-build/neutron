defmodule Nucleus.Models.DocumentTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Document

  describe "module exports" do
    test "exports insert/2" do
      assert function_exported?(Document, :insert, 2)
    end

    test "exports get/2" do
      assert function_exported?(Document, :get, 2)
    end

    test "exports query/2" do
      assert function_exported?(Document, :query, 2)
    end

    test "exports path/3" do
      assert function_exported?(Document, :path, 3)
    end

    test "exports count/1" do
      assert function_exported?(Document, :count, 1)
    end
  end
end
