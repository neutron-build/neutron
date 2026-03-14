defmodule Nucleus.Models.FTSTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.FTS

  describe "module exports" do
    test "exports index/3" do
      assert function_exported?(FTS, :index, 3)
    end

    test "exports search/2 and search/3" do
      assert function_exported?(FTS, :search, 2)
      assert function_exported?(FTS, :search, 3)
    end

    test "exports fuzzy_search/3 and fuzzy_search/4" do
      assert function_exported?(FTS, :fuzzy_search, 3)
      assert function_exported?(FTS, :fuzzy_search, 4)
    end

    test "exports remove/2" do
      assert function_exported?(FTS, :remove, 2)
    end

    test "exports doc_count/1" do
      assert function_exported?(FTS, :doc_count, 1)
    end

    test "exports term_count/1" do
      assert function_exported?(FTS, :term_count, 1)
    end
  end
end
