defmodule Nucleus.Models.BlobTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Blob

  describe "module exports" do
    test "exports store/3 and store/4" do
      assert function_exported?(Blob, :store, 3)
      assert function_exported?(Blob, :store, 4)
    end

    test "exports get/2" do
      assert function_exported?(Blob, :get, 2)
    end

    test "exports delete/2" do
      assert function_exported?(Blob, :delete, 2)
    end

    test "exports meta/2" do
      assert function_exported?(Blob, :meta, 2)
    end

    test "exports tag/4" do
      assert function_exported?(Blob, :tag, 4)
    end

    test "exports list/1 and list/2" do
      assert function_exported?(Blob, :list, 1)
      assert function_exported?(Blob, :list, 2)
    end

    test "exports count/1" do
      assert function_exported?(Blob, :count, 1)
    end

    test "exports dedup_ratio/1" do
      assert function_exported?(Blob, :dedup_ratio, 1)
    end
  end
end
