defmodule Nucleus.Models.StreamsTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Streams

  describe "module exports" do
    test "exports xadd/3" do
      assert function_exported?(Streams, :xadd, 3)
    end

    test "exports xlen/2" do
      assert function_exported?(Streams, :xlen, 2)
    end

    test "exports xrange/5" do
      assert function_exported?(Streams, :xrange, 5)
    end

    test "exports xread/4" do
      assert function_exported?(Streams, :xread, 4)
    end

    test "exports xgroup_create/3 and xgroup_create/4" do
      assert function_exported?(Streams, :xgroup_create, 3)
      assert function_exported?(Streams, :xgroup_create, 4)
    end

    test "exports xreadgroup/5" do
      assert function_exported?(Streams, :xreadgroup, 5)
    end

    test "exports xack/5" do
      assert function_exported?(Streams, :xack, 5)
    end
  end
end
