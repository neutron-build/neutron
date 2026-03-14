defmodule Nucleus.Models.GeoTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Geo

  describe "module exports" do
    test "exports distance/5" do
      assert function_exported?(Geo, :distance, 5)
    end

    test "exports distance_euclidean/5" do
      assert function_exported?(Geo, :distance_euclidean, 5)
    end

    test "exports within?/6" do
      assert function_exported?(Geo, :within?, 6)
    end

    test "exports area/5" do
      assert function_exported?(Geo, :area, 5)
    end

    test "exports make_point/3" do
      assert function_exported?(Geo, :make_point, 3)
    end

    test "exports st_x/2" do
      assert function_exported?(Geo, :st_x, 2)
    end

    test "exports st_y/2" do
      assert function_exported?(Geo, :st_y, 2)
    end
  end
end
