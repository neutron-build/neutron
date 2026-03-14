defmodule Nucleus.Models.TimeSeriesTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.TimeSeries

  describe "module exports" do
    test "exports insert/4" do
      assert function_exported?(TimeSeries, :insert, 4)
    end

    test "exports last/2" do
      assert function_exported?(TimeSeries, :last, 2)
    end

    test "exports count/2" do
      assert function_exported?(TimeSeries, :count, 2)
    end

    test "exports range_count/4" do
      assert function_exported?(TimeSeries, :range_count, 4)
    end

    test "exports range_avg/4" do
      assert function_exported?(TimeSeries, :range_avg, 4)
    end

    test "exports retention/3" do
      assert function_exported?(TimeSeries, :retention, 3)
    end

    test "exports match/3" do
      assert function_exported?(TimeSeries, :match, 3)
    end

    test "exports time_bucket/3" do
      assert function_exported?(TimeSeries, :time_bucket, 3)
    end
  end
end
