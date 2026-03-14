defmodule Neutron.AppTest do
  use ExUnit.Case

  describe "Neutron.App" do
    test "defines start/2 callback" do
      assert function_exported?(Neutron.App, :start, 2)
    end

    test "defines stop/1 callback" do
      assert function_exported?(Neutron.App, :stop, 1)
    end

    test "stop returns :ok" do
      assert :ok = Neutron.App.stop(%{})
    end
  end

  describe "Neutron.ETS.Manager" do
    test "defines start_link/1" do
      assert function_exported?(Neutron.ETS.Manager, :start_link, 1)
    end

    test "defines child_spec/1" do
      spec = Neutron.ETS.Manager.child_spec([])
      assert is_map(spec)
      assert spec.id == Neutron.ETS.Manager
    end
  end
end
