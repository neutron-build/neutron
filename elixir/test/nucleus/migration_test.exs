defmodule Nucleus.MigrationTest do
  use ExUnit.Case, async: true

  alias Nucleus.Migration

  describe "behaviour" do
    test "defines up/1 callback" do
      assert {:up, 1} in Nucleus.Migration.behaviour_info(:callbacks)
    end

    test "defines down/1 callback" do
      assert {:down, 1} in Nucleus.Migration.behaviour_info(:callbacks)
    end
  end

  describe "__using__ macro" do
    defmodule TestMigration do
      use Nucleus.Migration

      @impl true
      def up(_client), do: :ok

      @impl true
      def down(_client), do: :ok
    end

    test "provides execute/2 function" do
      assert function_exported?(TestMigration, :execute, 2)
    end

    test "migration can implement up/1" do
      assert :ok = TestMigration.up(:fake_client)
    end

    test "migration can implement down/1" do
      assert :ok = TestMigration.down(:fake_client)
    end
  end
end
