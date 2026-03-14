defmodule NeutronTest do
  use ExUnit.Case, async: true

  describe "Neutron" do
    test "version/0 returns a valid semver string" do
      version = Neutron.version()
      assert is_binary(version)
      assert version =~ ~r/^\d+\.\d+\.\d+$/
    end

    test "child_spec/1 returns valid spec with required options" do
      defmodule TestRouter do
        use Plug.Router
        plug(:match)
        plug(:dispatch)
        match(_, do: send_resp(conn, 200, "ok"))
      end

      spec = Neutron.child_spec(router: TestRouter, port: 9999)
      assert spec.id == Neutron
      assert spec.type == :supervisor
    end
  end
end
