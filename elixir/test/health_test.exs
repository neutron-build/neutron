defmodule Neutron.HealthTest do
  use ExUnit.Case, async: true
  use Plug.Test

  alias Neutron.Health

  describe "call/2" do
    test "responds to GET /health with 200" do
      conn =
        conn(:get, "/health")
        |> Health.call([])

      assert conn.status == 200
      assert conn.halted
    end

    test "returns JSON content type" do
      conn =
        conn(:get, "/health")
        |> Health.call([])

      content_type =
        Plug.Conn.get_resp_header(conn, "content-type")
        |> List.first()

      assert content_type =~ "application/json"
    end

    test "returns expected JSON body with status, nucleus, and version" do
      conn =
        conn(:get, "/health")
        |> Health.call([])

      body = Jason.decode!(conn.resp_body)
      assert body["status"] == "ok"

      # FRAMEWORK_CONTRACT.md §7 wants a tri-state STRING here, not a boolean.
      # This assertion used to read `assert is_boolean(body["nucleus"])`, which
      # is the shape the implementation happened to have — so the test passed
      # and guarded the defect instead of catching it.
      assert body["nucleus"] in ["connected", "disconnected", "unconfigured"]
      assert body["version"] == Neutron.version()
    end

    test "nucleus is \"unconfigured\" when Nucleus.Client is not running" do
      conn =
        conn(:get, "/health")
        |> Health.call([])

      body = Jason.decode!(conn.resp_body)

      # No client process means no nucleus is configured for this service.
      # §7 calls that "not an error", so `status` stays "ok".
      assert body["nucleus"] == "unconfigured"
      assert body["status"] == "ok"
    end

    test "nucleus is not feature detection" do
      # §7: "Feature detection (is the connected DB a Nucleus instance vs plain
      # Postgres) is §1, not /health." The old implementation returned
      # `Nucleus.Client.is_nucleus?/1` straight into this field, which answered
      # a different question in the wrong type.
      conn =
        conn(:get, "/health")
        |> Health.call([])

      body = Jason.decode!(conn.resp_body)
      refute is_boolean(body["nucleus"])
    end

    test "passes through non-health requests" do
      conn =
        conn(:get, "/api/users")
        |> Health.call([])

      refute conn.halted
      assert conn.status == nil
    end

    test "passes through non-GET requests to /health" do
      conn =
        conn(:post, "/health")
        |> Health.call([])

      refute conn.halted
      assert conn.status == nil
    end

    test "passes through PUT /health" do
      conn =
        conn(:put, "/health")
        |> Health.call([])

      refute conn.halted
    end
  end

  describe "init/1" do
    test "returns opts unchanged" do
      assert Health.init([]) == []
      assert Health.init(foo: :bar) == [foo: :bar]
    end
  end
end
