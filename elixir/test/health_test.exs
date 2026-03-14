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
      assert is_boolean(body["nucleus"])
      assert body["version"] == Neutron.version()
    end

    test "nucleus is false when Nucleus.Client is not running" do
      conn =
        conn(:get, "/health")
        |> Health.call([])

      body = Jason.decode!(conn.resp_body)
      assert body["nucleus"] == false
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
