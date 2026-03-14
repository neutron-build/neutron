defmodule Neutron.HandlerTest do
  use ExUnit.Case, async: true
  use Plug.Test

  alias Neutron.Handler

  defmodule SuccessHandler do
    use Neutron.Handler

    @impl true
    def handle(_conn, params) do
      {:ok, %{status: 200, body: %{message: "hello", params: params}}}
    end
  end

  defmodule NotFoundHandler do
    use Neutron.Handler

    @impl true
    def handle(_conn, _params) do
      {:error, Neutron.Error.not_found("Item not found")}
    end
  end

  defmodule CrashHandler do
    use Neutron.Handler

    @impl true
    def handle(_conn, _params) do
      {:error, :something_went_wrong}
    end
  end

  defmodule HeaderHandler do
    use Neutron.Handler

    @impl true
    def handle(_conn, _params) do
      {:ok, %{
        status: 201,
        body: %{created: true},
        headers: [{"x-custom", "value"}]
      }}
    end
  end

  describe "call/2" do
    test "calls handler and sends success response" do
      conn =
        conn(:get, "/test")
        |> Handler.call(SuccessHandler)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["message"] == "hello"
    end

    test "sends error response for {:error, Neutron.Error}" do
      conn =
        conn(:get, "/test")
        |> Handler.call(NotFoundHandler)

      assert conn.status == 404
      assert conn.halted
    end

    test "sends 500 for {:error, term()}" do
      conn =
        conn(:get, "/test")
        |> Handler.call(CrashHandler)

      assert conn.status == 500
      assert conn.halted
    end

    test "sends custom headers from response" do
      conn =
        conn(:get, "/test")
        |> Handler.call(HeaderHandler)

      assert conn.status == 201
      [custom] = get_resp_header(conn, "x-custom")
      assert custom == "value"
    end

    test "response has application/json content type" do
      conn =
        conn(:get, "/test")
        |> Handler.call(SuccessHandler)

      content_type = get_resp_header(conn, "content-type") |> List.first()
      assert content_type =~ "application/json"
    end
  end

  describe "extract_params/1" do
    test "extracts query params" do
      conn =
        conn(:get, "/test?foo=bar&baz=42")
        |> Plug.Conn.fetch_query_params()

      params = Handler.extract_params(conn)
      assert params["foo"] == "bar"
      assert params["baz"] == "42"
    end

    test "extracts path params" do
      conn =
        conn(:get, "/users/42")
        |> Map.put(:path_params, %{"id" => "42"})

      params = Handler.extract_params(conn)
      assert params["id"] == "42"
    end

    test "path params take precedence over query params" do
      conn =
        conn(:get, "/users/42?id=99")
        |> Map.put(:path_params, %{"id" => "42"})

      params = Handler.extract_params(conn)
      assert params["id"] == "42"
    end

    test "returns empty map when no params" do
      conn = conn(:get, "/test")
      params = Handler.extract_params(conn)
      assert is_map(params)
    end
  end
end
