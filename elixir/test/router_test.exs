defmodule Neutron.RouterTest do
  use ExUnit.Case, async: true
  use Plug.Test

  defmodule TestRouter do
    use Neutron.Router

    get "/" do
      json(conn, 200, %{message: "hello"})
    end

    get "/users/:id" do
      user_id = conn.path_params["id"]
      json(conn, 200, %{id: user_id})
    end

    post "/users" do
      json(conn, 201, %{created: true})
    end

    get "/text" do
      text(conn, 200, "plain text response")
    end

    get "/error" do
      send_error(conn, Neutron.Error.not_found("Thing not found"))
    end

    get "/query" do
      name = query_param(conn, "name", "unknown")
      json(conn, 200, %{name: name})
    end
  end

  setup do
    {:ok, router: TestRouter}
  end

  describe "GET routes" do
    test "root path returns JSON", %{router: router} do
      conn = conn(:get, "/") |> router.call(router.init([]))

      assert conn.status == 200
      assert conn.resp_body |> Jason.decode!() == %{"message" => "hello"}
      assert get_resp_header(conn, "content-type") |> hd() =~ "application/json"
    end

    test "path parameters are extracted", %{router: router} do
      conn = conn(:get, "/users/42") |> router.call(router.init([]))

      assert conn.status == 200
      assert conn.resp_body |> Jason.decode!() == %{"id" => "42"}
    end

    test "text response", %{router: router} do
      conn = conn(:get, "/text") |> router.call(router.init([]))

      assert conn.status == 200
      assert conn.resp_body == "plain text response"
      assert get_resp_header(conn, "content-type") |> hd() =~ "text/plain"
    end

    test "query parameters", %{router: router} do
      conn = conn(:get, "/query?name=Alice") |> router.call(router.init([]))

      assert conn.status == 200
      assert conn.resp_body |> Jason.decode!() == %{"name" => "Alice"}
    end
  end

  describe "POST routes" do
    test "post returns 201", %{router: router} do
      conn =
        conn(:post, "/users", Jason.encode!(%{name: "Alice"}))
        |> put_req_header("content-type", "application/json")
        |> router.call(router.init([]))

      assert conn.status == 201
    end
  end

  describe "error handling" do
    test "send_error returns RFC 7807 format", %{router: router} do
      conn = conn(:get, "/error") |> router.call(router.init([]))

      assert conn.status == 404
      assert get_resp_header(conn, "content-type") |> hd() =~ "application/problem+json"

      body = Jason.decode!(conn.resp_body)
      assert body["type"] == "https://neutron.dev/errors/not-found"
      assert body["title"] == "Not Found"
      assert body["status"] == 404
      assert body["detail"] == "Thing not found"
    end

    test "unmatched route returns 404", %{router: router} do
      conn = conn(:get, "/nonexistent") |> router.call(router.init([]))

      assert conn.status == 404

      body = Jason.decode!(conn.resp_body)
      assert body["type"] == "https://neutron.dev/errors/not-found"
    end
  end
end
