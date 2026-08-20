defmodule Neutron.Contract.Rfc7807Test do
  # FRAMEWORK_CONTRACT §2: every error response is RFC 7807 —
  # application/problem+json with the required member set — same shape as the
  # other SDKs, not merely "some JSON".
  use ExUnit.Case, async: true
  import Plug.Test
  import Plug.Conn

  alias Neutron.Error

  describe "send_error/2 — the wire format" do
    test "responds with the application/problem+json content type" do
      conn =
        conn(:get, "/api/users/42")
        |> Error.send_error(Error.not_found("User 42 not found"))

      assert [content_type] = get_resp_header(conn, "content-type")
      assert content_type =~ "application/problem+json"
      assert conn.halted
    end

    test "body carries exactly the required members and omits nil optionals" do
      conn =
        conn(:get, "/")
        |> Error.send_error(Error.not_found("User 42 not found"))

      body = Jason.decode!(conn.resp_body)
      assert body["type"] == "https://neutron.dev/errors/not-found"
      assert body["title"] == "Not Found"
      assert body["status"] == 404
      assert body["detail"] == "User 42 not found"
      # instance and errors are optional; a nil value must not serialize
      # as JSON null (RFC 7807 §3.1).
      refute Map.has_key?(body, "instance")
      refute Map.has_key?(body, "errors")
    end

    test "instance is included when set" do
      conn =
        conn(:get, "/")
        |> Error.send_error(Error.not_found("gone") |> Error.with_instance("/api/users/42"))

      assert Jason.decode!(conn.resp_body)["instance"] == "/api/users/42"
    end

    test "validation errors carry the contract's field-level errors array" do
      errors = [
        %{field: "email", message: "must be a valid email address", value: "not-an-email"},
        %{field: "name", message: "is required"}
      ]

      conn =
        conn(:post, "/api/users")
        |> Error.send_error(Error.validation("Request body failed validation", errors))

      body = Jason.decode!(conn.resp_body)

      assert body["type"] == "https://neutron.dev/errors/validation"
      assert body["title"] == "Validation Failed"
      assert body["status"] == 422

      assert [
               %{
                 "field" => "email",
                 "message" => "must be a valid email address",
                 "value" => "not-an-email"
               },
               %{"field" => "name", "message" => "is required"}
             ] = body["errors"]
    end
  end

  describe "the contract's standard error codes table" do
    test "every row matches status, type URI suffix and title" do
      for {status, suffix, title, build} <- standard_codes() do
        error = build.()

        assert error.status == status
        assert error.type == "https://neutron.dev/errors/#{suffix}"
        assert error.title == title
      end
    end
  end

  defp standard_codes do
    [
      {400, "bad-request", "Bad Request", fn -> Error.bad_request("x") end},
      {401, "unauthorized", "Unauthorized", fn -> Error.unauthorized("x") end},
      {403, "forbidden", "Forbidden", fn -> Error.forbidden("x") end},
      {404, "not-found", "Not Found", fn -> Error.not_found("x") end},
      {409, "conflict", "Conflict", fn -> Error.conflict("x") end},
      {422, "validation", "Validation Failed", fn -> Error.validation("x", []) end},
      {429, "rate-limited", "Rate Limited", fn -> Error.rate_limited("x") end},
      {500, "internal", "Internal Server Error", fn -> Error.internal("x") end}
    ]
  end

  describe "framework surfaces emit RFC 7807, not plain JSON" do
    defmodule ApiRouter do
      use Neutron.Router

      get "/items" do
        send_error(conn, Error.forbidden("nope"))
      end
    end

    test "a handler error sent through the router keeps the problem+json shape" do
      conn = conn(:get, "/items") |> ApiRouter.call(ApiRouter.init([]))

      assert conn.status == 403
      assert [ct] = get_resp_header(conn, "content-type")
      assert ct =~ "application/problem+json"

      body = Jason.decode!(conn.resp_body)
      assert body["type"] == "https://neutron.dev/errors/forbidden"
      assert body["status"] == 403
    end
  end
end
