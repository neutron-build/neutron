defmodule Neutron.ErrorTest do
  use ExUnit.Case, async: true

  alias Neutron.Error

  describe "error constructors" do
    test "bad_request/1" do
      err = Error.bad_request("invalid input")
      assert err.status == 400
      assert err.type == "https://neutron.dev/errors/bad-request"
      assert err.title == "Bad Request"
      assert err.detail == "invalid input"
      assert err.instance == nil
      assert err.errors == nil
    end

    test "unauthorized/1" do
      err = Error.unauthorized("token expired")
      assert err.status == 401
      assert err.type == "https://neutron.dev/errors/unauthorized"
      assert err.detail == "token expired"
    end

    test "unauthorized/0 with default" do
      err = Error.unauthorized()
      assert err.detail == "Authentication required"
    end

    test "forbidden/1" do
      err = Error.forbidden("admin only")
      assert err.status == 403
      assert err.type == "https://neutron.dev/errors/forbidden"
    end

    test "not_found/1" do
      err = Error.not_found("user 42 not found")
      assert err.status == 404
      assert err.type == "https://neutron.dev/errors/not-found"
      assert err.detail == "user 42 not found"
    end

    test "conflict/1" do
      err = Error.conflict("already exists")
      assert err.status == 409
      assert err.type == "https://neutron.dev/errors/conflict"
    end

    test "validation/2 with field errors" do
      errors = [
        %{field: "email", message: "must be a valid email", value: "not-email"},
        %{field: "name", message: "is required", value: nil}
      ]

      err = Error.validation("validation failed", errors)
      assert err.status == 422
      assert err.type == "https://neutron.dev/errors/validation"
      assert err.title == "Validation Failed"
      assert length(err.errors) == 2
    end

    test "rate_limited/1" do
      err = Error.rate_limited("slow down")
      assert err.status == 429
      assert err.type == "https://neutron.dev/errors/rate-limited"
    end

    test "internal/1" do
      err = Error.internal("something broke")
      assert err.status == 500
      assert err.type == "https://neutron.dev/errors/internal"
    end

    test "nucleus_required/1" do
      err = Error.nucleus_required("KV.get")
      assert err.status == 501
      assert err.type == "https://neutron.dev/errors/nucleus-required"
      assert err.detail =~ "KV.get"
      assert err.detail =~ "plain PostgreSQL"
    end
  end

  describe "with_instance/2" do
    test "sets the instance field" do
      err =
        Error.not_found("gone")
        |> Error.with_instance("/api/users/42")

      assert err.instance == "/api/users/42"
    end
  end

  describe "to_map/1" do
    test "includes required fields, excludes nil optional fields" do
      map =
        Error.not_found("gone")
        |> Error.to_map()

      assert Map.has_key?(map, :type)
      assert Map.has_key?(map, :title)
      assert Map.has_key?(map, :status)
      assert Map.has_key?(map, :detail)
      refute Map.has_key?(map, :instance)
      refute Map.has_key?(map, :errors)
    end

    test "includes instance when set" do
      map =
        Error.not_found("gone")
        |> Error.with_instance("/api/users/42")
        |> Error.to_map()

      assert map.instance == "/api/users/42"
    end

    test "includes errors for validation" do
      errors = [%{field: "name", message: "required", value: nil}]

      map =
        Error.validation("fail", errors)
        |> Error.to_map()

      assert length(map.errors) == 1
    end
  end

  describe "JSON encoding" do
    test "encodes to valid JSON" do
      err = Error.bad_request("test")
      {:ok, json} = Jason.encode(err)
      decoded = Jason.decode!(json)

      assert decoded["type"] == "https://neutron.dev/errors/bad-request"
      assert decoded["status"] == 400
    end
  end
end
