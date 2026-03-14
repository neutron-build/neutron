defmodule Neutron.Auth.JWTTest do
  use ExUnit.Case, async: true

  alias Neutron.Auth.JWT

  @secret "test-secret-key-that-is-at-least-32-bytes-long!!"

  describe "sign/2" do
    test "returns {:ok, token} for valid payload" do
      assert {:ok, token} = JWT.sign(%{user_id: 42}, secret: @secret)
      assert is_binary(token)
      assert String.contains?(token, ".")
    end

    test "token has three dot-separated parts (header.payload.signature)" do
      {:ok, token} = JWT.sign(%{role: "admin"}, secret: @secret)
      parts = String.split(token, ".")
      assert length(parts) == 3
    end

    test "includes default issuer 'neutron'" do
      {:ok, token} = JWT.sign(%{user_id: 1}, secret: @secret)
      {:ok, claims} = JWT.verify(token, secret: @secret)
      assert claims["iss"] == "neutron"
    end

    test "includes iat and exp claims" do
      {:ok, token} = JWT.sign(%{user_id: 1}, secret: @secret)
      {:ok, claims} = JWT.verify(token, secret: @secret)
      assert is_integer(claims["iat"])
      assert is_integer(claims["exp"])
      assert claims["exp"] > claims["iat"]
    end

    test "includes jti claim" do
      {:ok, token} = JWT.sign(%{user_id: 1}, secret: @secret)
      {:ok, claims} = JWT.verify(token, secret: @secret)
      assert is_binary(claims["jti"])
      assert String.length(claims["jti"]) > 0
    end

    test "respects custom TTL" do
      {:ok, token} = JWT.sign(%{user_id: 1}, secret: @secret, ttl: 3600)
      {:ok, claims} = JWT.verify(token, secret: @secret)
      assert claims["exp"] - claims["iat"] == 3600
    end

    test "default TTL is 86400 seconds (24h)" do
      {:ok, token} = JWT.sign(%{user_id: 1}, secret: @secret)
      {:ok, claims} = JWT.verify(token, secret: @secret)
      assert claims["exp"] - claims["iat"] == 86_400
    end

    test "respects custom issuer" do
      {:ok, token} = JWT.sign(%{user_id: 1}, secret: @secret, issuer: "my-app")
      {:ok, claims} = JWT.verify(token, secret: @secret, issuer: "my-app")
      assert claims["iss"] == "my-app"
    end

    test "includes payload fields in claims" do
      {:ok, token} = JWT.sign(%{user_id: 42, role: "admin"}, secret: @secret)
      {:ok, claims} = JWT.verify(token, secret: @secret)
      assert claims["user_id"] == 42 || claims[:user_id] == 42
      assert claims["role"] == "admin" || claims[:role] == "admin"
    end

    test "generates unique jti for each token" do
      {:ok, token1} = JWT.sign(%{user_id: 1}, secret: @secret)
      {:ok, token2} = JWT.sign(%{user_id: 1}, secret: @secret)
      {:ok, claims1} = JWT.verify(token1, secret: @secret)
      {:ok, claims2} = JWT.verify(token2, secret: @secret)
      assert claims1["jti"] != claims2["jti"]
    end
  end

  describe "verify/2" do
    test "verifies a valid token" do
      {:ok, token} = JWT.sign(%{user_id: 42}, secret: @secret)
      assert {:ok, claims} = JWT.verify(token, secret: @secret)
      assert claims["user_id"] == 42 || claims[:user_id] == 42
    end

    test "rejects token with wrong secret" do
      {:ok, token} = JWT.sign(%{user_id: 42}, secret: @secret)
      assert {:error, _reason} = JWT.verify(token, secret: "wrong-secret-that-is-long-enough!!!")
    end

    test "rejects an expired token" do
      {:ok, token} = JWT.sign(%{user_id: 42}, secret: @secret, ttl: -1)
      assert {:error, :token_expired} = JWT.verify(token, secret: @secret)
    end

    test "rejects token with wrong issuer" do
      {:ok, token} = JWT.sign(%{user_id: 42}, secret: @secret, issuer: "app-a")
      assert {:error, :invalid_issuer} = JWT.verify(token, secret: @secret, issuer: "app-b")
    end

    test "rejects garbage input" do
      assert {:error, _reason} = JWT.verify("not-a-jwt-token", secret: @secret)
    end

    test "rejects empty string" do
      assert {:error, _reason} = JWT.verify("", secret: @secret)
    end

    test "rejects tampered token" do
      {:ok, token} = JWT.sign(%{user_id: 42}, secret: @secret)
      # Flip a character in the signature part
      [header, payload, sig] = String.split(token, ".")
      tampered_sig = String.reverse(sig)
      tampered = "#{header}.#{payload}.#{tampered_sig}"
      assert {:error, _reason} = JWT.verify(tampered, secret: @secret)
    end
  end

  describe "peek/1" do
    test "decodes claims without verification" do
      {:ok, token} = JWT.sign(%{user_id: 42, role: "admin"}, secret: @secret)
      assert {:ok, claims} = JWT.peek(token)
      assert claims["user_id"] == 42 || claims[:user_id] == 42
    end

    test "returns error for garbage input" do
      assert {:error, :invalid_token} = JWT.peek("not-valid")
    end
  end

  describe "ttl/2" do
    test "returns remaining TTL for valid token" do
      {:ok, token} = JWT.sign(%{user_id: 42}, secret: @secret, ttl: 3600)
      assert {:ok, remaining} = JWT.ttl(token, secret: @secret)
      assert remaining > 0
      assert remaining <= 3600
    end

    test "returns error for expired token" do
      {:ok, token} = JWT.sign(%{user_id: 42}, secret: @secret, ttl: -1)
      assert {:error, :token_expired} = JWT.ttl(token, secret: @secret)
    end

    test "returns error for invalid token" do
      assert {:error, _reason} = JWT.ttl("garbage", secret: @secret)
    end
  end
end
