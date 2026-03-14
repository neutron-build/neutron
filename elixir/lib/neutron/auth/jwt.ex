defmodule Neutron.Auth.JWT do
  @moduledoc """
  JWT token creation and verification using HMAC-SHA256.

  Provides a Guardian-style API for issuing and verifying JWT tokens,
  backed by the JOSE library for standards-compliant signing.

  ## Configuration

  Set `NEUTRON_SECRET_KEY` in your environment or pass the secret explicitly.

  ## Example

      # Sign a token
      {:ok, token} = Neutron.Auth.JWT.sign(%{user_id: 42, role: "admin"})

      # Verify a token
      {:ok, claims} = Neutron.Auth.JWT.verify(token)
      claims["user_id"]  # => 42

      # Sign with custom expiry (1 hour)
      {:ok, token} = Neutron.Auth.JWT.sign(%{user_id: 42}, ttl: 3600)
  """

  @default_ttl 86_400
  @algorithm "HS256"

  @type claims :: map()

  @doc """
  Signs a payload into a JWT token.

  ## Options

    * `:ttl` — time-to-live in seconds (default: 86400 = 24h)
    * `:secret` — signing secret (default: from NEUTRON_SECRET_KEY)
    * `:issuer` — token issuer (default: "neutron")
  """
  @spec sign(claims(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def sign(payload, opts \\ []) do
    secret = Keyword.get(opts, :secret, get_secret())
    ttl = Keyword.get(opts, :ttl, @default_ttl)
    issuer = Keyword.get(opts, :issuer, "neutron")

    now = System.system_time(:second)

    claims =
      payload
      |> Map.merge(%{
        "iat" => now,
        "exp" => now + ttl,
        "iss" => issuer,
        "jti" => generate_jti()
      })

    jwk = JOSE.JWK.from_oct(secret)
    jws = JOSE.JWS.from_map(%{"alg" => @algorithm})

    {_, token} =
      JOSE.JWT.from_map(claims)
      |> JOSE.JWT.sign(jwk, jws)
      |> JOSE.JWS.compact()

    {:ok, token}
  rescue
    e -> {:error, Exception.message(e)}
  end

  @doc """
  Verifies a JWT token and returns the decoded claims.

  Checks signature, expiration, and issuer.

  ## Options

    * `:secret` — verification secret (default: from NEUTRON_SECRET_KEY)
    * `:issuer` — expected issuer (default: "neutron")
  """
  @spec verify(String.t(), keyword()) :: {:ok, claims()} | {:error, atom() | String.t()}
  def verify(token, opts \\ []) do
    secret = Keyword.get(opts, :secret, get_secret())
    issuer = Keyword.get(opts, :issuer, "neutron")

    jwk = JOSE.JWK.from_oct(secret)

    case JOSE.JWT.verify_strict(jwk, [@algorithm], token) do
      {true, %JOSE.JWT{fields: claims}, _jws} ->
        with :ok <- check_expiration(claims),
             :ok <- check_issuer(claims, issuer) do
          {:ok, claims}
        end

      {false, _, _} ->
        {:error, :invalid_signature}

      {:error, _reason} ->
        {:error, :invalid_token}
    end
  rescue
    _ -> {:error, :invalid_token}
  end

  @doc """
  Decodes a JWT token without verifying the signature.

  Useful for inspecting token contents. Do not trust the claims without verification.
  """
  @spec peek(String.t()) :: {:ok, claims()} | {:error, term()}
  def peek(token) do
    case JOSE.JWT.peek_payload(token) do
      %JOSE.JWT{fields: claims} -> {:ok, claims}
      _ -> {:error, :invalid_token}
    end
  rescue
    _ -> {:error, :invalid_token}
  end

  @doc """
  Returns the remaining TTL of a token in seconds, or :expired.
  """
  @spec ttl(String.t(), keyword()) :: {:ok, non_neg_integer()} | {:error, :expired | term()}
  def ttl(token, opts \\ []) do
    case verify(token, opts) do
      {:ok, claims} ->
        exp = Map.get(claims, "exp", 0)
        remaining = exp - System.system_time(:second)

        if remaining > 0 do
          {:ok, remaining}
        else
          {:error, :expired}
        end

      error ->
        error
    end
  end

  # --- Internal ---

  defp check_expiration(claims) do
    case Map.get(claims, "exp") do
      nil ->
        :ok

      exp when is_number(exp) ->
        if exp > System.system_time(:second) do
          :ok
        else
          {:error, :token_expired}
        end

      _ ->
        :ok
    end
  end

  defp check_issuer(claims, expected) do
    case Map.get(claims, "iss") do
      nil -> :ok
      ^expected -> :ok
      _ -> {:error, :invalid_issuer}
    end
  end

  defp get_secret do
    Neutron.Config.load().secret_key
  end

  defp generate_jti do
    :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
  end
end
