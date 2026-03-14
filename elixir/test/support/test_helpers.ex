defmodule Neutron.TestHelpers do
  @moduledoc """
  Shared test helpers for Neutron test suite.
  """

  @doc """
  Creates a fresh ETS table for testing, returns the table reference.
  Deletes the table if it already exists.
  """
  def ensure_ets_table(name, opts \\ [:set, :public, :named_table]) do
    try do
      :ets.delete(name)
    rescue
      ArgumentError -> :ok
    end

    :ets.new(name, opts)
  end

  @doc """
  Creates a test Plug.Conn with JSON body.
  """
  def conn_with_json(method, path, body) when is_map(body) do
    Plug.Test.conn(method, path, Jason.encode!(body))
    |> Plug.Conn.put_req_header("content-type", "application/json")
  end

  @doc """
  Creates a test Plug.Conn with a Bearer token.
  """
  def conn_with_bearer(method, path, token) do
    Plug.Test.conn(method, path)
    |> Plug.Conn.put_req_header("authorization", "Bearer #{token}")
  end

  @doc """
  Creates a test Plug.Conn with a session cookie.
  """
  def conn_with_session_cookie(method, path, session_id, cookie_name \\ "_neutron_session") do
    Plug.Test.conn(method, path)
    |> Plug.Conn.put_req_header("cookie", "#{cookie_name}=#{session_id}")
  end

  @doc """
  A test secret key for JWT tests.
  """
  def test_secret, do: "test-secret-key-that-is-at-least-32-bytes-long!!"

  @doc """
  Signs a JWT token with the test secret.
  """
  def sign_test_token(claims, opts \\ []) do
    opts = Keyword.put_new(opts, :secret, test_secret())
    Neutron.Auth.JWT.sign(claims, opts)
  end
end

defmodule Neutron.TestWorker do
  @moduledoc "A test job worker that succeeds."
  use Neutron.Jobs.Worker

  @impl true
  def perform(_args) do
    :ok
  end
end

defmodule Neutron.FailingTestWorker do
  @moduledoc "A test job worker that always fails."
  use Neutron.Jobs.Worker

  @impl true
  def perform(_args) do
    raise "intentional failure"
  end
end

defmodule Neutron.SlowTestWorker do
  @moduledoc "A test job worker that takes time."
  use Neutron.Jobs.Worker

  @impl true
  def perform(%{sleep_ms: ms}) do
    Process.sleep(ms)
    :ok
  end
end
