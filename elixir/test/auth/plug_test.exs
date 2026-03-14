defmodule Neutron.Auth.PlugTest do
  use ExUnit.Case
  use Plug.Test

  alias Neutron.Auth.Plug, as: AuthPlug

  @secret "test-secret-key-that-is-at-least-32-bytes-long!!"

  setup do
    # Ensure the ETS table exists for sessions
    try do
      :ets.delete(:neutron_sessions)
    rescue
      ArgumentError -> :ok
    end

    :ets.new(:neutron_sessions, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ok
  end

  describe "init/1" do
    test "returns default options" do
      opts = AuthPlug.init([])
      assert opts.type == :bearer
      assert opts.required == true
      assert opts.header == "authorization"
      assert opts.session_cookie == "_neutron_session"
      assert opts.secret == nil
    end

    test "accepts custom options" do
      opts = AuthPlug.init(type: :session, required: false, secret: "mysecret")
      assert opts.type == :session
      assert opts.required == false
      assert opts.secret == "mysecret"
    end
  end

  describe "bearer token auth" do
    test "extracts and verifies valid bearer token" do
      {:ok, token} = Neutron.Auth.JWT.sign(%{user_id: 42, role: "admin"}, secret: @secret)
      opts = AuthPlug.init(type: :bearer, secret: @secret)

      conn =
        conn(:get, "/protected")
        |> put_req_header("authorization", "Bearer #{token}")
        |> AuthPlug.call(opts)

      refute conn.halted
      assert conn.assigns[:current_user]["user_id"] == 42 || conn.assigns[:current_user][:user_id] == 42
      assert conn.assigns[:auth_token] == token
    end

    test "handles case-insensitive 'bearer' prefix" do
      {:ok, token} = Neutron.Auth.JWT.sign(%{user_id: 42}, secret: @secret)
      opts = AuthPlug.init(type: :bearer, secret: @secret)

      conn =
        conn(:get, "/protected")
        |> put_req_header("authorization", "bearer #{token}")
        |> AuthPlug.call(opts)

      refute conn.halted
      assert conn.assigns[:current_user] != nil
    end

    test "rejects request without bearer token when required" do
      opts = AuthPlug.init(type: :bearer, secret: @secret, required: true)

      conn =
        conn(:get, "/protected")
        |> AuthPlug.call(opts)

      assert conn.halted
      assert conn.status == 401
    end

    test "rejects request with invalid token when required" do
      opts = AuthPlug.init(type: :bearer, secret: @secret, required: true)

      conn =
        conn(:get, "/protected")
        |> put_req_header("authorization", "Bearer invalid-token")
        |> AuthPlug.call(opts)

      assert conn.halted
      assert conn.status == 401
    end

    test "sets current_user to nil when token missing and not required" do
      opts = AuthPlug.init(type: :bearer, secret: @secret, required: false)

      conn =
        conn(:get, "/protected")
        |> AuthPlug.call(opts)

      refute conn.halted
      assert conn.assigns[:current_user] == nil
    end

    test "sets current_user to nil when token invalid and not required" do
      opts = AuthPlug.init(type: :bearer, secret: @secret, required: false)

      conn =
        conn(:get, "/protected")
        |> put_req_header("authorization", "Bearer invalid-token")
        |> AuthPlug.call(opts)

      refute conn.halted
      assert conn.assigns[:current_user] == nil
    end
  end

  describe "session auth" do
    test "extracts session from cookie and loads data" do
      {:ok, session_id} = Neutron.Auth.Session.create(%{user_id: 42, name: "Alice"})
      opts = AuthPlug.init(type: :session, required: true)

      conn =
        conn(:get, "/protected")
        |> put_req_header("cookie", "_neutron_session=#{session_id}")
        |> AuthPlug.call(opts)

      refute conn.halted
      assert conn.assigns[:current_user] == %{user_id: 42, name: "Alice"}
      assert conn.assigns[:session_id] == session_id
    end

    test "rejects when session cookie is missing and required" do
      opts = AuthPlug.init(type: :session, required: true)

      conn =
        conn(:get, "/protected")
        |> AuthPlug.call(opts)

      assert conn.halted
      assert conn.status == 401
    end

    test "rejects when session is expired and required" do
      {:ok, session_id} = Neutron.Auth.Session.create(%{user_id: 42}, ttl: -1)
      opts = AuthPlug.init(type: :session, required: true)

      conn =
        conn(:get, "/protected")
        |> put_req_header("cookie", "_neutron_session=#{session_id}")
        |> AuthPlug.call(opts)

      assert conn.halted
      assert conn.status == 401
    end

    test "sets current_user to nil when session missing and not required" do
      opts = AuthPlug.init(type: :session, required: false)

      conn =
        conn(:get, "/protected")
        |> AuthPlug.call(opts)

      refute conn.halted
      assert conn.assigns[:current_user] == nil
    end

    test "uses custom session cookie name" do
      {:ok, session_id} = Neutron.Auth.Session.create(%{user_id: 42})
      opts = AuthPlug.init(type: :session, session_cookie: "my_session")

      conn =
        conn(:get, "/protected")
        |> put_req_header("cookie", "my_session=#{session_id}")
        |> AuthPlug.call(opts)

      refute conn.halted
      assert conn.assigns[:current_user] == %{user_id: 42}
    end
  end

  describe "require_auth/2" do
    test "passes when current_user is set" do
      conn =
        conn(:get, "/")
        |> Plug.Conn.assign(:current_user, %{"user_id" => 42})
        |> AuthPlug.require_auth()

      refute conn.halted
    end

    test "halts with 401 when current_user is nil" do
      conn =
        conn(:get, "/")
        |> Plug.Conn.assign(:current_user, nil)
        |> AuthPlug.require_auth()

      assert conn.halted
      assert conn.status == 401
    end

    test "halts with 401 when current_user not in assigns" do
      conn =
        conn(:get, "/")
        |> AuthPlug.require_auth()

      assert conn.halted
      assert conn.status == 401
    end
  end

  describe "require_role/2" do
    test "passes when user has the required role (string key)" do
      conn =
        conn(:get, "/")
        |> Plug.Conn.assign(:current_user, %{"role" => "admin"})
        |> AuthPlug.require_role("admin")

      refute conn.halted
    end

    test "passes when user has the required role (atom key)" do
      conn =
        conn(:get, "/")
        |> Plug.Conn.assign(:current_user, %{role: "editor"})
        |> AuthPlug.require_role("editor")

      refute conn.halted
    end

    test "passes when user has one of the required roles" do
      conn =
        conn(:get, "/")
        |> Plug.Conn.assign(:current_user, %{"role" => "editor"})
        |> AuthPlug.require_role(["admin", "editor"])

      refute conn.halted
    end

    test "rejects when user lacks the required role" do
      conn =
        conn(:get, "/")
        |> Plug.Conn.assign(:current_user, %{"role" => "viewer"})
        |> AuthPlug.require_role("admin")

      assert conn.halted
      assert conn.status == 403
    end

    test "rejects when user has no role" do
      conn =
        conn(:get, "/")
        |> Plug.Conn.assign(:current_user, %{"user_id" => 42})
        |> AuthPlug.require_role("admin")

      assert conn.halted
      assert conn.status == 403
    end
  end
end
