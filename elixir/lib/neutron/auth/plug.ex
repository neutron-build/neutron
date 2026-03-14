defmodule Neutron.Auth.Plug do
  @moduledoc """
  Authentication plugs for protecting routes.

  ## Bearer Token Auth

      # In your router or pipeline:
      plug Neutron.Auth.Plug, type: :bearer

      # Access the verified claims:
      conn.assigns[:current_user]  # => %{"user_id" => 42, ...}

  ## Session Auth

      plug Neutron.Auth.Plug, type: :session

  ## Optional Auth

      # Doesn't halt on missing token, just sets assigns to nil
      plug Neutron.Auth.Plug, type: :bearer, required: false
  """

  @behaviour Plug

  @impl true
  def init(opts) do
    %{
      type: Keyword.get(opts, :type, :bearer),
      required: Keyword.get(opts, :required, true),
      secret: Keyword.get(opts, :secret),
      header: Keyword.get(opts, :header, "authorization"),
      session_cookie: Keyword.get(opts, :session_cookie, "_neutron_session")
    }
  end

  @impl true
  def call(conn, %{type: :bearer} = opts) do
    case extract_bearer_token(conn, opts.header) do
      {:ok, token} ->
        verify_opts = if opts.secret, do: [secret: opts.secret], else: []

        case Neutron.Auth.JWT.verify(token, verify_opts) do
          {:ok, claims} ->
            conn
            |> Plug.Conn.assign(:current_user, claims)
            |> Plug.Conn.assign(:auth_token, token)

          {:error, reason} ->
            if opts.required do
              Neutron.Error.send_error(
                conn,
                Neutron.Error.unauthorized("Invalid token: #{reason}")
              )
            else
              Plug.Conn.assign(conn, :current_user, nil)
            end
        end

      {:error, :missing} ->
        if opts.required do
          Neutron.Error.send_error(
            conn,
            Neutron.Error.unauthorized("Bearer token required")
          )
        else
          Plug.Conn.assign(conn, :current_user, nil)
        end
    end
  end

  def call(conn, %{type: :session} = opts) do
    conn = Plug.Conn.fetch_cookies(conn)

    case Map.get(conn.cookies, opts.session_cookie) do
      nil ->
        if opts.required do
          Neutron.Error.send_error(
            conn,
            Neutron.Error.unauthorized("Session required")
          )
        else
          Plug.Conn.assign(conn, :current_user, nil)
        end

      session_id ->
        case Neutron.Auth.Session.get(session_id) do
          {:ok, data} ->
            conn
            |> Plug.Conn.assign(:current_user, data)
            |> Plug.Conn.assign(:session_id, session_id)

          {:error, _reason} ->
            if opts.required do
              Neutron.Error.send_error(
                conn,
                Neutron.Error.unauthorized("Invalid or expired session")
              )
            else
              Plug.Conn.assign(conn, :current_user, nil)
            end
        end
    end
  end

  @doc """
  Requires that `current_user` is set in assigns. Halts with 401 if not.

  Use after an auth plug with `required: false` to selectively require auth.
  """
  @spec require_auth(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def require_auth(conn, _opts \\ []) do
    case conn.assigns[:current_user] do
      nil ->
        Neutron.Error.send_error(
          conn,
          Neutron.Error.unauthorized("Authentication required")
        )

      _ ->
        conn
    end
  end

  @doc """
  Requires that `current_user` has the given role.
  """
  @spec require_role(Plug.Conn.t(), String.t() | [String.t()]) :: Plug.Conn.t()
  def require_role(conn, roles) when is_list(roles) do
    case conn.assigns[:current_user] do
      %{"role" => role} when role in roles -> conn
      %{role: role} when role in roles -> conn
      _ -> Neutron.Error.send_error(conn, Neutron.Error.forbidden("Insufficient permissions"))
    end
  end

  def require_role(conn, role), do: require_role(conn, [role])

  # --- Internal ---

  defp extract_bearer_token(conn, header) do
    case Plug.Conn.get_req_header(conn, header) do
      ["Bearer " <> token | _] -> {:ok, String.trim(token)}
      ["bearer " <> token | _] -> {:ok, String.trim(token)}
      _ -> {:error, :missing}
    end
  end
end
