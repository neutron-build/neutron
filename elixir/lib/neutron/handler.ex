defmodule Neutron.Handler do
  @moduledoc """
  Behaviour for request handlers with typed extraction.

  Handlers separate request handling logic from routing. Each handler implements
  a `handle/2` callback that receives the connection and extracted parameters,
  returning `{:ok, response}` or `{:error, reason}`.

  ## Example

      defmodule MyApp.Handlers.GetUser do
        use Neutron.Handler

        @impl true
        def handle(conn, params) do
          user_id = params["id"]

          case MyApp.Users.get(user_id) do
            {:ok, user} ->
              {:ok, %{status: 200, body: user}}

            {:error, :not_found} ->
              {:error, Neutron.Error.not_found("User \#{user_id} not found")}
          end
        end
      end

  ## Using in Router

      defmodule MyApp.Router do
        use Neutron.Router

        get "/users/:id" do
          Neutron.Handler.call(conn, MyApp.Handlers.GetUser)
        end
      end
  """

  @type response :: %{
          status: non_neg_integer(),
          body: term(),
          headers: [{String.t(), String.t()}]
        }

  @callback handle(Plug.Conn.t(), map()) ::
              {:ok, response()} | {:error, Neutron.Error.t()} | {:error, term()}

  defmacro __using__(_opts) do
    quote do
      @behaviour Neutron.Handler
    end
  end

  @doc """
  Calls a handler module, extracting parameters and sending the response.
  """
  @spec call(Plug.Conn.t(), module()) :: Plug.Conn.t()
  def call(conn, handler_module) do
    params = extract_params(conn)

    case handler_module.handle(conn, params) do
      {:ok, response} ->
        send_response(conn, response)

      {:error, %Neutron.Error{} = error} ->
        Neutron.Error.send_error(conn, error)

      {:error, reason} ->
        Neutron.Error.send_error(
          conn,
          Neutron.Error.internal("Handler error: #{inspect(reason)}")
        )
    end
  end

  @doc """
  Extracts all parameters from a connection into a unified map.

  Merges path params, query params, and body params (if JSON).
  Path params take precedence over query params, which take precedence over body.
  """
  @spec extract_params(Plug.Conn.t()) :: map()
  def extract_params(conn) do
    conn = Plug.Conn.fetch_query_params(conn)

    body_params =
      case Plug.Conn.get_req_header(conn, "content-type") do
        ["application/json" <> _ | _] ->
          case Plug.Conn.read_body(conn) do
            {:ok, body, _conn} ->
              case Jason.decode(body) do
                {:ok, parsed} when is_map(parsed) -> parsed
                _ -> %{}
              end

            _ ->
              %{}
          end

        _ ->
          %{}
      end

    body_params
    |> Map.merge(conn.query_params || %{})
    |> Map.merge(conn.path_params || %{})
  end

  defp send_response(conn, %{status: status, body: body} = response) do
    headers = Map.get(response, :headers, [])

    conn =
      Enum.reduce(headers, conn, fn {key, value}, acc ->
        Plug.Conn.put_resp_header(acc, key, value)
      end)

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, Jason.encode!(body))
  end
end
