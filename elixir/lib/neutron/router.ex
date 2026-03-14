defmodule Neutron.Router do
  @moduledoc """
  Macro-based router DSL built on Plug.

  Provides a Phoenix-style routing DSL with pattern matching, path parameters,
  and JSON response helpers.

  ## Example

      defmodule MyApp.Router do
        use Neutron.Router

        get "/" do
          json(conn, 200, %{message: "Welcome to Neutron"})
        end

        get "/users/:id" do
          user_id = conn.path_params["id"]
          json(conn, 200, %{id: user_id})
        end

        post "/users" do
          {:ok, body} = read_json(conn)
          json(conn, 201, %{created: body["name"]})
        end

        scope "/api/v1" do
          get "/items" do
            json(conn, 200, %{items: []})
          end

          get "/items/:id" do
            json(conn, 200, %{id: conn.path_params["id"]})
          end
        end
      end

  ## Path Parameters

  Use `:param` syntax in paths. Parameters are available in `conn.path_params`.

  ## Scopes

  Group routes under a common prefix with `scope/2`:

      scope "/api" do
        get "/users" do ... end    # matches /api/users
      end
  """

  defmacro __using__(_opts) do
    quote do
      use Plug.Router
      import Neutron.Router, only: [scope: 2]
      import Neutron.Router.Helpers

      plug(:match)
      plug(:dispatch)

      # Store route metadata for OpenAPI generation
      Module.register_attribute(__MODULE__, :neutron_routes, accumulate: true)

      @before_compile Neutron.Router
    end
  end

  @doc """
  Groups routes under a common path prefix.

  ## Example

      scope "/api/v1" do
        get "/users" do
          json(conn, 200, %{users: []})
        end
      end
  """
  defmacro scope(prefix, do: block) do
    quote do
      # Store the current scope prefix
      @neutron_scope_prefix (Module.get_attribute(__MODULE__, :neutron_scope_prefix, "") <>
                               unquote(prefix))

      unquote(rewrite_routes(block, prefix))

      # Reset scope prefix
      @neutron_scope_prefix Module.get_attribute(__MODULE__, :neutron_scope_prefix, "")
                            |> String.replace_trailing(unquote(prefix), "")
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      @doc false
      def __neutron_routes__, do: @neutron_routes

      # Catch-all 404
      match _ do
        Neutron.Error.send_error(
          conn,
          Neutron.Error.not_found("Route not found: #{conn.method} #{conn.request_path}")
        )
      end
    end
  end

  # Rewrites route macros inside a scope block to prepend the prefix
  defp rewrite_routes({:__block__, meta, statements}, prefix) do
    rewritten =
      Enum.map(statements, fn
        {method, m, [path | rest]} when method in [:get, :post, :put, :patch, :delete, :options] ->
          new_path =
            quote do
              unquote(prefix) <> unquote(path)
            end

          {method, m, [new_path | rest]}

        other ->
          other
      end)

    {:__block__, meta, rewritten}
  end

  defp rewrite_routes({method, m, [path | rest]}, prefix)
       when method in [:get, :post, :put, :patch, :delete, :options] do
    new_path =
      quote do
        unquote(prefix) <> unquote(path)
      end

    {method, m, [new_path | rest]}
  end

  defp rewrite_routes(other, _prefix), do: other
end

defmodule Neutron.Router.Helpers do
  @moduledoc """
  Helper functions available inside Neutron router blocks.
  """

  import Plug.Conn

  @doc """
  Sends a JSON response.

  ## Example

      json(conn, 200, %{message: "OK"})
  """
  @spec json(Plug.Conn.t(), non_neg_integer(), term()) :: Plug.Conn.t()
  def json(conn, status, data) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(data))
  end

  @doc """
  Reads and parses the JSON request body.

  Returns `{:ok, parsed_map}` or `{:error, reason}`.
  """
  @spec read_json(Plug.Conn.t()) :: {:ok, map()} | {:error, term()}
  def read_json(conn) do
    with {:ok, body, _conn} <- read_body(conn),
         {:ok, parsed} <- Jason.decode(body) do
      {:ok, parsed}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_json}
    end
  end

  @doc """
  Reads and parses the JSON request body, raising on failure.
  """
  @spec read_json!(Plug.Conn.t()) :: map()
  def read_json!(conn) do
    case read_json(conn) do
      {:ok, data} -> data
      {:error, reason} -> raise "Failed to parse JSON body: #{inspect(reason)}"
    end
  end

  @doc """
  Sends a plain text response.
  """
  @spec text(Plug.Conn.t(), non_neg_integer(), String.t()) :: Plug.Conn.t()
  def text(conn, status, body) do
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(status, body)
  end

  @doc """
  Sends an RFC 7807 error response.
  """
  @spec send_error(Plug.Conn.t(), Neutron.Error.t()) :: Plug.Conn.t()
  def send_error(conn, error) do
    Neutron.Error.send_error(conn, error)
  end

  @doc """
  Gets a path parameter from the connection, returning nil if not found.
  """
  @spec path_param(Plug.Conn.t(), String.t()) :: String.t() | nil
  def path_param(conn, key) do
    conn.path_params[key] || conn.params[key]
  end

  @doc """
  Gets a query parameter from the connection.
  """
  @spec query_param(Plug.Conn.t(), String.t(), term()) :: term()
  def query_param(conn, key, default \\ nil) do
    conn = Plug.Conn.fetch_query_params(conn)
    Map.get(conn.query_params, key, default)
  end

  @doc """
  Gets a request header value. Returns the first value or default.
  """
  @spec get_header(Plug.Conn.t(), String.t(), term()) :: String.t() | term()
  def get_header(conn, key, default \\ nil) do
    case Plug.Conn.get_req_header(conn, String.downcase(key)) do
      [value | _] -> value
      [] -> default
    end
  end
end
