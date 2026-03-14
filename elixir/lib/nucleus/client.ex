defmodule Nucleus.Client do
  @moduledoc """
  Nucleus database client with Postgrex connection pool and feature detection.

  Connects to a PostgreSQL/Nucleus instance, auto-detects capabilities via
  `SELECT VERSION()`, and provides access to all 14 Nucleus data models.

  ## Starting

  The client is typically started as part of the Neutron application supervision tree:

      children = [
        {Nucleus.Client, url: "postgres://user:pass@localhost:5432/mydb"}
      ]

  Or started standalone:

      {:ok, client} = Nucleus.Client.start_link(url: "postgres://...")

  ## Feature Detection

  On connect, the client runs `SELECT VERSION()` and parses the result.
  If it contains "Nucleus", all 14 models are available. Otherwise, only
  standard SQL queries work.

      Nucleus.Client.is_nucleus?(client)    # => true/false
      Nucleus.Client.features(client)        # => %Nucleus.Features{...}

  ## Models

  Access each of the 14 Nucleus models through the client:

      Nucleus.Models.KV.get(client, "key")
      Nucleus.Models.Vector.search(client, "embeddings", query_vec)
      Nucleus.Models.Graph.add_node(client, "Person", %{name: "Alice"})
  """

  use GenServer
  require Logger

  @type t :: pid() | atom()

  defmodule Features do
    @moduledoc "Detected Nucleus capabilities."
    defstruct [
      :version,
      is_nucleus: false,
      has_kv: false,
      has_vector: false,
      has_ts: false,
      has_document: false,
      has_graph: false,
      has_fts: false,
      has_geo: false,
      has_blob: false,
      has_streams: false,
      has_columnar: false,
      has_datalog: false,
      has_cdc: false,
      has_pubsub: false
    ]

    @type t :: %__MODULE__{
            version: String.t() | nil,
            is_nucleus: boolean(),
            has_kv: boolean(),
            has_vector: boolean(),
            has_ts: boolean(),
            has_document: boolean(),
            has_graph: boolean(),
            has_fts: boolean(),
            has_geo: boolean(),
            has_blob: boolean(),
            has_streams: boolean(),
            has_columnar: boolean(),
            has_datalog: boolean(),
            has_cdc: boolean(),
            has_pubsub: boolean()
          }
  end

  # --- Client API ---

  @doc """
  Starts the Nucleus client.

  ## Options

    * `:url` — PostgreSQL connection URL (required)
    * `:name` — process name (default: `Nucleus.Client`)
    * `:pool_size` — connection pool size (default: 10)
    * `:queue_target` — target queue time in ms (default: 50)
    * `:queue_interval` — queue check interval in ms (default: 1000)
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Returns a child spec for supervision trees."
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  @doc "Executes a raw SQL query, returning `{:ok, %Postgrex.Result{}}` or `{:error, term()}`."
  @spec query(t(), String.t(), list()) :: {:ok, Postgrex.Result.t()} | {:error, term()}
  def query(client, sql, params \\ []) do
    GenServer.call(client, {:query, sql, params})
  end

  @doc "Executes a raw SQL query, raising on error."
  @spec query!(t(), String.t(), list()) :: Postgrex.Result.t()
  def query!(client, sql, params \\ []) do
    case query(client, sql, params) do
      {:ok, result} -> result
      {:error, error} -> raise "Nucleus query failed: #{inspect(error)}"
    end
  end

  @doc "Returns true if the connected database is Nucleus."
  @spec is_nucleus?(t()) :: boolean()
  def is_nucleus?(client) do
    GenServer.call(client, :is_nucleus?)
  end

  @doc "Returns the detected features."
  @spec features(t()) :: Features.t()
  def features(client) do
    GenServer.call(client, :features)
  end

  @doc "Pings the database to verify connectivity."
  @spec ping(t()) :: :ok | {:error, term()}
  def ping(client) do
    case query(client, "SELECT 1") do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc "Returns the underlying Postgrex connection pool pid."
  @spec pool(t()) :: pid()
  def pool(client) do
    GenServer.call(client, :pool)
  end

  @doc """
  Requires Nucleus for a given feature. Returns `:ok` or a Neutron.Error.

  Used internally by model modules to guard Nucleus-only operations.
  """
  @spec require_nucleus(t(), String.t()) :: :ok | {:error, Neutron.Error.t()}
  def require_nucleus(client, feature) do
    if is_nucleus?(client) do
      :ok
    else
      {:error, Neutron.Error.nucleus_required(feature)}
    end
  end

  # --- GenServer Implementation ---

  @impl true
  def init(opts) do
    url = Keyword.fetch!(opts, :url)
    pool_size = Keyword.get(opts, :pool_size, 10)

    postgrex_opts =
      parse_url(url)
      |> Keyword.merge(
        pool_size: pool_size,
        queue_target: Keyword.get(opts, :queue_target, 50),
        queue_interval: Keyword.get(opts, :queue_interval, 1000)
      )

    case Postgrex.start_link(postgrex_opts) do
      {:ok, conn} ->
        features = detect_features(conn)
        Logger.info("[Nucleus] Connected to #{features.version}")

        if features.is_nucleus do
          Logger.info("[Nucleus] Nucleus detected — all 14 models available")
        else
          Logger.info("[Nucleus] Plain PostgreSQL detected — SQL-only mode")
        end

        {:ok, %{conn: conn, features: features}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:query, sql, params}, _from, state) do
    result = Postgrex.query(state.conn, sql, params)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:is_nucleus?, _from, state) do
    {:reply, state.features.is_nucleus, state}
  end

  @impl true
  def handle_call(:features, _from, state) do
    {:reply, state.features, state}
  end

  @impl true
  def handle_call(:pool, _from, state) do
    {:reply, state.conn, state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.conn && Process.alive?(state.conn) do
      GenServer.stop(state.conn)
    end

    :ok
  rescue
    _ -> :ok
  end

  # --- Internal ---

  defp detect_features(conn) do
    case Postgrex.query(conn, "SELECT VERSION()", []) do
      {:ok, %{rows: [[version]]}} ->
        is_nucleus = String.contains?(version, "Nucleus")

        %Features{
          version: version,
          is_nucleus: is_nucleus,
          has_kv: is_nucleus,
          has_vector: is_nucleus,
          has_ts: is_nucleus,
          has_document: is_nucleus,
          has_graph: is_nucleus,
          has_fts: is_nucleus,
          has_geo: is_nucleus,
          has_blob: is_nucleus,
          has_streams: is_nucleus,
          has_columnar: is_nucleus,
          has_datalog: is_nucleus,
          has_cdc: is_nucleus,
          has_pubsub: is_nucleus
        }

      {:error, _} ->
        %Features{version: "unknown", is_nucleus: false}
    end
  end

  defp parse_url(url) do
    uri = URI.parse(url)
    userinfo = (uri.userinfo || "") |> String.split(":", parts: 2)
    username = Enum.at(userinfo, 0, "postgres")
    password = Enum.at(userinfo, 1)
    database = String.trim_leading(uri.path || "/postgres", "/")

    opts = [
      hostname: uri.host || "localhost",
      port: uri.port || 5432,
      username: username,
      database: database
    ]

    if password do
      Keyword.put(opts, :password, password)
    else
      opts
    end
  end
end
