defmodule Nucleus.Models.Graph do
  @moduledoc """
  Graph model — GRAPH_ADD_NODE, GRAPH_ADD_EDGE, GRAPH_QUERY, GRAPH_NEIGHBORS.

  ## Example

      alias Nucleus.Models.Graph

      {:ok, alice_id} = Graph.add_node(client, "Person", %{name: "Alice"})
      {:ok, bob_id} = Graph.add_node(client, "Person", %{name: "Bob"})
      {:ok, edge_id} = Graph.add_edge(client, alice_id, bob_id, "KNOWS", %{since: 2020})

      {:ok, neighbors} = Graph.neighbors(client, alice_id)
      {:ok, path} = Graph.shortest_path(client, alice_id, bob_id)
      {:ok, result} = Graph.query(client, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
  """

  @type client :: Nucleus.Client.t()

  @doc "Adds a node with a label and optional properties. Returns the node ID."
  @spec add_node(client(), String.t(), map()) :: {:ok, integer()} | {:error, term()}
  def add_node(client, label, properties \\ %{}) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Graph.add_node") do
      props_json = Jason.encode!(properties)

      case Nucleus.Client.query(client, "SELECT GRAPH_ADD_NODE($1, $2)", [label, props_json]) do
        {:ok, %{rows: [[id]]}} -> {:ok, id}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Adds an edge between two nodes. Returns the edge ID."
  @spec add_edge(client(), integer(), integer(), String.t(), map()) ::
          {:ok, integer()} | {:error, term()}
  def add_edge(client, from_id, to_id, edge_type, properties \\ %{}) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Graph.add_edge") do
      props_json = Jason.encode!(properties)

      case Nucleus.Client.query(client, "SELECT GRAPH_ADD_EDGE($1, $2, $3, $4)", [
             from_id,
             to_id,
             edge_type,
             props_json
           ]) do
        {:ok, %{rows: [[id]]}} -> {:ok, id}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Deletes a node by ID."
  @spec delete_node(client(), integer()) :: {:ok, boolean()} | {:error, term()}
  def delete_node(client, node_id) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Graph.delete_node") do
      case Nucleus.Client.query(client, "SELECT GRAPH_DELETE_NODE($1)", [node_id]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Deletes an edge by ID."
  @spec delete_edge(client(), integer()) :: {:ok, boolean()} | {:error, term()}
  def delete_edge(client, edge_id) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Graph.delete_edge") do
      case Nucleus.Client.query(client, "SELECT GRAPH_DELETE_EDGE($1)", [edge_id]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Executes a Cypher-style graph query. Returns parsed JSON result."
  @spec query(client(), String.t()) :: {:ok, map()} | {:error, term()}
  def query(client, cypher) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Graph.query") do
      case Nucleus.Client.query(client, "SELECT GRAPH_QUERY($1)", [cypher]) do
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:ok, %{rows: [[map]]}} when is_map(map) -> {:ok, map}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Returns the neighbors of a node.

  Direction: `:out` (default), `:in`, `:both`.
  """
  @spec neighbors(client(), integer(), atom()) :: {:ok, list()} | {:error, term()}
  def neighbors(client, node_id, direction \\ :out) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Graph.neighbors") do
      dir = Atom.to_string(direction)

      case Nucleus.Client.query(client, "SELECT GRAPH_NEIGHBORS($1, $2)", [node_id, dir]) do
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:ok, %{rows: [[list]]}} when is_list(list) -> {:ok, list}
        {:ok, %{rows: []}} -> {:ok, []}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Finds the shortest path between two nodes."
  @spec shortest_path(client(), integer(), integer()) :: {:ok, list()} | {:error, term()}
  def shortest_path(client, from_id, to_id) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Graph.shortest_path") do
      case Nucleus.Client.query(client, "SELECT GRAPH_SHORTEST_PATH($1, $2)", [from_id, to_id]) do
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:ok, %{rows: [[list]]}} when is_list(list) -> {:ok, list}
        {:ok, %{rows: []}} -> {:ok, []}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the total number of nodes."
  @spec node_count(client()) :: {:ok, integer()} | {:error, term()}
  def node_count(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Graph.node_count") do
      case Nucleus.Client.query(client, "SELECT GRAPH_NODE_COUNT()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the total number of edges."
  @spec edge_count(client()) :: {:ok, integer()} | {:error, term()}
  def edge_count(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Graph.edge_count") do
      case Nucleus.Client.query(client, "SELECT GRAPH_EDGE_COUNT()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
