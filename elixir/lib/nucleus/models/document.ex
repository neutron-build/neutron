defmodule Nucleus.Models.Document do
  @moduledoc """
  Document/JSON model — DOC_INSERT, DOC_GET, DOC_QUERY, DOC_PATH.

  ## Example

      alias Nucleus.Models.Document

      {:ok, doc_id} = Document.insert(client, %{name: "Alice", age: 30})
      {:ok, doc} = Document.get(client, doc_id)
      {:ok, name} = Document.path(client, doc_id, "name")
  """

  @type client :: Nucleus.Client.t()

  @doc "Inserts a JSON document. Returns the document ID."
  @spec insert(client(), map() | String.t()) :: {:ok, integer()} | {:error, term()}
  def insert(client, document) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.insert") do
      json = if is_binary(document), do: document, else: Jason.encode!(document)

      case Nucleus.Client.query(client, "SELECT DOC_INSERT($1)", [json]) do
        {:ok, %{rows: [[id]]}} -> {:ok, id}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Gets a document by ID. Returns parsed JSON."
  @spec get(client(), integer()) :: {:ok, map() | nil} | {:error, term()}
  def get(client, id) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.get") do
      case Nucleus.Client.query(client, "SELECT DOC_GET($1)", [id]) do
        {:ok, %{rows: [[nil]]}} -> {:ok, nil}
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:ok, %{rows: [[map]]}} when is_map(map) -> {:ok, map}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Queries documents by a JSON query expression. Returns matching document IDs."
  @spec query(client(), map() | String.t()) :: {:ok, [integer()]} | {:error, term()}
  def query(client, query_expr) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.query") do
      json = if is_binary(query_expr), do: query_expr, else: Jason.encode!(query_expr)

      case Nucleus.Client.query(client, "SELECT DOC_QUERY($1)", [json]) do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) and raw != "" ->
          ids =
            raw
            |> String.split(",")
            |> Enum.map(&String.to_integer(String.trim(&1)))

          {:ok, ids}

        {:ok, _} ->
          {:ok, []}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Extracts a value at a path from a document."
  @spec path(client(), integer(), String.t()) :: {:ok, term()} | {:error, term()}
  def path(client, id, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.path") do
      case Nucleus.Client.query(client, "SELECT DOC_PATH($1, $2)", [id, key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the total number of documents."
  @spec count(client()) :: {:ok, integer()} | {:error, term()}
  def count(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.count") do
      case Nucleus.Client.query(client, "SELECT DOC_COUNT()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
