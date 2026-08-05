defmodule Nucleus.Models.Document do
  @moduledoc """
  Document/JSON model — DOC_INSERT, DOC_GET, DOC_QUERY, DOC_PATH.

  ## Example

      alias Nucleus.Models.Document

      {:ok, doc_id} = Document.insert(client, %{name: "Alice", age: 30})
      {:ok, doc} = Document.get(client, doc_id)
      {:ok, name} = Document.path(client, doc_id, "name")

  ## Collections

  Every function has an `_in` counterpart taking a collection as its first
  argument after the client. A document belongs to exactly one collection and
  an operation naming one sees only that one — a document elsewhere reads as
  absent rather than raising, so an id cannot be used to probe across the
  boundary. The plain functions address the default (unnamed) collection,
  which is where documents written before collections existed live.

      {:ok, id} = Document.insert_in(client, "tenant_a", %{name: "Alice"})
      {:ok, nil} = Document.get_in(client, "tenant_b", id)
  """

  @type client :: Nucleus.Client.t()

  # Nucleus reports a parameter whose type it cannot infer as TEXT, and the
  # driver then refuses to bind an integer to it. The engine parses a
  # text-encoded integer id for exactly this reason, so sending the digits is
  # the supported encoding rather than a workaround.
  defp doc_id(id) when is_integer(id), do: Integer.to_string(id)
  defp doc_id(id) when is_binary(id), do: id

  @doc "Inserts a JSON document. Returns the document ID."
  @spec insert(client(), map() | String.t()) :: {:ok, integer()} | {:error, term()}
  def insert(client, document), do: insert_in(client, "", document)

  @doc "Inserts a JSON document into `collection`. Returns the document ID."
  @spec insert_in(client(), String.t(), map() | String.t()) ::
          {:ok, integer()} | {:error, term()}
  def insert_in(client, collection, document) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.insert") do
      json = if is_binary(document), do: document, else: Jason.encode!(document)

      # The one-argument form when no collection is named, so this still works
      # against a server that predates collections.
      {sql, params} =
        if collection == "",
          do: {"SELECT DOC_INSERT($1)", [json]},
          else: {"SELECT DOC_INSERT($1, $2)", [collection, json]}

      case Nucleus.Client.query(client, sql, params) do
        {:ok, %{rows: [[id]]}} -> {:ok, id}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Gets a document by ID. Returns parsed JSON."
  @spec get(client(), integer()) :: {:ok, map() | nil} | {:error, term()}
  def get(client, id), do: get_in_collection(client, "", id)

  @doc """
  Gets a document by ID from `collection`. A document in another collection
  reads as `nil`.
  """
  @spec get_in_collection(client(), String.t(), integer()) ::
          {:ok, map() | nil} | {:error, term()}
  def get_in_collection(client, collection, id) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.get") do
      {sql, params} =
        if collection == "",
          do: {"SELECT DOC_GET($1)", [doc_id(id)]},
          else: {"SELECT DOC_GET($1, $2)", [collection, doc_id(id)]}

      case Nucleus.Client.query(client, sql, params) do
        {:ok, %{rows: [[nil]]}} -> {:ok, nil}
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:ok, %{rows: [[map]]}} when is_map(map) -> {:ok, map}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Replaces a document by ID. Returns `{:ok, true}` if the document existed."
  @spec update(client(), integer(), map() | String.t()) :: {:ok, boolean()} | {:error, term()}
  def update(client, id, document), do: update_in(client, "", id, document)

  @doc """
  Replaces a document by ID within `collection`. Returns `{:ok, false}` — not an
  error — when the document belongs to a different collection, so one
  collection can never overwrite another's document.
  """
  @spec update_in(client(), String.t(), integer(), map() | String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def update_in(client, collection, id, document) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.update") do
      json = if is_binary(document), do: document, else: Jason.encode!(document)

      {sql, params} =
        if collection == "",
          do: {"SELECT DOC_UPDATE($1, $2)", [doc_id(id), json]},
          else: {"SELECT DOC_UPDATE($1, $2, $3)", [collection, doc_id(id), json]}

      case Nucleus.Client.query(client, sql, params) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Deletes a document by ID. Returns `{:ok, true}` if the document existed."
  @spec delete(client(), integer()) :: {:ok, boolean()} | {:error, term()}
  def delete(client, id), do: delete_in(client, "", id)

  @doc "Deletes a document by ID from `collection`."
  @spec delete_in(client(), String.t(), integer()) :: {:ok, boolean()} | {:error, term()}
  def delete_in(client, collection, id) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.delete") do
      {sql, params} =
        if collection == "",
          do: {"SELECT DOC_DELETE($1)", [doc_id(id)]},
          else: {"SELECT DOC_DELETE($1, $2)", [collection, doc_id(id)]}

      case Nucleus.Client.query(client, sql, params) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Queries documents by a JSON query expression. Returns matching document IDs."
  @spec query(client(), map() | String.t()) :: {:ok, [integer()]} | {:error, term()}
  def query(client, query_expr), do: query_in(client, "", query_expr)

  @doc "Queries one collection. Matches in other collections are not returned."
  @spec query_in(client(), String.t(), map() | String.t()) ::
          {:ok, [integer()]} | {:error, term()}
  def query_in(client, collection, query_expr) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.query") do
      json = if is_binary(query_expr), do: query_expr, else: Jason.encode!(query_expr)

      {sql, params} =
        if collection == "",
          do: {"SELECT DOC_QUERY($1)", [json]},
          else: {"SELECT DOC_QUERY($1, $2)", [collection, json]}

      case Nucleus.Client.query(client, sql, params) do
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
  def path(client, id, key), do: path_in(client, "", id, key)

  @doc """
  Extracts a value at a path from a document in `collection`.

  The scoped form is a distinct FUNCTION (`DOC_PATH_IN`) rather than an extra
  argument: the key tail is variadic, so a leading collection could not be told
  apart from a leading id.
  """
  @spec path_in(client(), String.t(), integer(), String.t()) ::
          {:ok, term()} | {:error, term()}
  def path_in(client, collection, id, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.path") do
      {sql, params} =
        if collection == "",
          do: {"SELECT DOC_PATH($1, $2)", [doc_id(id), key]},
          else: {"SELECT DOC_PATH_IN($1, $2, $3)", [collection, doc_id(id), key]}

      case Nucleus.Client.query(client, sql, params) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the total number of documents."
  @spec count(client()) :: {:ok, integer()} | {:error, term()}
  def count(client), do: count_in(client, "")

  @doc "Returns the number of documents in `collection`."
  @spec count_in(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def count_in(client, collection) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.count") do
      {sql, params} =
        if collection == "",
          do: {"SELECT DOC_COUNT()", []},
          else: {"SELECT DOC_COUNT($1)", [collection]}

      case Nucleus.Client.query(client, sql, params) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
