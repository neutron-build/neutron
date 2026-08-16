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

  # ── Filter-based operations ────────────────────────────────────────────────
  #
  # The engine indexes documents but only DOC_QUERY takes a filter, and it
  # answers with ids. Update-by-filter, delete-by-filter and find therefore
  # resolve ids first and act per id, which is exactly what the Python client
  # does — the contract is defined by behaviour, not by where the loop runs.
  #
  # These did not exist until 2026-08-15. Their absence was invisible for as
  # long as Elixir could not connect to Nucleus at all; the live conformance
  # suite reported them the day it first ran.

  @doc """
  Finds documents in `collection` matching `filter`.

  `filter` is the same JSON query expression `query_in/3` takes. Returns whole
  documents rather than ids.
  """
  @spec find(client(), String.t(), map() | String.t(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def find(client, collection, filter, opts \\ []) do
    with {:ok, pairs} <- find_with_ids(client, collection, filter, opts) do
      {:ok, Enum.map(pairs, fn {_id, doc} -> doc end)}
    end
  end

  @doc """
  Finds the first document in `collection` matching `filter`, or `nil`.

  `{:ok, nil}` for no match — absence is not an error, matching `get_in_collection/3`.
  """
  @spec find_one(client(), String.t(), map() | String.t()) ::
          {:ok, map() | nil} | {:error, term()}
  def find_one(client, collection, filter) do
    with {:ok, pairs} <- find_with_ids(client, collection, filter, limit: 1) do
      case pairs do
        [{_id, doc} | _] -> {:ok, doc}
        [] -> {:ok, nil}
      end
    end
  end

  @doc """
  Merges `patch` into every document in `collection` matching `filter`.
  Returns the number updated.

  This is a PARTIAL update: fields in `patch` overwrite, fields absent from it
  survive. `update_in/4` replaces a whole document by id and is a different
  operation despite the shared name root.
  """
  @spec update_where(client(), String.t(), map() | String.t(), map()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def update_where(client, collection, filter, patch) do
    with {:ok, pairs} <- find_with_ids(client, collection, filter, limit: 10_000) do
      Enum.reduce_while(pairs, {:ok, 0}, fn {id, existing}, {:ok, n} ->
        merged = Map.merge(existing, stringify_keys(patch))

        case update_in(client, collection, id, merged) do
          {:ok, true} -> {:cont, {:ok, n + 1}}
          {:ok, _} -> {:cont, {:ok, n}}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  @doc """
  Deletes every document in `collection` matching `filter`. Returns the number
  deleted.
  """
  @spec delete_where(client(), String.t(), map() | String.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def delete_where(client, collection, filter) do
    with {:ok, pairs} <- find_with_ids(client, collection, filter, limit: 10_000) do
      Enum.reduce_while(pairs, {:ok, 0}, fn {id, _doc}, {:ok, n} ->
        case delete_in(client, collection, id) do
          {:ok, true} -> {:cont, {:ok, n + 1}}
          {:ok, _} -> {:cont, {:ok, n}}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  @doc """
  Extracts a value at a nested path. Accepts any number of keys.

  `path_in/4` takes exactly one key; the engine's DOC_PATH is variadic, so a
  nested path was unreachable from Elixir. Sending zero keys would build
  `DOC_PATH($1, )` — a malformed statement whose error names a syntax problem
  rather than the empty path that caused it — so it is refused here.
  """
  @spec path_all(client(), String.t(), integer(), [String.t()]) ::
          {:ok, term()} | {:error, term()}
  def path_all(_client, _collection, _id, []), do: {:error, :empty_path}

  def path_all(client, collection, id, keys) when is_list(keys) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Document.path") do
      # $1 (and $2 when scoped) are taken; the key tail starts after them.
      offset = if collection == "", do: 1, else: 2

      placeholders =
        keys
        |> Enum.with_index(offset + 1)
        |> Enum.map_join(", ", fn {_k, i} -> "$#{i}" end)

      {sql, params} =
        if collection == "",
          do:
            {"SELECT DOC_PATH($1, #{placeholders})", [doc_id(id) | keys]},
            else: {"SELECT DOC_PATH_IN($1, $2, #{placeholders})", [collection, doc_id(id) | keys]}

      case Nucleus.Client.query(client, sql, params) do
        {:ok, %{rows: [[val]]}} -> {:ok, decode_path_value(val)}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  # DOC_PATH hands back raw JSON, so a stored string arrives as ~s("ada").
  # S22 decided the cross-SDK contract: get_path returns the VALUE.
  defp decode_path_value(nil), do: nil

  defp decode_path_value(raw) when is_binary(raw) do
    case Jason.decode(raw) do
      {:ok, decoded} -> decoded
      {:error, _} -> raw
    end
  end

  defp decode_path_value(other), do: other

  # Atom keys in a patch would serialise fine but never match the string keys a
  # decoded document carries, so a merge would add a duplicate field rather
  # than overwrite.
  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {k, v}
    end)
  end

  defp find_with_ids(client, collection, filter, opts) do
    limit = Keyword.get(opts, :limit, 100)
    skip = Keyword.get(opts, :skip, 0)

    with {:ok, ids} <- query_in(client, collection, filter) do
      ids
      |> Enum.drop(skip)
      |> Enum.take(limit)
      |> Enum.reduce_while({:ok, []}, fn id, {:ok, acc} ->
        case get_in_collection(client, collection, id) do
          {:ok, nil} -> {:cont, {:ok, acc}}
          {:ok, doc} -> {:cont, {:ok, [{id, doc} | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        other -> other
      end
    end
  end

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
