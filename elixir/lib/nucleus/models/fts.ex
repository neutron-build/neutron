defmodule Nucleus.Models.FTS do
  @moduledoc """
  Full-text search model — FTS_INDEX, FTS_SEARCH, FTS_FUZZY_SEARCH.

  ## Example

      alias Nucleus.Models.FTS

      FTS.index(client, 1, "The quick brown fox")
      FTS.index(client, 2, "The lazy dog")

      {:ok, results} = FTS.search(client, "quick fox", 10)
      {:ok, fuzzy} = FTS.fuzzy_search(client, "quik", 2, 10)
  """

  @type client :: Nucleus.Client.t()

  @doc "Indexes text content for a document ID."
  @spec index(client(), integer(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def index(client, doc_id, text) do
    with :ok <- Nucleus.Client.require_nucleus(client, "FTS.index") do
      case Nucleus.Client.query(client, "SELECT FTS_INDEX($1, $2)", [doc_id, text]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Searches indexed text. Returns a list of `%{doc_id: id, score: score}` results.
  """
  @spec search(client(), String.t(), integer()) ::
          {:ok, [%{doc_id: integer(), score: float()}]} | {:error, term()}
  def search(client, query, limit \\ 10) do
    with :ok <- Nucleus.Client.require_nucleus(client, "FTS.search") do
      case Nucleus.Client.query(client, "SELECT FTS_SEARCH($1, $2)", [query, limit]) do
        {:ok, %{rows: [[json]]}} when is_binary(json) ->
          {:ok, Jason.decode!(json) |> normalize_results()}

        {:ok, _} ->
          {:ok, []}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc """
  Fuzzy search with a maximum edit distance.
  """
  @spec fuzzy_search(client(), String.t(), integer(), integer()) ::
          {:ok, [%{doc_id: integer(), score: float()}]} | {:error, term()}
  def fuzzy_search(client, query, max_distance, limit \\ 10) do
    with :ok <- Nucleus.Client.require_nucleus(client, "FTS.fuzzy_search") do
      case Nucleus.Client.query(client, "SELECT FTS_FUZZY_SEARCH($1, $2, $3)", [
             query,
             max_distance,
             limit
           ]) do
        {:ok, %{rows: [[json]]}} when is_binary(json) ->
          {:ok, Jason.decode!(json) |> normalize_results()}

        {:ok, _} ->
          {:ok, []}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Removes a document from the search index."
  @spec remove(client(), integer()) :: {:ok, boolean()} | {:error, term()}
  def remove(client, doc_id) do
    with :ok <- Nucleus.Client.require_nucleus(client, "FTS.remove") do
      case Nucleus.Client.query(client, "SELECT FTS_REMOVE($1)", [doc_id]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the total number of indexed documents."
  @spec doc_count(client()) :: {:ok, integer()} | {:error, term()}
  def doc_count(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "FTS.doc_count") do
      case Nucleus.Client.query(client, "SELECT FTS_DOC_COUNT()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the total number of indexed terms."
  @spec term_count(client()) :: {:ok, integer()} | {:error, term()}
  def term_count(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "FTS.term_count") do
      case Nucleus.Client.query(client, "SELECT FTS_TERM_COUNT()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  defp normalize_results(results) when is_list(results) do
    Enum.map(results, fn item ->
      %{
        doc_id: item["doc_id"] || item["id"],
        score: item["score"] || 0.0
      }
    end)
  end

  defp normalize_results(_), do: []
end
