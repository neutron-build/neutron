defmodule Nucleus.Models.Vector do
  @moduledoc """
  Vector similarity search model.

  Provides vector storage, indexing, and nearest-neighbor search using
  Nucleus SQL functions: `VECTOR()`, `VECTOR_DISTANCE()`, `VECTOR_DIMS()`.

  ## Example

      alias Nucleus.Models.Vector

      # Create a collection
      Vector.create_collection(client, "embeddings", 384, :cosine)

      # Insert vectors
      Vector.insert(client, "embeddings", "doc1", [0.1, 0.2, 0.3, ...], %{title: "Hello"})

      # Search
      {:ok, results} = Vector.search(client, "embeddings", query_vec, limit: 10, metric: :cosine)
  """

  @type client :: Nucleus.Client.t()
  @type metric :: :l2 | :cosine | :inner

  @identifier_regex ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/

  @doc """
  Creates a vector collection (table + index).
  """
  @spec create_collection(client(), String.t(), pos_integer(), metric()) ::
          :ok | {:error, term()}
  def create_collection(client, name, dimension, metric \\ :cosine) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Vector.create_collection"),
         :ok <- validate_identifier(name) do
      create_sql =
        "CREATE TABLE IF NOT EXISTS #{name} (id TEXT PRIMARY KEY, embedding VECTOR(#{dimension}), metadata JSONB DEFAULT '{}')"

      index_sql =
        "CREATE INDEX IF NOT EXISTS idx_#{name}_embedding ON #{name} USING VECTOR (embedding) WITH (metric = '#{metric_string(metric)}')"

      with {:ok, _} <- Nucleus.Client.query(client, create_sql),
           {:ok, _} <- Nucleus.Client.query(client, index_sql) do
        :ok
      end
    end
  end

  @doc """
  Inserts a vector with metadata into a collection.
  """
  @spec insert(client(), String.t(), String.t(), [float()], map()) :: :ok | {:error, term()}
  def insert(client, collection, id, vector, metadata \\ %{}) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Vector.insert"),
         :ok <- validate_identifier(collection) do
      vec_json = Jason.encode!(vector)
      meta_json = Jason.encode!(metadata)

      sql =
        "INSERT INTO #{collection} (id, embedding, metadata) VALUES ($1, VECTOR($2), $3)"

      case Nucleus.Client.query(client, sql, [id, vec_json, meta_json]) do
        {:ok, _} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Performs a vector similarity search.

  ## Options

    * `:limit` — max results (default: 10)
    * `:metric` — distance metric: `:l2`, `:cosine`, `:inner` (default: `:cosine`)
    * `:filter` — metadata filter map (e.g., `%{"category" => "science"}`)
  """
  @spec search(client(), String.t(), [float()], keyword()) ::
          {:ok, [%{id: String.t(), distance: float(), metadata: map()}]} | {:error, term()}
  def search(client, collection, query_vector, opts \\ []) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Vector.search"),
         :ok <- validate_identifier(collection) do
      limit = Keyword.get(opts, :limit, 10)
      metric = Keyword.get(opts, :metric, :cosine)
      filter = Keyword.get(opts, :filter, %{})

      vec_json = Jason.encode!(query_vector)

      {where_clause, filter_params, param_offset} = build_filter(filter, 3)

      sql =
        "SELECT id, metadata, VECTOR_DISTANCE(embedding, VECTOR($1), $2) AS distance FROM #{collection}#{where_clause} ORDER BY distance LIMIT $#{param_offset}"

      params = [vec_json, metric_string(metric)] ++ filter_params ++ [limit]

      case Nucleus.Client.query(client, sql, params) do
        {:ok, %{rows: rows}} ->
          results =
            Enum.map(rows, fn [id, metadata, distance] ->
              meta =
                case metadata do
                  m when is_binary(m) -> Jason.decode!(m)
                  m when is_map(m) -> m
                  _ -> %{}
                end

              %{id: id, distance: distance, metadata: meta}
            end)

          {:ok, results}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Deletes a vector by ID from a collection."
  @spec delete(client(), String.t(), String.t()) :: :ok | {:error, term()}
  def delete(client, collection, id) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Vector.delete"),
         :ok <- validate_identifier(collection) do
      sql = "DELETE FROM #{collection} WHERE id = $1"

      case Nucleus.Client.query(client, sql, [id]) do
        {:ok, _} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the dimensionality of a vector."
  @spec dims(client(), [float()]) :: {:ok, integer()} | {:error, term()}
  def dims(client, vector) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Vector.dims") do
      vec_json = Jason.encode!(vector)

      case Nucleus.Client.query(client, "SELECT VECTOR_DIMS(VECTOR($1))", [vec_json]) do
        {:ok, %{rows: [[n]]}} -> {:ok, n}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Computes the distance between two vectors."
  @spec distance(client(), [float()], [float()], metric()) ::
          {:ok, float()} | {:error, term()}
  def distance(client, a, b, metric \\ :l2) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Vector.distance") do
      a_json = Jason.encode!(a)
      b_json = Jason.encode!(b)

      case Nucleus.Client.query(
             client,
             "SELECT VECTOR_DISTANCE(VECTOR($1), VECTOR($2), $3)",
             [a_json, b_json, metric_string(metric)]
           ) do
        {:ok, %{rows: [[d]]}} -> {:ok, d}
        {:error, _} = error -> error
      end
    end
  end

  # --- Internal ---

  defp metric_string(:l2), do: "l2"
  defp metric_string(:cosine), do: "cosine"
  defp metric_string(:inner), do: "inner"
  defp metric_string(other) when is_binary(other), do: other

  defp validate_identifier(name) do
    if Regex.match?(@identifier_regex, name) do
      :ok
    else
      {:error, Neutron.Error.bad_request("Invalid identifier: #{name}")}
    end
  end

  defp build_filter(filter, start_idx) when filter == %{} do
    {"", [], start_idx}
  end

  defp build_filter(filter, start_idx) do
    {clauses, params, idx} =
      Enum.reduce(filter, {[], [], start_idx}, fn {key, value}, {c, p, i} ->
        {["metadata->>$#{i} = $#{i + 1}" | c], p ++ [key, to_string(value)], i + 2}
      end)

    where = " WHERE " <> (Enum.reverse(clauses) |> Enum.join(" AND "))
    {where, params, idx}
  end
end
