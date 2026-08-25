defmodule Nucleus.Models.Blob do
  @moduledoc """
  Blob storage model — BLOB_STORE, BLOB_GET, BLOB_DELETE, BLOB_META, BLOB_TAG.

  ## Example

      alias Nucleus.Models.Blob

      Blob.store(client, "avatar.png", hex_data, "image/png")
      {:ok, hex} = Blob.get(client, "avatar.png")
      {:ok, meta} = Blob.meta(client, "avatar.png")
      Blob.tag(client, "avatar.png", "user_id", "42")
  """

  @type client :: Nucleus.Client.t()

  # ── Buckets ────────────────────────────────────────────────────────────────
  #
  # The engine has no bucket dimension: BLOB_STORE and friends take one flat
  # key. Buckets are a client-side convention — "bucket/key" — and every SDK
  # implements them the same way, so a blob written from Python is readable from
  # Elixir and vice versa. Diverging on the separator would partition the
  # keyspace silently, which is why this is spelled out rather than inlined.
  #
  # The `_in` functions below are the bucket-scoped forms. The unscoped ones are
  # unchanged and address the flat keyspace directly.
  defp scoped(bucket, key) when is_binary(bucket) and bucket != "", do: "#{bucket}/#{key}"
  defp scoped(_bucket, key), do: key

  @doc "Stores binary data (hex-encoded) in `bucket`."
  @spec store_in(client(), String.t(), String.t(), String.t(), String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def store_in(client, bucket, key, data_hex, content_type \\ "application/octet-stream"),
    do: store(client, scoped(bucket, key), data_hex, content_type)

  @doc "Retrieves hex-encoded blob data from `bucket`."
  @spec get_in_bucket(client(), String.t(), String.t()) ::
          {:ok, String.t() | nil} | {:error, term()}
  def get_in_bucket(client, bucket, key), do: get(client, scoped(bucket, key))

  @doc "Deletes a blob from `bucket`."
  @spec delete_in(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def delete_in(client, bucket, key), do: delete(client, scoped(bucket, key))

  @doc "Returns metadata for a blob in `bucket`."
  @spec meta_in(client(), String.t(), String.t()) :: {:ok, map() | nil} | {:error, term()}
  def meta_in(client, bucket, key), do: meta(client, scoped(bucket, key))

  @doc """
  Whether a blob exists.

  There is no BLOB_EXISTS: this asks BLOB_META and reports whether it answered.
  Metadata rather than BLOB_GET on purpose — `get` would pull the whole payload
  across the wire to answer a boolean.
  """
  @spec exists(client(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def exists(client, key) do
    case meta(client, key) do
      {:ok, nil} -> {:ok, false}
      {:ok, _meta} -> {:ok, true}
      {:error, _} = error -> error
    end
  end

  @doc "Whether a blob exists in `bucket`."
  @spec exists_in(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def exists_in(client, bucket, key), do: exists(client, scoped(bucket, key))

  @doc "Stores binary data (hex-encoded)."
  @spec store(client(), String.t(), String.t(), String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def store(client, key, data_hex, content_type \\ "application/octet-stream") do
    with :ok <- Nucleus.Client.require_nucleus(client, "Blob.store") do
      case Nucleus.Client.query(client, "SELECT BLOB_STORE($1, $2, $3)", [
             key,
             data_hex,
             content_type
           ]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Retrieves hex-encoded blob data."
  @spec get(client(), String.t()) :: {:ok, String.t() | nil} | {:error, term()}
  def get(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Blob.get") do
      case Nucleus.Client.query(client, "SELECT BLOB_GET($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Deletes a blob."
  @spec delete(client(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def delete(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Blob.delete") do
      case Nucleus.Client.query(client, "SELECT BLOB_DELETE($1)", [key]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns metadata for a blob as parsed JSON."
  @spec meta(client(), String.t()) :: {:ok, map() | nil} | {:error, term()}
  def meta(client, key) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Blob.meta") do
      case Nucleus.Client.query(client, "SELECT BLOB_META($1)", [key]) do
        {:ok, %{rows: [[nil]]}} -> {:ok, nil}
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:ok, %{rows: [[map]]}} when is_map(map) -> {:ok, map}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Tags a blob with a key-value pair."
  @spec tag(client(), String.t(), String.t(), String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def tag(client, key, tag_key, tag_value) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Blob.tag") do
      case Nucleus.Client.query(client, "SELECT BLOB_TAG($1, $2, $3)", [key, tag_key, tag_value]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Lists blobs, optionally filtered by prefix."
  @spec list(client(), String.t() | nil) :: {:ok, list()} | {:error, term()}
  def list(client, prefix \\ nil) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Blob.list") do
      result =
        if prefix do
          Nucleus.Client.query(client, "SELECT BLOB_LIST($1)", [prefix])
        else
          Nucleus.Client.query(client, "SELECT BLOB_LIST()", [])
        end

      # BLOB_LIST's only Ok arm is Value::Text("[...]") — an empty store
      # answers "[]", never NULL — and the wire column is TEXT, so Postgrex
      # always delivers a binary; any other shape raises rather than
      # collapsing into {:ok, []}.
      case result do
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the total number of blobs."
  @spec count(client()) :: {:ok, integer()} | {:error, term()}
  def count(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Blob.count") do
      case Nucleus.Client.query(client, "SELECT BLOB_COUNT()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the deduplication ratio."
  @spec dedup_ratio(client()) :: {:ok, float()} | {:error, term()}
  def dedup_ratio(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Blob.dedup_ratio") do
      case Nucleus.Client.query(client, "SELECT BLOB_DEDUP_RATIO()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
