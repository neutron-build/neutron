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

      case result do
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:ok, %{rows: [[list]]}} when is_list(list) -> {:ok, list}
        {:ok, _} -> {:ok, []}
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
