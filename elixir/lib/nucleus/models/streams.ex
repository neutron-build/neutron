defmodule Nucleus.Models.Streams do
  @moduledoc """
  Streams model (append-only logs) — STREAM_XADD, STREAM_XLEN, STREAM_XRANGE, etc.

  ## Example

      alias Nucleus.Models.Streams

      {:ok, entry_id} = Streams.xadd(client, "events", %{type: "click", url: "/home"})
      {:ok, 1} = Streams.xlen(client, "events")
      {:ok, entries} = Streams.xrange(client, "events", 0, :inf, 100)
  """

  @type client :: Nucleus.Client.t()

  @doc """
  Appends an entry to a stream. Returns the entry ID.

  Fields are provided as a map which is flattened to key-value pairs.
  """
  @spec xadd(client(), String.t(), map()) :: {:ok, String.t()} | {:error, term()}
  def xadd(client, stream, fields) when is_map(fields) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Streams.xadd") do
      # Flatten map to alternating key, value args
      flat =
        Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)

      # Build parameterized query
      placeholders =
        flat
        |> Enum.with_index(2)
        |> Enum.map(fn {_, i} -> "$#{i}" end)
        |> Enum.join(", ")

      sql = "SELECT STREAM_XADD($1, #{placeholders})"
      params = [stream | flat]

      case Nucleus.Client.query(client, sql, params) do
        {:ok, %{rows: [[id]]}} -> {:ok, id}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the number of entries in a stream."
  @spec xlen(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def xlen(client, stream) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Streams.xlen") do
      case Nucleus.Client.query(client, "SELECT STREAM_XLEN($1)", [stream]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Reads entries from a stream in a time range."
  @spec xrange(client(), String.t(), integer(), integer() | :inf, integer()) ::
          {:ok, list()} | {:error, term()}
  def xrange(client, stream, start_ms, end_ms, count) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Streams.xrange") do
      end_val = if end_ms == :inf, do: 9_999_999_999_999, else: end_ms

      case Nucleus.Client.query(client, "SELECT STREAM_XRANGE($1, $2, $3, $4)", [
             stream,
             start_ms,
             end_val,
             count
           ]) do
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:ok, %{rows: [[list]]}} when is_list(list) -> {:ok, list}
        {:ok, _} -> {:ok, []}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Reads new entries from a stream after a given ID."
  @spec xread(client(), String.t(), integer(), integer()) ::
          {:ok, list()} | {:error, term()}
  def xread(client, stream, last_id_ms, count) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Streams.xread") do
      case Nucleus.Client.query(client, "SELECT STREAM_XREAD($1, $2, $3)", [
             stream,
             last_id_ms,
             count
           ]) do
        {:ok, %{rows: [[json]]}} when is_binary(json) -> {:ok, Jason.decode!(json)}
        {:ok, %{rows: [[list]]}} when is_list(list) -> {:ok, list}
        {:ok, _} -> {:ok, []}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Creates a consumer group on a stream."
  @spec xgroup_create(client(), String.t(), String.t(), integer()) ::
          {:ok, boolean()} | {:error, term()}
  def xgroup_create(client, stream, group, start_id \\ 0) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Streams.xgroup_create") do
      case Nucleus.Client.query(client, "SELECT STREAM_XGROUP_CREATE($1, $2, $3)", [
             stream,
             group,
             start_id
           ]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Reads from a stream as part of a consumer group."
  @spec xreadgroup(client(), String.t(), String.t(), String.t(), integer()) ::
          {:ok, list() | String.t()} | {:error, term()}
  def xreadgroup(client, stream, group, consumer, count) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Streams.xreadgroup") do
      case Nucleus.Client.query(client, "SELECT STREAM_XREADGROUP($1, $2, $3, $4)", [
             stream,
             group,
             consumer,
             count
           ]) do
        {:ok, %{rows: [[json]]}} when is_binary(json) ->
          case Jason.decode(json) do
            {:ok, parsed} -> {:ok, parsed}
            {:error, _} -> {:ok, json}
          end

        {:ok, _} ->
          {:ok, []}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc "Acknowledges processing of a stream entry in a consumer group."
  @spec xack(client(), String.t(), String.t(), integer(), integer()) ::
          {:ok, boolean()} | {:error, term()}
  def xack(client, stream, group, id_ms, id_seq) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Streams.xack") do
      case Nucleus.Client.query(client, "SELECT STREAM_XACK($1, $2, $3, $4)", [
             stream,
             group,
             id_ms,
             id_seq
           ]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
