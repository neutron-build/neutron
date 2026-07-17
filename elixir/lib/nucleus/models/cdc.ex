defmodule Nucleus.Models.CDC do
  @moduledoc """
  Change Data Capture model — CDC_READ, CDC_COUNT, CDC_TABLE_READ.

  ## Example

      alias Nucleus.Models.CDC

      {:ok, changes} = CDC.read(client, 0, 100)
      {:ok, count} = CDC.count(client)
      {:ok, table_changes} = CDC.table_read(client, "users", 0, 100)
  """

  @type client :: Nucleus.Client.t()

  @typedoc ~S(A CDC event: %{"seq" => integer, "table" => String.t, "change" => String.t, "ts" => integer})
  @type event :: map()

  @doc "Reads up to `limit` CDC events after a given sequence number."
  @spec read(client(), integer(), integer()) :: {:ok, [event()]} | {:error, term()}
  def read(client, after_sequence, limit \\ 100) do
    with :ok <- Nucleus.Client.require_nucleus(client, "CDC.read") do
      case Nucleus.Client.query(client, "SELECT CDC_READ($1, $2)", [after_sequence, limit]) do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) -> {:ok, decode_events(raw)}
        {:ok, _} -> {:ok, []}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the total number of CDC events."
  @spec count(client()) :: {:ok, integer()} | {:error, term()}
  def count(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "CDC.count") do
      case Nucleus.Client.query(client, "SELECT CDC_COUNT()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Reads up to `limit` CDC events for a specific table after a given sequence number."
  @spec table_read(client(), String.t(), integer(), integer()) ::
          {:ok, [event()]} | {:error, term()}
  def table_read(client, table, after_sequence, limit \\ 100) do
    with :ok <- Nucleus.Client.require_nucleus(client, "CDC.table_read") do
      case Nucleus.Client.query(client, "SELECT CDC_TABLE_READ($1, $2, $3)", [
             table,
             after_sequence,
             limit
           ]) do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) -> {:ok, decode_events(raw)}
        {:ok, _} -> {:ok, []}
        {:error, _} = error -> error
      end
    end
  end

  # --- Internal ---

  # Engine returns a JSON array of {"seq","table","change","ts"} events.
  defp decode_events(raw) do
    case Jason.decode(raw) do
      {:ok, events} when is_list(events) -> events
      _ -> []
    end
  end
end
