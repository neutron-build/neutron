defmodule Nucleus.Models.CDC do
  @moduledoc """
  Change Data Capture model — CDC_READ, CDC_COUNT, CDC_TABLE_READ.

  ## Example

      alias Nucleus.Models.CDC

      {:ok, changes} = CDC.read(client, 0)
      {:ok, count} = CDC.count(client)
      {:ok, table_changes} = CDC.table_read(client, "users", 0)
  """

  @type client :: Nucleus.Client.t()

  @doc "Reads CDC events from a given offset."
  @spec read(client(), integer()) :: {:ok, String.t()} | {:error, term()}
  def read(client, offset) do
    with :ok <- Nucleus.Client.require_nucleus(client, "CDC.read") do
      case Nucleus.Client.query(client, "SELECT CDC_READ($1)", [offset]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
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

  @doc "Reads CDC events for a specific table from a given offset."
  @spec table_read(client(), String.t(), integer()) :: {:ok, String.t()} | {:error, term()}
  def table_read(client, table, offset) do
    with :ok <- Nucleus.Client.require_nucleus(client, "CDC.table_read") do
      case Nucleus.Client.query(client, "SELECT CDC_TABLE_READ($1, $2)", [table, offset]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
