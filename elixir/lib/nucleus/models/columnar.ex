defmodule Nucleus.Models.Columnar do
  @moduledoc """
  Columnar analytics model — COLUMNAR_INSERT, COLUMNAR_COUNT, COLUMNAR_SUM/AVG/MIN/MAX.

  ## Example

      alias Nucleus.Models.Columnar

      Columnar.insert(client, "events", %{user_id: 42, action: "click", value: 1.5})
      {:ok, count} = Columnar.count(client, "events")
      {:ok, total} = Columnar.sum(client, "events", "value")
      {:ok, avg} = Columnar.avg(client, "events", "value")
  """

  @type client :: Nucleus.Client.t()

  @doc "Inserts a row into a columnar table."
  @spec insert(client(), String.t(), map()) :: {:ok, boolean()} | {:error, term()}
  def insert(client, table, values) when is_map(values) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Columnar.insert") do
      json = Jason.encode!(values)

      case Nucleus.Client.query(client, "SELECT COLUMNAR_INSERT($1, $2)", [table, json]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the row count for a columnar table."
  @spec count(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def count(client, table) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Columnar.count") do
      case Nucleus.Client.query(client, "SELECT COLUMNAR_COUNT($1)", [table]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the sum of a column."
  @spec sum(client(), String.t(), String.t()) :: {:ok, number()} | {:error, term()}
  def sum(client, table, column) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Columnar.sum") do
      case Nucleus.Client.query(client, "SELECT COLUMNAR_SUM($1, $2)", [table, column]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the average of a column."
  @spec avg(client(), String.t(), String.t()) :: {:ok, float()} | {:error, term()}
  def avg(client, table, column) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Columnar.avg") do
      case Nucleus.Client.query(client, "SELECT COLUMNAR_AVG($1, $2)", [table, column]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the minimum value of a column."
  @spec min(client(), String.t(), String.t()) :: {:ok, term()} | {:error, term()}
  def min(client, table, column) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Columnar.min") do
      case Nucleus.Client.query(client, "SELECT COLUMNAR_MIN($1, $2)", [table, column]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the maximum value of a column."
  @spec max(client(), String.t(), String.t()) :: {:ok, term()} | {:error, term()}
  def max(client, table, column) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Columnar.max") do
      case Nucleus.Client.query(client, "SELECT COLUMNAR_MAX($1, $2)", [table, column]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
