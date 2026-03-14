defmodule Nucleus.Models.TimeSeries do
  @moduledoc """
  Time-series model — TS_INSERT, TS_LAST, TS_COUNT, TS_RANGE, TIME_BUCKET.

  ## Example

      alias Nucleus.Models.TimeSeries

      TimeSeries.insert(client, "cpu_usage", System.system_time(:millisecond), 72.5)
      {:ok, 72.5} = TimeSeries.last(client, "cpu_usage")
      {:ok, avg} = TimeSeries.range_avg(client, "cpu_usage", start_ms, end_ms)
  """

  @type client :: Nucleus.Client.t()

  @doc "Inserts a time-series data point."
  @spec insert(client(), String.t(), integer(), number()) :: :ok | {:error, term()}
  def insert(client, series, timestamp_ms, value) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.insert") do
      case Nucleus.Client.query(client, "SELECT TS_INSERT($1, $2, $3)", [
             series,
             timestamp_ms,
             value
           ]) do
        {:ok, _} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the last value in a series."
  @spec last(client(), String.t()) :: {:ok, float() | nil} | {:error, term()}
  def last(client, series) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.last") do
      case Nucleus.Client.query(client, "SELECT TS_LAST($1)", [series]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the count of data points in a series."
  @spec count(client(), String.t()) :: {:ok, integer()} | {:error, term()}
  def count(client, series) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.count") do
      case Nucleus.Client.query(client, "SELECT TS_COUNT($1)", [series]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the count of data points in a time range."
  @spec range_count(client(), String.t(), integer(), integer()) ::
          {:ok, integer()} | {:error, term()}
  def range_count(client, series, start_ms, end_ms) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.range_count") do
      case Nucleus.Client.query(client, "SELECT TS_RANGE_COUNT($1, $2, $3)", [
             series,
             start_ms,
             end_ms
           ]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Returns the average value in a time range."
  @spec range_avg(client(), String.t(), integer(), integer()) ::
          {:ok, float() | nil} | {:error, term()}
  def range_avg(client, series, start_ms, end_ms) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.range_avg") do
      case Nucleus.Client.query(client, "SELECT TS_RANGE_AVG($1, $2, $3)", [
             series,
             start_ms,
             end_ms
           ]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:ok, %{rows: []}} -> {:ok, nil}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Sets a retention policy on a series (auto-delete after N days)."
  @spec retention(client(), String.t(), integer()) :: {:ok, boolean()} | {:error, term()}
  def retention(client, series, days) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.retention") do
      case Nucleus.Client.query(client, "SELECT TS_RETENTION($1, $2)", [series, days]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Pattern matching on a time series."
  @spec match(client(), String.t(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def match(client, series, pattern) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.match") do
      case Nucleus.Client.query(client, "SELECT TS_MATCH($1, $2)", [series, pattern]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Aggregates data points into time buckets.

  Intervals: `"second"`, `"minute"`, `"hour"`, `"day"`, `"week"`, `"month"`.
  """
  @spec time_bucket(client(), String.t(), integer()) :: {:ok, integer()} | {:error, term()}
  def time_bucket(client, interval, timestamp) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.time_bucket") do
      case Nucleus.Client.query(client, "SELECT TIME_BUCKET($1, $2)", [interval, timestamp]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
