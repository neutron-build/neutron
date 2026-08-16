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

  @doc "Sets the global retention policy (auto-delete points older than `max_age_ms`)."
  @spec retention(client(), integer()) :: :ok | {:error, term()}
  def retention(client, max_age_ms) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.retention") do
      case Nucleus.Client.query(client, "SELECT TS_RETENTION($1)", [max_age_ms]) do
        {:ok, _} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Aggregates a series into fixed windows across a range.

  Returns one `{bucket_start_ms, value}` per `window_ms`-sized bucket between
  `start_ms` and `end_ms`, skipping buckets with no data.

  `fn` is `:avg` or `:count`. The engine ships `TS_RANGE_AVG` and
  `TS_RANGE_COUNT` and nothing else — sum/min/max/first/last do not exist, so
  they are rejected here rather than silently returning an average.

  Two traps this inherits from the Python implementation, both of which cost a
  release there:

    * `TIME_BUCKET` takes `(bucket_millis, ts)`, both integers. Passing an
      interval NAME — "minute", "hour" — raises on the type, and that bug meant
      Python's `aggregate` had never once worked.
    * Alignment is to `window_ms`, not to a calendar unit. Aligning a
      five-minute window to an hour boundary produces buckets that do not line
      up with the window the caller asked for, which is a wrong answer rather
      than an error.
  """
  @spec aggregate(client(), String.t(), integer(), integer(), integer(), :avg | :count) ::
          {:ok, [{integer(), float()}]} | {:error, term()}
  def aggregate(client, series, start_ms, end_ms, window_ms, fun \\ :avg) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.aggregate"),
         {:ok, sql_fn} <- aggregate_fn(fun) do
      cond do
        window_ms <= 0 -> {:ok, []}
        end_ms <= start_ms -> {:ok, []}
        true -> do_aggregate(client, series, start_ms, end_ms, window_ms, sql_fn)
      end
    end
  end

  defp aggregate_fn(:avg), do: {:ok, "TS_RANGE_AVG"}
  defp aggregate_fn(:count), do: {:ok, "TS_RANGE_COUNT"}

  defp aggregate_fn(other),
    do: {:error, {:unsupported_aggregate, other, [:avg, :count]}}

  defp do_aggregate(client, series, start_ms, end_ms, window_ms, sql_fn) do
    with {:ok, aligned} <- time_bucket(client, window_ms, start_ms) do
      bucket_start = aligned || start_ms

      bucket_start
      |> Stream.iterate(&(&1 + window_ms))
      |> Stream.take_while(&(&1 < end_ms))
      |> Enum.reduce_while({:ok, []}, fn bstart, {:ok, acc} ->
        effective_end = min(bstart + window_ms, end_ms)

        case Nucleus.Client.query(client, "SELECT #{sql_fn}($1, $2, $3)", [
               series,
               bstart,
               effective_end
             ]) do
          {:ok, %{rows: [[nil]]}} -> {:cont, {:ok, acc}}
          {:ok, %{rows: [[v]]}} -> {:cont, {:ok, [{bstart, v / 1} | acc]}}
          {:ok, %{rows: []}} -> {:cont, {:ok, acc}}
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        other -> other
      end
    end
  end

  @doc """
  Truncates a timestamp (ms) down to its bucket of `bucket_ms` milliseconds.
  """
  @spec time_bucket(client(), integer(), integer()) :: {:ok, integer()} | {:error, term()}
  def time_bucket(client, bucket_ms, timestamp) do
    with :ok <- Nucleus.Client.require_nucleus(client, "TimeSeries.time_bucket") do
      case Nucleus.Client.query(client, "SELECT TIME_BUCKET($1, $2)", [bucket_ms, timestamp]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
