defmodule Nucleus.Retry do
  @moduledoc """
  Serialization-failure classification and a managed retry helper.

  `SERIALIZABLE` is real on the shipping engine, which makes SQLSTATE 40001
  something applications actually receive. A serializable transaction that is
  never retried is a transaction that randomly fails under concurrency, and no
  PostgreSQL driver retries for you — drivers surface the code, the application
  decides. This is the SDK's answer to that obligation, per
  `FRAMEWORK_CONTRACT.md` §3.14.

      Nucleus.Retry.with_tx(client, fn ->
        Nucleus.Repo.query(client, "UPDATE accounts SET balance = balance - 10 WHERE id = $1", [id])
      end, isolation: "SERIALIZABLE")

  Classification is by SQLSTATE, never by message text, and it looks through
  `Postgrex.Error` wrapping so a code stays visible after the SDK wraps it.
  """

  @doc """
  The transaction lost a conflict and MUST be retried from the beginning.

  Raised by two mechanisms: strict 2PL wait-die on the disk engine (the younger
  transaction is killed to break a potential deadlock) and SSI on the MVCC
  engine (a dangerous structure detected at commit).
  """
  @serialization_failure "40001"

  @doc """
  `lock_timeout` elapsed waiting for a table lock. Deliberately NOT retryable:
  the holder is still there, so retrying spins against a lock that is not
  moving. Raise `lock_timeout` or find the transaction holding it.
  """
  @lock_not_available "55P03"

  @doc """
  A statement was issued after the transaction had already been aborted. Only
  ROLLBACK is accepted, so the whole transaction must re-run — which is what
  `with_tx/3` does.
  """
  @in_failed_transaction "25P02"

  @default_max_attempts 5
  @default_base_delay_ms 2
  @default_max_delay_ms 250

  def serialization_failure, do: @serialization_failure
  def lock_not_available, do: @lock_not_available
  def in_failed_transaction, do: @in_failed_transaction

  @doc "The SQLSTATE of an error, or `nil` if it carries no code."
  @spec sqlstate(term()) :: String.t() | nil
  def sqlstate(%Postgrex.Error{postgres: %{code: code}}) when is_atom(code) do
    # Postgrex normalises known codes to atoms (:serialization_failure); the
    # raw five-character code is what the contract classifies on.
    case code do
      :serialization_failure -> @serialization_failure
      :lock_not_available -> @lock_not_available
      :in_failed_sql_transaction -> @in_failed_transaction
      _ -> nil
    end
  end

  def sqlstate(%Postgrex.Error{postgres: %{pg_code: code}}) when is_binary(code), do: code
  def sqlstate({:error, reason}), do: sqlstate(reason)
  def sqlstate(_), do: nil

  @doc """
  Whether `error` is a conflict the caller should retry (40001, or 25P02 from a
  transaction already killed by one).
  """
  @spec serialization_failure?(term()) :: boolean()
  def serialization_failure?(error) do
    sqlstate(error) in [@serialization_failure, @in_failed_transaction]
  end

  @doc """
  Whether `error` is a `lock_timeout` expiry (55P03).

  Kept distinct from `serialization_failure?/1` on purpose: the two call for
  opposite responses. A serialization failure means "someone else won, try
  again"; a lock timeout means "the lock is still held, retrying will not help".
  """
  @spec lock_not_available?(term()) :: boolean()
  def lock_not_available?(error), do: sqlstate(error) == @lock_not_available

  @doc """
  Run `fun` inside a transaction, retrying it on serialization failure.

  `fun` MUST be idempotent with respect to anything outside the database: it can
  run more than once. Everything it does through the transaction is rolled back
  between attempts; anything it does elsewhere (sending mail, charging a card,
  writing to ETS) is not.

  On success the transaction commits. On a serialization failure it is rolled
  back and retried with jittered exponential backoff. On any other error it is
  rolled back and the error returned unchanged — in particular a `lock_timeout`
  (55P03) is NOT retried, because the lock is still held.

  ## Options

    * `:max_attempts` — attempts including the first (default #{@default_max_attempts})
    * `:base_delay_ms` — delay before the second attempt (default #{@default_base_delay_ms})
    * `:max_delay_ms`  — backoff ceiling (default #{@default_max_delay_ms})
    * `:isolation`     — e.g. `"SERIALIZABLE"`; omitted leaves the server default
  """
  @spec with_tx(Nucleus.Client.t(), (-> term()), keyword()) :: {:ok, term()} | {:error, term()}
  def with_tx(client, fun, opts \\ []) when is_function(fun, 0) do
    max_attempts = max(Keyword.get(opts, :max_attempts, @default_max_attempts), 1)
    base_delay = Keyword.get(opts, :base_delay_ms, @default_base_delay_ms)
    max_delay = Keyword.get(opts, :max_delay_ms, @default_max_delay_ms)
    isolation = Keyword.get(opts, :isolation)

    attempt(client, fun, isolation, 1, max_attempts, base_delay, max_delay, nil)
  end

  defp attempt(_client, _fun, _iso, n, max, _base, _max_delay, last) when n > max do
    {:error, last}
  end

  defp attempt(client, fun, isolation, n, max, base, max_delay, _last) do
    result =
      Nucleus.Repo.transaction(client, fn ->
        if isolation do
          # An engine that cannot honour the level refuses rather than silently
          # downgrading, so this surfaces the mismatch instead of hiding it.
          Nucleus.Repo.query(client, "SET TRANSACTION ISOLATION LEVEL #{isolation}", [])
        end

        fun.()
      end)

    cond do
      not retryable?(result) ->
        result

      n == max ->
        {:error, error_of(result)}

      true ->
        # Full jitter. Without it two conflicting transactions retry in lockstep
        # and collide again on the same schedule — and under wait-die the
        # younger one loses every round, so a fixed backoff can starve it.
        delay = min(base * :math.pow(2, n - 1), max_delay) |> trunc()
        Process.sleep(:rand.uniform(max(delay, 1)))
        attempt(client, fun, isolation, n + 1, max, base, max_delay, error_of(result))
    end
  end

  defp retryable?({:error, reason}), do: serialization_failure?(reason)
  defp retryable?(_), do: false

  defp error_of({:error, reason}), do: reason
  defp error_of(other), do: other
end
