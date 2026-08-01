defmodule Nucleus.RetryTest do
  use ExUnit.Case, async: true

  alias Nucleus.Retry

  # Classification is the part that must be right without a live server: get it
  # wrong and either a retryable conflict is surfaced as a hard failure, or a
  # lock timeout is retried in a loop against a lock that is not moving.

  describe "sqlstate/1" do
    test "reads the code Postgrex normalised to an atom" do
      assert Retry.sqlstate(pg_error(:serialization_failure)) == "40001"
      assert Retry.sqlstate(pg_error(:lock_not_available)) == "55P03"
      assert Retry.sqlstate(pg_error(:in_failed_sql_transaction)) == "25P02"
    end

    test "looks through an {:error, _} wrapper" do
      assert Retry.sqlstate({:error, pg_error(:serialization_failure)}) == "40001"
    end

    test "is nil for anything without a code" do
      assert Retry.sqlstate(:timeout) == nil
      assert Retry.sqlstate(%RuntimeError{message: "40001"}) == nil
    end
  end

  describe "serialization_failure?/1" do
    test "40001 and 25P02 are retryable" do
      assert Retry.serialization_failure?(pg_error(:serialization_failure))
      assert Retry.serialization_failure?(pg_error(:in_failed_sql_transaction))
    end

    test "a lock timeout is NOT retryable" do
      # The distinction that matters: the holder is still there, so retrying
      # spins against a lock that is not moving.
      refute Retry.serialization_failure?(pg_error(:lock_not_available))
      assert Retry.lock_not_available?(pg_error(:lock_not_available))
    end

    test "an unrelated error is not retryable" do
      refute Retry.serialization_failure?(pg_error(:syntax_error))
      refute Retry.serialization_failure?(:econnrefused)
      refute Retry.lock_not_available?(:econnrefused)
    end
  end

  describe "code accessors" do
    test "expose the SQLSTATEs the contract names" do
      assert Retry.serialization_failure() == "40001"
      assert Retry.lock_not_available() == "55P03"
      assert Retry.in_failed_transaction() == "25P02"
    end
  end

  defp pg_error(code), do: %Postgrex.Error{postgres: %{code: code}}
end
