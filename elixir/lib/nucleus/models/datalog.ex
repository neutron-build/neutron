defmodule Nucleus.Models.Datalog do
  @moduledoc """
  Datalog reasoning model — DATALOG_ASSERT, DATALOG_RETRACT, DATALOG_RULE, DATALOG_QUERY.

  ## Example

      alias Nucleus.Models.Datalog

      Datalog.assert(client, "parent(alice, bob)")
      Datalog.assert(client, "parent(bob, charlie)")
      Datalog.rule(client, "ancestor(X, Y)", "parent(X, Y)")
      Datalog.rule(client, "ancestor(X, Z)", "parent(X, Y), ancestor(Y, Z)")
      {:ok, result} = Datalog.query(client, "ancestor(alice, X)")
  """

  @type client :: Nucleus.Client.t()

  @doc "Asserts a fact into the Datalog knowledge base. Returns the engine status text."
  @spec assert(client(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def assert(client, fact) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.assert") do
      case Nucleus.Client.query(client, "SELECT DATALOG_ASSERT($1)", [fact]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Retracts a fact from the knowledge base. Returns the engine status text."
  @spec retract(client(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def retract(client, fact) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.retract") do
      case Nucleus.Client.query(client, "SELECT DATALOG_RETRACT($1)", [fact]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Defines a Datalog rule. Returns the engine status text.

  The head and body are joined into a single `head :- body` rule string,
  which is what the engine's DATALOG_RULE expects.
  """
  @spec rule(client(), String.t(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def rule(client, head, body) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.rule") do
      case Nucleus.Client.query(client, "SELECT DATALOG_RULE($1)", ["#{head} :- #{body}"]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Queries the Datalog knowledge base. Returns a list of result tuples (lists of strings)."
  @spec query(client(), String.t()) :: {:ok, [[String.t()]]} | {:error, term()}
  def query(client, query_str) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.query") do
      # DATALOG_QUERY answers Value::Text on its only Ok arm, and sql_query
      # always formats a "[...]" array — an empty result answers "[]", never
      # NULL — and the wire column is TEXT, so Postgrex always delivers a
      # binary; any other shape raises rather than collapsing into {:ok, []}.
      case Nucleus.Client.query(client, "SELECT DATALOG_QUERY($1)", [query_str]) do
        {:ok, %{rows: [[raw]]}} when is_binary(raw) -> {:ok, decode_rows(raw)}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Clears all facts and rules for a predicate. Returns the engine status text."
  @spec clear(client(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def clear(client, predicate) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.clear") do
      case Nucleus.Client.query(client, "SELECT DATALOG_CLEAR($1)", [predicate]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Imports all graph edges as Datalog facts: `predicate(from_id, edge_type, to_id)`.

  Returns the engine status text, e.g. `"IMPORTED 3 edges into edge"`.
  """
  @spec import_graph(client(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def import_graph(client, predicate) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.import_graph") do
      case Nucleus.Client.query(client, "SELECT DATALOG_IMPORT_GRAPH($1)", [predicate]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  # --- Internal ---

  # The payload is always a JSON array of result tuples, so one that does not
  # decode to a JSON array is a contract violation and raises.
  defp decode_rows(raw) do
    case Jason.decode!(raw) do
      rows when is_list(rows) -> rows
      other -> raise ArgumentError, "expected a JSON array payload, got: #{inspect(other)}"
    end
  end
end
