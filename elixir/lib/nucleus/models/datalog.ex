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

  @doc "Asserts a fact into the Datalog knowledge base."
  @spec assert(client(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def assert(client, fact) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.assert") do
      case Nucleus.Client.query(client, "SELECT DATALOG_ASSERT($1)", [fact]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Retracts a fact from the knowledge base."
  @spec retract(client(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def retract(client, fact) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.retract") do
      case Nucleus.Client.query(client, "SELECT DATALOG_RETRACT($1)", [fact]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Defines a Datalog rule."
  @spec rule(client(), String.t(), String.t()) :: {:ok, boolean()} | {:error, term()}
  def rule(client, head, body) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.rule") do
      case Nucleus.Client.query(client, "SELECT DATALOG_RULE($1, $2)", [head, body]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Queries the Datalog knowledge base. Returns CSV-formatted results."
  @spec query(client(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def query(client, query_str) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.query") do
      case Nucleus.Client.query(client, "SELECT DATALOG_QUERY($1)", [query_str]) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Clears all facts and rules."
  @spec clear(client()) :: {:ok, boolean()} | {:error, term()}
  def clear(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.clear") do
      case Nucleus.Client.query(client, "SELECT DATALOG_CLEAR()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Imports all graph nodes and edges as Datalog facts."
  @spec import_graph(client()) :: {:ok, integer()} | {:error, term()}
  def import_graph(client) do
    with :ok <- Nucleus.Client.require_nucleus(client, "Datalog.import_graph") do
      case Nucleus.Client.query(client, "SELECT DATALOG_IMPORT_GRAPH()", []) do
        {:ok, %{rows: [[val]]}} -> {:ok, val}
        {:error, _} = error -> error
      end
    end
  end
end
