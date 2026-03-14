defmodule Nucleus.Repo do
  @moduledoc """
  Ecto-style repository module for SQL queries against Nucleus.

  Provides a higher-level API for common CRUD operations on SQL tables.

  ## Example

      # Query rows
      {:ok, users} = Nucleus.Repo.all(client, "users", where: [active: true], limit: 10)

      # Insert
      {:ok, id} = Nucleus.Repo.insert(client, "users", %{name: "Alice", email: "alice@example.com"})

      # Update
      {:ok, count} = Nucleus.Repo.update(client, "users", %{active: false}, where: [id: 42])

      # Delete
      {:ok, count} = Nucleus.Repo.delete(client, "users", where: [id: 42])

      # Raw query
      {:ok, result} = Nucleus.Repo.query(client, "SELECT * FROM users WHERE age > $1", [21])
  """

  @identifier_regex ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/

  @doc "Validates a SQL identifier (table/column name)."
  @spec valid_identifier?(String.t()) :: boolean()
  def valid_identifier?(name), do: Regex.match?(@identifier_regex, name)

  @doc """
  Queries all rows from a table.

  ## Options

    * `:where` — keyword list of column = value conditions
    * `:limit` — max rows to return
    * `:offset` — number of rows to skip
    * `:order_by` — column to sort by (string or `{column, :asc/:desc}`)
    * `:select` — list of columns to select (default: `*`)
  """
  @spec all(Nucleus.Client.t(), String.t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def all(client, table, opts \\ []) do
    unless valid_identifier?(table), do: raise(ArgumentError, "Invalid table name: #{table}")

    select = Keyword.get(opts, :select, ["*"]) |> Enum.join(", ")
    {where_clause, params} = build_where(Keyword.get(opts, :where, []))

    sql = "SELECT #{select} FROM #{table}#{where_clause}"

    sql =
      case Keyword.get(opts, :order_by) do
        nil -> sql
        {col, :desc} -> "#{sql} ORDER BY #{col} DESC"
        {col, :asc} -> "#{sql} ORDER BY #{col} ASC"
        col when is_binary(col) -> "#{sql} ORDER BY #{col}"
        _ -> sql
      end

    sql =
      case Keyword.get(opts, :limit) do
        nil -> sql
        limit -> "#{sql} LIMIT #{limit}"
      end

    sql =
      case Keyword.get(opts, :offset) do
        nil -> sql
        offset -> "#{sql} OFFSET #{offset}"
      end

    case Nucleus.Client.query(client, sql, params) do
      {:ok, %{columns: columns, rows: rows}} ->
        maps = Enum.map(rows, fn row -> columns |> Enum.zip(row) |> Map.new() end)
        {:ok, maps}

      {:error, _} = error ->
        error
    end
  end

  @doc "Inserts a row and returns the result."
  @spec insert(Nucleus.Client.t(), String.t(), map()) :: {:ok, Postgrex.Result.t()} | {:error, term()}
  def insert(client, table, data) when is_map(data) do
    unless valid_identifier?(table), do: raise(ArgumentError, "Invalid table name: #{table}")

    columns = Map.keys(data) |> Enum.map(&to_string/1)
    values = Map.values(data)
    placeholders = Enum.with_index(columns, 1) |> Enum.map(fn {_, i} -> "$#{i}" end)

    sql =
      "INSERT INTO #{table} (#{Enum.join(columns, ", ")}) VALUES (#{Enum.join(placeholders, ", ")}) RETURNING *"

    Nucleus.Client.query(client, sql, values)
  end

  @doc "Updates rows matching conditions."
  @spec update(Nucleus.Client.t(), String.t(), map(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def update(client, table, data, opts \\ []) when is_map(data) do
    unless valid_identifier?(table), do: raise(ArgumentError, "Invalid table name: #{table}")

    {set_parts, set_params, idx} =
      data
      |> Enum.reduce({[], [], 1}, fn {col, val}, {parts, params, i} ->
        {["#{col} = $#{i}" | parts], params ++ [val], i + 1}
      end)

    set_clause = Enum.reverse(set_parts) |> Enum.join(", ")

    {where_clause, where_params} =
      build_where(Keyword.get(opts, :where, []), idx)

    sql = "UPDATE #{table} SET #{set_clause}#{where_clause}"

    case Nucleus.Client.query(client, sql, set_params ++ where_params) do
      {:ok, %{num_rows: count}} -> {:ok, count}
      {:error, _} = error -> error
    end
  end

  @doc "Deletes rows matching conditions."
  @spec delete(Nucleus.Client.t(), String.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def delete(client, table, opts \\ []) do
    unless valid_identifier?(table), do: raise(ArgumentError, "Invalid table name: #{table}")

    {where_clause, params} = build_where(Keyword.get(opts, :where, []))

    sql = "DELETE FROM #{table}#{where_clause}"

    case Nucleus.Client.query(client, sql, params) do
      {:ok, %{num_rows: count}} -> {:ok, count}
      {:error, _} = error -> error
    end
  end

  @doc "Executes a raw SQL query."
  @spec query(Nucleus.Client.t(), String.t(), list()) ::
          {:ok, Postgrex.Result.t()} | {:error, term()}
  def query(client, sql, params \\ []) do
    Nucleus.Client.query(client, sql, params)
  end

  @doc "Executes a SQL query within a transaction."
  @spec transaction(Nucleus.Client.t(), (-> term())) :: {:ok, term()} | {:error, term()}
  def transaction(client, fun) do
    pool = Nucleus.Client.pool(client)

    Postgrex.transaction(pool, fn conn ->
      Process.put(:nucleus_tx_conn, conn)

      try do
        fun.()
      after
        Process.delete(:nucleus_tx_conn)
      end
    end)
  end

  # --- Internal ---

  defp build_where([], _start_idx \\ 1), do: {"", []}

  defp build_where(conditions, start_idx) when is_list(conditions) do
    {clauses, params, _idx} =
      Enum.reduce(conditions, {[], [], start_idx}, fn {col, val}, {clauses, params, i} ->
        {["#{col} = $#{i}" | clauses], params ++ [val], i + 1}
      end)

    where = " WHERE " <> (Enum.reverse(clauses) |> Enum.join(" AND "))
    {where, params}
  end
end
