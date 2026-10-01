defmodule Nucleus.Migration do
  @moduledoc """
  Legacy callback migration runner for PostgreSQL.

  Manages database schema migrations with up/down semantics. Tracks applied
  migrations in a schema-qualified `_neutron_migrations` table. Each step
  takes the CLI transaction advisory lock and commits callback SQL and history
  together. Steps may interleave between commits. Nucleus is unsupported.
  Callbacks must use synchronous, same-process `Nucleus.Client.query` calls;
  external effects and spawned processes are not transaction participants.
  Existing history has no checksum and remains unverified; use CLI migrations
  for new managed applications. Invoke outside `Nucleus.Repo.transaction`.

  ## Defining Migrations

      defmodule MyApp.Migrations.CreateUsers do
        use Nucleus.Migration

        @impl true
        def up(client) do
          execute(client, \"""
            CREATE TABLE users (
              id SERIAL PRIMARY KEY,
              name TEXT NOT NULL,
              email TEXT UNIQUE NOT NULL,
              created_at TIMESTAMPTZ DEFAULT NOW()
            )
          \""")
        end

        @impl true
        def down(client) do
          execute(client, "DROP TABLE IF EXISTS users")
        end
      end

  ## Running Migrations

      # Apply all pending migrations
      Nucleus.Migration.run(client, [
        {1, MyApp.Migrations.CreateUsers},
        {2, MyApp.Migrations.CreatePosts}
      ])

      # Rollback the last migration
      Nucleus.Migration.rollback(client, [
        {1, MyApp.Migrations.CreateUsers},
        {2, MyApp.Migrations.CreatePosts}
      ])
  """

  @callback up(Nucleus.Client.t()) :: :ok | {:error, term()}
  @callback down(Nucleus.Client.t()) :: :ok | {:error, term()}

  defmacro __using__(_opts) do
    quote do
      @behaviour Nucleus.Migration

      @doc false
      def execute(client, sql) do
        case Nucleus.Client.query(client, sql) do
          {:ok, _} -> :ok
          {:error, _} = error -> error
        end
      end
    end
  end

  @migrations_table "_neutron_migrations"
  @lock_key 7_043_516_000_858_342_567
  @foreign_tables ["__nucleus_migrations", "__pg_migrations", "_neutron_migration_lock"]

  @doc "Ensures an admitted PostgreSQL legacy history table exists."
  @spec ensure_table(Nucleus.Client.t()) :: :ok | {:error, term()}
  def ensure_table(client) do
    case step(client, nil, fn _conn, _table -> :ok end) do
      {:ok, {:ok, _scope}} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc "Returns applied versions under a short PostgreSQL transaction lock."
  @spec applied(Nucleus.Client.t()) :: {:ok, [integer()]} | {:error, term()}
  def applied(client) do
    case step(client, nil, fn conn, table -> versions(conn, table) end) do
      {:ok, {result, _scope}} -> {:ok, result}
      {:error, _} = error -> error
    end
  end

  @doc "Applies pending callbacks with a fresh locked history read for every step."
  @spec run(Nucleus.Client.t(), [{integer(), module()}]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def run(client, migrations) do
    with {:ok, plan} <- prepare(migrations),
         {:ok, {_result, scope}} <- step(client, nil, fn _conn, _table -> :ok end) do
      Enum.reduce_while(plan, {:ok, 0, scope}, fn {version, mod}, {:ok, count, scope} ->
        case step(client, scope, fn conn, table ->
               if version in versions(conn, table) do
                 0
               else
                 callback(conn, client, mod, :up)

                 query(conn, "INSERT INTO #{table} (version, name) VALUES ($1, $2)", [
                   version,
                   inspect(mod)
                 ])

                 1
               end
             end) do
          {:ok, {delta, scope}} -> {:cont, {:ok, count + delta, scope}}
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, count, _scope} -> {:ok, count}
        {:error, _} = error -> error
      end
    end
  end

  @doc "Rolls back the latest applied callback and its history in one locked step."
  @spec rollback(Nucleus.Client.t(), [{integer(), module()}]) :: :ok | {:error, term()}
  def rollback(client, migrations) do
    with {:ok, plan} <- prepare(migrations) do
      case step(client, nil, fn conn, table ->
             case List.last(versions(conn, table)) do
               nil ->
                 Postgrex.rollback(conn, :no_migrations_to_rollback)

               version ->
                 case List.keyfind(plan, version, 0) do
                   nil ->
                     Postgrex.rollback(conn, :migration_not_found)

                   {_, mod} ->
                     callback(conn, client, mod, :down)
                     query(conn, "DELETE FROM #{table} WHERE version = $1", [version])
                     :ok
                 end
             end
           end) do
        {:ok, {:ok, _scope}} -> :ok
        {:error, _} = error -> error
      end
    end
  end

  @doc "Reports status for admitted, unverified legacy PostgreSQL history."
  @spec status(Nucleus.Client.t(), [{integer(), module()}]) ::
          {:ok, [%{version: integer(), module: module(), status: :up | :down}]} | {:error, term()}
  def status(client, migrations) do
    with {:ok, _plan} <- prepare(migrations), {:ok, applied} <- applied(client) do
      {:ok,
       Enum.map(migrations, fn {version, mod} ->
         %{version: version, module: mod, status: if(version in applied, do: :up, else: :down)}
       end)}
    end
  end

  defp prepare(migrations) when is_list(migrations) do
    valid =
      Enum.all?(migrations, fn
        {version, mod} when is_integer(version) and version > 0 and is_atom(mod) ->
          Code.ensure_loaded?(mod) and function_exported?(mod, :up, 1) and
            function_exported?(mod, :down, 1)

        _ ->
          false
      end)

    if valid and length(Enum.uniq_by(migrations, &elem(&1, 0))) == length(migrations),
      do: {:ok, Enum.sort_by(migrations, &elem(&1, 0))},
      else: {:error, :invalid_migration_plan}
  end

  defp prepare(_), do: {:error, :invalid_migration_plan}

  defp step(client, expected, fun) do
    cond do
      Process.get(:nucleus_tx_conn) != nil ->
        {:error, :migration_requires_no_existing_transaction}

      Nucleus.Client.is_nucleus?(client) ->
        {:error, :migration_requires_postgresql}

      true ->
        Postgrex.transaction(Nucleus.Client.pool(client), fn conn ->
          [[version]] = query(conn, "SELECT version()", []).rows

          if not String.starts_with?(version, "PostgreSQL ") or
               String.contains?(version, "Nucleus"),
             do: Postgrex.rollback(conn, :migration_requires_postgresql)

          query(conn, "SELECT pg_advisory_xact_lock($1)", [@lock_key])

          {schema, schema_oid} =
            case query(
                   conn,
                   "SELECT n.nspname, n.oid::bigint FROM pg_catalog.pg_namespace n WHERE n.nspname = current_schema()",
                   []
                 ).rows do
              [[schema, schema_oid]] -> {schema, schema_oid}
              _ -> Postgrex.rollback(conn, :migration_requires_current_schema)
            end

          if expected != nil and {schema, schema_oid} != {elem(expected, 0), elem(expected, 1)},
            do: Postgrex.rollback(conn, :migration_scope_changed)

          table = quote_identifier(schema) <> "." <> quote_identifier(@migrations_table)
          oid = admit(conn, schema)

          if expected != nil and oid != elem(expected, 2),
            do: Postgrex.rollback(conn, :migration_history_changed)

          if oid == nil do
            query(
              conn,
              "CREATE TABLE #{table} (version BIGINT PRIMARY KEY, name TEXT NOT NULL, applied_at TIMESTAMPTZ DEFAULT NOW())",
              []
            )
          end

          oid = admit(conn, schema)
          result = fun.(conn, table)
          if admit(conn, schema) != oid, do: Postgrex.rollback(conn, :migration_history_changed)
          {result, {schema, schema_oid, oid}}
        end)
    end
  end

  defp admit(conn, schema) do
    names = [@migrations_table | @foreign_tables]

    temp =
      query(
        conn,
        "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace = pg_my_temp_schema() AND relname = ANY($1::text[])",
        [names]
      ).rows

    if temp != [], do: Postgrex.rollback(conn, :migration_temp_history_shadow)

    foreign =
      query(
        conn,
        "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=ANY($2::text[])",
        [schema, @foreign_tables]
      ).rows

    if foreign != [], do: Postgrex.rollback(conn, :migration_foreign_history)

    own =
      query(
        conn,
        "SELECT c.oid::bigint,c.relkind::text,c.relpersistence::text FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2",
        [schema, @migrations_table]
      ).rows

    [[resolved]] = query(conn, "SELECT to_regclass($1)::oid::bigint", [@migrations_table]).rows

    case own do
      [] ->
        if resolved != nil, do: Postgrex.rollback(conn, :migration_history_shadow)
        nil

      [[oid, "r", "p"]] ->
        if resolved != oid, do: Postgrex.rollback(conn, :migration_history_shadow)

        columns =
          query(
            conn,
            "SELECT attname,atttypid::bigint,attnotnull,attnum::int,attidentity::text,attgenerated::text FROM pg_catalog.pg_attribute WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped ORDER BY attnum",
            [oid]
          ).rows

        case columns do
          [
            ["version", 20, true, version_num, "", ""],
            ["name", 25, true, _, "", ""],
            ["applied_at", 1184, _, _, "", ""]
          ] ->
            constraints =
              query(
                conn,
                "SELECT contype::text,conkey FROM pg_catalog.pg_constraint WHERE conrelid=$1::oid",
                [oid]
              ).rows

            if constraints != [["p", [version_num]]],
              do: Postgrex.rollback(conn, :migration_incompatible_history)

            oid

          _ ->
            Postgrex.rollback(conn, :migration_incompatible_history)
        end

      _ ->
        Postgrex.rollback(conn, :migration_incompatible_history)
    end
  end

  defp versions(conn, table) do
    query(conn, "SELECT version FROM #{table} ORDER BY version", []).rows |> Enum.map(&hd/1)
  end

  defp callback(conn, client, mod, direction) do
    previous = Process.put(:nucleus_tx_conn, conn)

    try do
      case apply(mod, direction, [client]) do
        :ok -> :ok
        {:error, reason} -> Postgrex.rollback(conn, reason)
        other -> Postgrex.rollback(conn, {:invalid_migration_callback_return, other})
      end
    after
      if previous == nil,
        do: Process.delete(:nucleus_tx_conn),
        else: Process.put(:nucleus_tx_conn, previous)
    end
  end

  defp query(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, result} -> result
      {:error, reason} -> Postgrex.rollback(conn, reason)
    end
  end

  defp quote_identifier(name), do: "\"" <> String.replace(name, "\"", "\"\"") <> "\""
end
