defmodule Nucleus.Migration do
  @moduledoc """
  SQL migration runner for Nucleus/PostgreSQL.

  Manages database schema migrations with up/down semantics. Tracks applied
  migrations in a `_neutron_migrations` table.

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

  @doc """
  Ensures the migrations tracking table exists.
  """
  @spec ensure_table(Nucleus.Client.t()) :: :ok | {:error, term()}
  def ensure_table(client) do
    sql = """
    CREATE TABLE IF NOT EXISTS #{@migrations_table} (
      version BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      applied_at TIMESTAMPTZ DEFAULT NOW()
    )
    """

    case Nucleus.Client.query(client, sql) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  @doc """
  Returns a list of applied migration versions.
  """
  @spec applied(Nucleus.Client.t()) :: {:ok, [integer()]} | {:error, term()}
  def applied(client) do
    with :ok <- ensure_table(client) do
      case Nucleus.Client.query(
             client,
             "SELECT version FROM #{@migrations_table} ORDER BY version"
           ) do
        {:ok, %{rows: rows}} ->
          {:ok, Enum.map(rows, fn [v] -> v end)}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc """
  Runs all pending migrations.

  Migrations is a list of `{version, module}` tuples, ordered by version.
  """
  @spec run(Nucleus.Client.t(), [{integer(), module()}]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def run(client, migrations) do
    with :ok <- ensure_table(client),
         {:ok, applied_versions} <- applied(client) do
      pending =
        migrations
        |> Enum.reject(fn {version, _mod} -> version in applied_versions end)
        |> Enum.sort_by(fn {version, _} -> version end)

      Enum.reduce_while(pending, {:ok, 0}, fn {version, mod}, {:ok, count} ->
        case mod.up(client) do
          :ok ->
            case Nucleus.Client.query(
                   client,
                   "INSERT INTO #{@migrations_table} (version, name) VALUES ($1, $2)",
                   [version, inspect(mod)]
                 ) do
              {:ok, _} ->
                {:cont, {:ok, count + 1}}

              {:error, _} = error ->
                {:halt, error}
            end

          {:error, _} = error ->
            {:halt, error}
        end
      end)
    end
  end

  @doc """
  Rolls back the last applied migration.
  """
  @spec rollback(Nucleus.Client.t(), [{integer(), module()}]) ::
          :ok | {:error, term()}
  def rollback(client, migrations) do
    with {:ok, applied_versions} <- applied(client) do
      case List.last(applied_versions) do
        nil ->
          {:error, :no_migrations_to_rollback}

        last_version ->
          case Enum.find(migrations, fn {v, _} -> v == last_version end) do
            {_version, mod} ->
              with :ok <- mod.down(client),
                   {:ok, _} <-
                     Nucleus.Client.query(
                       client,
                       "DELETE FROM #{@migrations_table} WHERE version = $1",
                       [last_version]
                     ) do
                :ok
              end

            nil ->
              {:error, :migration_not_found}
          end
      end
    end
  end

  @doc """
  Returns the current migration status.
  """
  @spec status(Nucleus.Client.t(), [{integer(), module()}]) ::
          {:ok, [%{version: integer(), module: module(), status: :up | :down}]}
          | {:error, term()}
  def status(client, migrations) do
    with {:ok, applied_versions} <- applied(client) do
      statuses =
        Enum.map(migrations, fn {version, mod} ->
          %{
            version: version,
            module: mod,
            status: if(version in applied_versions, do: :up, else: :down)
          }
        end)

      {:ok, statuses}
    end
  end
end
