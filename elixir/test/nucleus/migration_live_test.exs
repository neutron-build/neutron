defmodule Nucleus.MigrationLiveTest do
  use ExUnit.Case, async: false
  alias Nucleus.{Client, Migration, Repo}
  @url System.get_env("NEUTRON_TEST_DATABASE_URL")
  @moduletag skip: @url == nil
  @key 7_043_516_000_858_342_567

  defmodule First do
    use Migration
    def up(c), do: execute(c, "CREATE TABLE migration_first(id INT)")
    def down(c), do: execute(c, "DROP TABLE migration_first")
  end

  defmodule Second do
    use Migration
    def up(c), do: execute(c, "CREATE TABLE migration_second(id INT)")
    def down(c), do: execute(c, "DROP TABLE migration_second")
  end

  defmodule Partial do
    use Migration

    def up(c) do
      :ok = execute(c, "CREATE TABLE migration_partial(id INT)")
      {:error, :intentional_failure}
    end

    def down(_), do: :ok
  end

  defmodule Unexpected do
    use Migration

    def up(c) do
      :ok = execute(c, "CREATE TABLE migration_partial(id INT)")
      :unexpected
    end

    def down(_), do: :ok
  end

  defmodule Raised do
    use Migration

    def up(c) do
      :ok = execute(c, "CREATE TABLE migration_partial(id INT)")
      raise "intentional_callback_exception"
    end

    def down(_), do: :ok
  end

  defmodule FailedDown do
    use Migration
    def up(c), do: execute(c, "CREATE TABLE migration_first(id INT)")

    def down(c) do
      :ok = execute(c, "DROP TABLE migration_first")
      {:error, :intentional_down_failure}
    end
  end

  defmodule Changed do
    use Migration
    def up(c), do: execute(c, "CREATE TABLE migration_changed(id INT)")
    def down(_), do: :ok
  end

  defmodule Barrier do
    use Migration

    def up(c) do
      {:ok, %{rows: [[pid]]}} = Client.query(c, "SELECT pg_backend_pid()")
      send(Process.get(:migration_test_parent), {:callback, self(), pid})

      receive do
        :release -> execute(c, "INSERT INTO migration_effects DEFAULT VALUES")
      after
        10_000 -> {:error, :barrier_timeout}
      end
    end

    def down(_), do: :ok
  end

  defmodule CancelBarrier do
    use Migration

    def up(c) do
      :ok = execute(c, "CREATE TABLE migration_partial(id INT)")
      {:ok, %{rows: [[pid]]}} = Client.query(c, "SELECT pg_backend_pid()")
      send(Process.get(:migration_test_parent), {:callback, self(), pid})
      execute(c, "SELECT pg_advisory_xact_lock(81277013012)")
    end

    def down(_), do: :ok
  end

  def opts(url) do
    u = URI.parse(url)
    [user | pass] = String.split(u.userinfo || "postgres", ":", parts: 2)

    [
      hostname: u.host,
      port: u.port || 5432,
      username: URI.decode(user),
      password: URI.decode(List.first(pass) || ""),
      database: String.trim_leading(u.path, "/")
    ]
  end

  def sql(c, text, params \\ []), do: Postgrex.query!(c, text, params)
  def present(c, name), do: sql(c, "SELECT to_regclass($1) IS NOT NULL", [name]).rows == [[true]]
  def wait(fun, attempts \\ 500)
  def wait(fun, 0), do: assert(fun.(), "observed barrier/state did not reach expected condition")

  def wait(fun, attempts) do
    if fun.(),
      do: :ok,
      else:
        (
          Process.sleep(10)
          wait(fun, attempts - 1)
        )
  end

  def start_client(url) do
    Client.start_link(
      url: url,
      name: String.to_atom("migration_live_#{System.unique_integer([:positive])}"),
      pool_size: 1
    )
  end

  setup do
    {:ok, oracle} = Postgrex.start_link(opts(@url))
    Process.unlink(oracle)

    for [name] <-
          sql(oracle, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public'").rows do
      sql(oracle, "DROP TABLE \"#{name}\" CASCADE")
    end

    {:ok, client} = start_client(@url)
    Process.unlink(client)

    on_exit(fn ->
      for pid <- [client, oracle], Process.alive?(pid), do: GenServer.stop(pid)
    end)

    %{oracle: oracle, client: client}
  end

  test "per-step success, rollback and unchanged callback stay unverified", %{
    client: c,
    oracle: o
  } do
    assert {:ok, 2} = Migration.run(c, [{2, Second}, {1, First}])
    assert {:ok, [1, 2]} = Migration.applied(c)
    assert :ok = Migration.rollback(c, [{1, First}, {2, Second}])
    refute present(o, "migration_second")
    assert {:ok, 0} = Migration.run(c, [{1, Changed}])
    refute present(o, "migration_changed")

    assert {:ok, [%{version: 2, status: :down}, %{version: 1, status: :up}]} =
             Migration.status(c, [{2, Second}, {1, First}])
  end

  test "returned callback error aborts only its step and restores process context", %{
    client: c,
    oracle: o
  } do
    assert {:error, :intentional_failure} = Migration.run(c, [{1, First}, {2, Partial}])
    assert present(o, "migration_first")
    refute present(o, "migration_partial")
    assert sql(o, "SELECT version FROM _neutron_migrations").rows == [[1]]
    assert Process.get(:nucleus_tx_conn) == nil
  end

  test "unexpected callback return aborts DDL", %{client: c, oracle: o} do
    assert {:error, {:invalid_migration_callback_return, :unexpected}} =
             Migration.run(c, [{1, Unexpected}])

    refute present(o, "migration_partial")
  end

  test "callback success followed by history trigger failure rolls DDL back", %{
    client: c,
    oracle: o
  } do
    assert :ok = Migration.ensure_table(c)

    sql(
      o,
      "CREATE OR REPLACE FUNCTION refuse_migration_record() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'record boundary'; END $$"
    )

    sql(
      o,
      "CREATE TRIGGER refuse_migration_record BEFORE INSERT ON _neutron_migrations FOR EACH ROW EXECUTE FUNCTION refuse_migration_record()"
    )

    assert {:error, %Postgrex.Error{}} = Migration.run(c, [{1, First}])
    refute present(o, "migration_first")
    assert sql(o, "SELECT count(*) FROM _neutron_migrations").rows == [[0]]
  end

  test "rejects an outer Repo transaction without nested callback SQL", %{client: c, oracle: o} do
    assert {:ok, {:error, :migration_requires_no_existing_transaction}} =
             Repo.transaction(c, fn -> Migration.run(c, [{1, First}]) end)

    refute present(o, "migration_first")
    refute present(o, "_neutron_migrations")
    assert Process.get(:nucleus_tx_conn) == nil
  end

  test "rejects canonical, SDK, domain, view and known foreign ledgers", %{client: c, oracle: o} do
    for ddl <- [
          "CREATE TABLE _neutron_migrations(version TEXT PRIMARY KEY,name TEXT NOT NULL,applied_at TIMESTAMPTZ DEFAULT NOW())",
          "CREATE TABLE _neutron_migrations(version INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at TIMESTAMPTZ DEFAULT NOW())",
          "CREATE TABLE _neutron_migrations(version BIGINT PRIMARY KEY,name TEXT NOT NULL,applied_at TIMESTAMPTZ DEFAULT NOW(),checksum TEXT,owner TEXT,format TEXT)",
          "CREATE VIEW _neutron_migrations AS SELECT 1::bigint AS version, 'view'::text AS name, now() AS applied_at"
        ] do
      sql(o, ddl)
      assert {:error, :migration_incompatible_history} = Migration.run(c, [{1, First}])
      refute present(o, "migration_first")

      if String.starts_with?(ddl, "CREATE VIEW"),
        do: sql(o, "DROP VIEW _neutron_migrations"),
        else: sql(o, "DROP TABLE _neutron_migrations")
    end

    sql(o, "CREATE DOMAIN migration_version AS BIGINT")

    sql(
      o,
      "CREATE TABLE _neutron_migrations(version migration_version PRIMARY KEY,name TEXT NOT NULL,applied_at TIMESTAMPTZ DEFAULT NOW())"
    )

    assert {:error, :migration_incompatible_history} = Migration.run(c, [{1, First}])
    sql(o, "DROP TABLE _neutron_migrations")
    sql(o, "DROP DOMAIN migration_version")

    for foreign <- ["__nucleus_migrations", "__pg_migrations", "_neutron_migration_lock"] do
      sql(o, "CREATE TABLE #{foreign}(name TEXT)")
      assert {:error, :migration_foreign_history} = Migration.run(c, [{1, First}])
      refute present(o, "_neutron_migrations")
      sql(o, "DROP TABLE #{foreign}")
    end
  end

  test "temp shadow is refused without permanent DDL", %{client: c, oracle: o} do
    assert :ok = Migration.ensure_table(c)

    assert {:ok, _} =
             Client.query(
               c,
               "CREATE TEMP TABLE _neutron_migrations(version BIGINT PRIMARY KEY,name TEXT NOT NULL,applied_at TIMESTAMPTZ DEFAULT NOW())"
             )

    assert {:error, :migration_temp_history_shadow} = Migration.run(c, [{1, First}])
    refute present(o, "migration_first")
    assert sql(o, "SELECT count(*) FROM public._neutron_migrations").rows == [[0]]
  end

  test "rejects a resolved history outside the intended first search schema", %{
    client: c,
    oracle: o
  } do
    assert :ok = Migration.ensure_table(c)
    sql(o, "CREATE SCHEMA migration_other")

    try do
      assert {:ok, _} = Client.query(c, "SET search_path TO migration_other, public")
      assert {:error, :migration_history_shadow} = Migration.run(c, [{1, First}])

      assert sql(o, "SELECT to_regclass('migration_other._neutron_migrations') IS NULL").rows == [
               [true]
             ]
    after
      Client.query(c, "SET search_path TO public")
      sql(o, "DROP SCHEMA migration_other CASCADE")
    end
  end

  test "duplicate starts wait on the actual key and execute one callback", %{client: c, oracle: o} do
    sql(o, "CREATE TABLE migration_effects(id BIGSERIAL)")
    {:ok, other} = start_client(@url)
    parent = self()

    a =
      Task.async(fn ->
        Process.put(:migration_test_parent, parent)
        Migration.run(c, [{1, Barrier}])
      end)

    assert_receive {:callback, owner, pid}, 5_000

    b =
      Task.async(fn ->
        Process.put(:migration_test_parent, parent)
        Migration.run(other, [{1, Barrier}])
      end)

    wait(fn ->
      sql(
        o,
        "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted AND classid::bigint*4294967296+objid::bigint=$1",
        [@key]
      ).rows == [[1]]
    end)

    assert sql(
             o,
             "SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND granted AND classid::bigint*4294967296+objid::bigint=$2",
             [pid, @key]
           ).rows == [[1]]

    send(owner, :release)
    assert Task.await(a, 5_000) == {:ok, 1}
    assert Task.await(b, 5_000) == {:ok, 0}
    assert sql(o, "SELECT count(*) FROM migration_effects").rows == [[1]]
    GenServer.stop(other)
  end

  test "owner death discards interrupted transaction and releases the lock", %{
    client: c,
    oracle: o
  } do
    sql(o, "SELECT pg_advisory_lock(81277013012)")
    parent = self()

    owner =
      spawn(fn ->
        Process.put(:migration_test_parent, parent)
        Migration.run(c, [{1, CancelBarrier}])
      end)

    assert_receive {:callback, ^owner, old_pid}, 5_000

    wait(fn ->
      sql(
        o,
        "SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted",
        [old_pid]
      ).rows == [[1]]
    end)

    Process.exit(owner, :kill)
    wait(fn -> not Process.alive?(owner) end)
    assert {:ok, %{rows: [[new_pid]]}} = Client.query(c, "SELECT pg_backend_pid()")
    assert new_pid != old_pid
    IO.puts("owner-death PostgreSQL backend replacement: old=#{old_pid} new=#{new_pid}")
    sql(o, "SELECT pg_advisory_unlock(81277013012)")

    wait(fn ->
      sql(o, "SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory'", [old_pid]).rows ==
        [[0]]
    end)

    refute present(o, "migration_partial")
    assert sql(o, "SELECT count(*) FROM _neutron_migrations").rows == [[0]]
    assert {:ok, 1} = Migration.run(c, [{1, First}])
  end

  test "raised callback restores context and rolls back DDL", %{client: c, oracle: o} do
    assert_raise RuntimeError, "intentional_callback_exception", fn ->
      Migration.run(c, [{1, Raised}])
    end

    refute present(o, "migration_partial")
    assert sql(o, "SELECT count(*) FROM _neutron_migrations").rows == [[0]]
    assert Process.get(:nucleus_tx_conn) == nil
    assert {:ok, 1} = Migration.run(c, [{1, First}])
  end

  test "failed down callback preserves business table and history", %{client: c, oracle: o} do
    assert {:ok, 1} = Migration.run(c, [{1, FailedDown}])
    assert {:error, :intentional_down_failure} = Migration.rollback(c, [{1, FailedDown}])
    assert present(o, "migration_first")
    assert sql(o, "SELECT version FROM _neutron_migrations").rows == [[1]]
  end

  @tag skip: System.get_env("NEUTRON_TEST_NUCLEUS_URL") == nil
  test "published Nucleus refuses every runner entrypoint before mutation" do
    url = System.get_env("NEUTRON_TEST_NUCLEUS_URL")

    if url do
      {:ok, c} = start_client(url)
      {:ok, o} = Postgrex.start_link(opts(url))

      try do
        assert Client.is_nucleus?(c)
        assert {:error, :migration_requires_postgresql} = Migration.ensure_table(c)
        assert {:error, :migration_requires_postgresql} = Migration.applied(c)
        assert {:error, :migration_requires_postgresql} = Migration.status(c, [{1, First}])
        assert {:error, :migration_requires_postgresql} = Migration.run(c, [{1, First}])
        assert {:error, :migration_requires_postgresql} = Migration.rollback(c, [{1, First}])
        assert sql(o, "SELECT tablename FROM pg_tables WHERE schemaname='public'").rows == []
      after
        GenServer.stop(c)
        GenServer.stop(o)
      end
    else
      flunk("NEUTRON_TEST_NUCLEUS_URL required for provider refusal coverage")
    end
  end

  test "missing advisory lock permission fails before history or callback DDL", %{oracle: o} do
    role = "v10_elixir_lock_" <> Integer.to_string(System.unique_integer([:positive]))
    sql(o, "CREATE ROLE #{role} LOGIN PASSWORD 'ephemeral_probe'")
    sql(o, "GRANT USAGE, CREATE ON SCHEMA public TO #{role}")

    sql(
      o,
      "CREATE FUNCTION public.pg_advisory_xact_lock(bigint) RETURNS void LANGUAGE plpgsql AS 'BEGIN RETURN; END'"
    )

    sql(o, "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_advisory_xact_lock(bigint) FROM PUBLIC")
    url = URI.parse(@url) |> Map.put(:userinfo, role <> ":ephemeral_probe") |> URI.to_string()
    {:ok, c} = start_client(url)
    assert {:ok, _} = Client.query(c, "SET search_path TO public, pg_catalog")
    assert {:ok, _} = Client.query(c, "SELECT public.pg_advisory_xact_lock($1)", [@key])

    try do
      assert {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege}}} =
               Migration.run(c, [{1, First}])

      refute present(o, "_neutron_migrations")
      refute present(o, "migration_first")
    after
      GenServer.stop(c)
      sql(o, "DROP FUNCTION public.pg_advisory_xact_lock(bigint)")
      sql(o, "GRANT EXECUTE ON FUNCTION pg_catalog.pg_advisory_xact_lock(bigint) TO PUBLIC")
      sql(o, "DROP OWNED BY #{role}")
      sql(o, "DROP ROLE #{role}")
    end
  end

  test "absent current schema refuses before DDL", %{client: c, oracle: o} do
    assert {:ok, _} = Client.query(c, "SET search_path = v10_missing_schema")

    try do
      assert {:error, :migration_requires_current_schema} = Migration.run(c, [{1, First}])
      refute present(o, "_neutron_migrations")
      refute present(o, "migration_first")
    after
      Client.query(c, "SET search_path = public")
    end
  end

  test "captured ledger schema is quoted safely", %{client: c, oracle: o} do
    schema = "v10 \"quoted"
    quoted = "\"" <> String.replace(schema, "\"", "\"\"") <> "\""
    sql(o, "CREATE SCHEMA " <> quoted)
    assert {:ok, _} = Client.query(c, "SET search_path = " <> quoted)

    try do
      assert {:ok, 1} = Migration.run(c, [{1, First}])
      assert sql(o, "SELECT version FROM #{quoted}._neutron_migrations").rows == [[1]]
      assert sql(o, "SELECT count(*) FROM #{quoted}.migration_first").rows == [[0]]
      refute present(o, "_neutron_migrations")
    after
      Client.query(c, "SET search_path = public")
      sql(o, "DROP SCHEMA #{quoted} CASCADE")
    end
  end

  test "duplicate and nonpositive plans refuse before creating history", %{client: c, oracle: o} do
    assert {:error, :invalid_migration_plan} = Migration.run(c, [{1, First}, {1, First}])
    assert {:error, :invalid_migration_plan} = Migration.run(c, [{0, First}])
    refute present(o, "_neutron_migrations")
  end

  test "catalog functions cannot be shadowed by application search_path", %{client: c, oracle: o} do
    sql(o, "CREATE SCHEMA v10_function_shadow")

    sql(
      o,
      "CREATE FUNCTION v10_function_shadow.pg_advisory_xact_lock(bigint) RETURNS void LANGUAGE plpgsql AS 'BEGIN RETURN; END'"
    )

    for {name, args, result} <- [
          {"version", "", "text"},
          {"current_schema", "", "name"},
          {"pg_my_temp_schema", "", "oid"},
          {"to_regclass", "text", "regclass"},
          {"now", "", "timestamptz"}
        ] do
      sql(
        o,
        "CREATE FUNCTION v10_function_shadow.#{name}(#{args}) RETURNS #{result} LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''shadow function called''; END'"
      )
    end

    sql(o, "CREATE TABLE v10_function_shadow.migration_effects(id INT)")
    assert {:ok, _} = Client.query(c, "SET search_path TO v10_function_shadow, pg_catalog")
    parent = self()

    task =
      Task.async(fn ->
        Process.put(:migration_test_parent, parent)
        Migration.run(c, [{1, Barrier}])
      end)

    try do
      assert_receive {:callback, owner, pid}, 5_000

      assert sql(
               o,
               "SELECT count(*) FROM pg_catalog.pg_locks WHERE pid=$1 AND locktype='advisory' AND granted AND classid::bigint*4294967296+objid::bigint=$2",
               [pid, @key]
             ).rows == [[1]]

      assert sql(o, "SELECT pg_catalog.pg_try_advisory_xact_lock($1)", [@key]).rows == [[false]]
      send(owner, :release)
      assert Task.await(task, 5_000) == {:ok, 1}
      assert sql(o, "SELECT version FROM v10_function_shadow._neutron_migrations").rows == [[1]]
      assert sql(o, "SELECT count(*) FROM v10_function_shadow.migration_effects").rows == [[1]]
      assert sql(o, "SELECT pg_catalog.pg_try_advisory_xact_lock($1)", [@key]).rows == [[true]]
    after
      Task.shutdown(task, :brutal_kill)
      Client.query(c, "SET search_path TO public")
      sql(o, "DROP SCHEMA v10_function_shadow CASCADE")
    end
  end
end
