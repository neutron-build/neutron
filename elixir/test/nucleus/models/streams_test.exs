defmodule Nucleus.Models.StreamsTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Streams

  describe "module exports" do
    test "exports xadd/3" do
      assert function_exported?(Streams, :xadd, 3)
    end

    test "exports xlen/2" do
      assert function_exported?(Streams, :xlen, 2)
    end

    test "exports xrange/5" do
      assert function_exported?(Streams, :xrange, 5)
    end

    test "exports xread/4" do
      assert function_exported?(Streams, :xread, 4)
    end

    test "exports xgroup_create/3 and xgroup_create/4" do
      assert function_exported?(Streams, :xgroup_create, 3)
      assert function_exported?(Streams, :xgroup_create, 4)
    end

    test "exports xreadgroup/5" do
      assert function_exported?(Streams, :xreadgroup, 5)
    end

    # xack/4, not /5: the id is one "<ms>-<seq>" string rather than two
    # integers, so it composes with what xadd returns.
    test "exports xack/4" do
      assert function_exported?(Streams, :xack, 4)
    end
  end

  # Nucleus v0.1.8: XREADGROUP on a missing group is a NOGROUP error from the
  # server (SQLSTATE 22000), never an empty batch. The model must hand the
  # Postgrex error to the caller instead of collapsing it into {:ok, []}.
  describe "xreadgroup error surfacing" do
    test "NOGROUP is returned as an error, not an empty batch" do
      nogroup = %Postgrex.Error{
        postgres: %{
          pg_code: "22000",
          message: "NOGROUP No such consumer group 'workers' for stream 'events'"
        }
      }

      assert {:error, %Postgrex.Error{} = err} =
               Streams.xreadgroup(
                 fake_client({:error, nogroup}),
                 "events",
                 "workers",
                 "worker-1",
                 5
               )

      assert err.postgres.message =~ "NOGROUP"
    end

    test "a successful caught-up read still decodes to an empty list" do
      ok = %Postgrex.Result{rows: [["[]"]], num_rows: 1}

      assert {:ok, []} =
               Streams.xreadgroup(fake_client({:ok, ok}), "events", "workers", "worker-1", 5)
    end
  end

  # The engine answers XRANGE/XREAD on a missing stream with "" — an empty
  # TEXT cell (scalar_fns.rs None arms), which Postgrex delivers as "". That
  # is the reachable empty case. A NULL cell never occurs: both engine arms
  # return Text, so the model raises on it rather than collapsing it.
  describe "xrange/xread payload handling" do
    test "missing stream (\"\" payload) is an empty list, not a decode error" do
      ok = %Postgrex.Result{rows: [[""]], num_rows: 1}

      assert {:ok, []} = Streams.xrange(fake_client({:ok, ok}), "events", 0, :inf, 100)
      assert {:ok, []} = Streams.xread(fake_client({:ok, ok}), "events", 0, 100)
    end

    test "a NULL cell is a contract violation, not an empty result" do
      null_cell = %Postgrex.Result{rows: [[nil]], num_rows: 1}

      assert_raise CaseClauseError, fn ->
        Streams.xrange(fake_client({:ok, null_cell}), "events", 0, :inf, 100)
      end

      assert_raise CaseClauseError, fn ->
        Streams.xread(fake_client({:ok, null_cell}), "events", 0, 100)
      end
    end

    # The wire column is TEXT (scalar_fns.rs declares STREAM_XRANGE/XREAD as
    # DataType::Text) and both engine arms construct Value::Text, so Postgrex
    # always delivers a binary. A list cell cannot arrive; the model raises on
    # it rather than passing it through.
    test "a list cell cannot arrive from the TEXT column and raises" do
      entries = [%{"id" => "100-0"}]
      list_cell = %Postgrex.Result{rows: [[entries]], num_rows: 1}

      assert_raise CaseClauseError, fn ->
        Streams.xrange(fake_client({:ok, list_cell}), "events", 0, :inf, 100)
      end

      assert_raise CaseClauseError, fn ->
        Streams.xread(fake_client({:ok, list_cell}), "events", 0, 100)
      end
    end

    test "a populated payload decodes to entries" do
      json = Jason.encode!([%{"id" => "100-0", "fields" => %{"action" => "login"}}])
      ok = %Postgrex.Result{rows: [[json]], num_rows: 1}

      assert {:ok, [%{"id" => "100-0", "fields" => %{"action" => "login"}}]} =
               Streams.xrange(fake_client({:ok, ok}), "events", 0, :inf, 100)
    end

    test "server errors pass through" do
      busygroup = %Postgrex.Error{
        postgres: %{pg_code: "23000", message: "BUSYGROUP consumer group already exists"}
      }

      assert {:error, %Postgrex.Error{}} =
               Streams.xread(fake_client({:error, busygroup}), "events", 0, 100)
    end
  end

  # Stands in for the Nucleus.Client GenServer: the model only needs a process
  # that answers :is_nucleus? and {:query, sql, params} the way the real client
  # (backed by Postgrex) does.
  defp fake_client(query_reply) do
    spawn_link(fn -> fake_loop(query_reply) end)
  end

  defp fake_loop(query_reply) do
    receive do
      {:"$gen_call", from, :is_nucleus?} ->
        GenServer.reply(from, true)
        fake_loop(query_reply)

      {:"$gen_call", from, {:query, _sql, _params}} ->
        GenServer.reply(from, query_reply)
        fake_loop(query_reply)
    end
  end
end
