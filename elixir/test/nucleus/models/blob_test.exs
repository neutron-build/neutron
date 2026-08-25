defmodule Nucleus.Models.BlobTest do
  use ExUnit.Case, async: true

  alias Nucleus.Models.Blob

  describe "module exports" do
    test "exports store/3 and store/4" do
      assert function_exported?(Blob, :store, 3)
      assert function_exported?(Blob, :store, 4)
    end

    test "exports get/2" do
      assert function_exported?(Blob, :get, 2)
    end

    test "exports delete/2" do
      assert function_exported?(Blob, :delete, 2)
    end

    test "exports meta/2" do
      assert function_exported?(Blob, :meta, 2)
    end

    test "exports tag/4" do
      assert function_exported?(Blob, :tag, 4)
    end

    test "exports list/1 and list/2" do
      assert function_exported?(Blob, :list, 1)
      assert function_exported?(Blob, :list, 2)
    end

    test "exports count/1" do
      assert function_exported?(Blob, :count, 1)
    end

    test "exports dedup_ratio/1" do
      assert function_exported?(Blob, :dedup_ratio, 1)
    end
  end

  # BLOB_LIST's only Ok arm is Value::Text("[...]") (scalar_fns.rs) — an empty
  # store answers "[]", never NULL — and the wire column is TEXT, so Postgrex
  # always delivers a binary; any other shape raises rather than collapsing
  # into {:ok, []}.
  describe "list/2 payload handling" do
    test "an empty store (\"[]\" payload) is an empty list" do
      ok = %Postgrex.Result{rows: [["[]"]], num_rows: 1}

      assert {:ok, []} = Blob.list(fake_client({:ok, ok}))
      assert {:ok, []} = Blob.list(fake_client({:ok, ok}), "prefix/")
    end

    test "a populated payload decodes to keys" do
      ok = %Postgrex.Result{rows: [[~s(["avatar.png"])]]}
      assert {:ok, ["avatar.png"]} = Blob.list(fake_client({:ok, ok}))
    end

    test "a NULL cell is a contract violation, not an empty result" do
      null_cell = %Postgrex.Result{rows: [[nil]], num_rows: 1}

      assert_raise CaseClauseError, fn ->
        Blob.list(fake_client({:ok, null_cell}))
      end
    end

    test "a list cell cannot arrive from the TEXT column and raises" do
      list_cell = %Postgrex.Result{rows: [[["avatar.png"]]], num_rows: 1}

      assert_raise CaseClauseError, fn ->
        Blob.list(fake_client({:ok, list_cell}))
      end
    end

    test "server errors pass through" do
      error = %Postgrex.Error{postgres: %{pg_code: "42P01", message: "undefined function"}}

      assert {:error, %Postgrex.Error{}} = Blob.list(fake_client({:error, error}))
    end
  end

  # Stands in for the Nucleus.Client GenServer, as in streams_test.exs.
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
