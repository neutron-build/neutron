defmodule Neutron.Auth.SessionTest do
  use ExUnit.Case

  alias Neutron.Auth.Session

  setup do
    # Ensure the ETS table exists for sessions
    try do
      :ets.delete(:neutron_sessions)
    rescue
      ArgumentError -> :ok
    end

    :ets.new(:neutron_sessions, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ok
  end

  describe "create/2" do
    test "creates a session and returns {:ok, session_id}" do
      assert {:ok, session_id} = Session.create(%{user_id: 42})
      assert is_binary(session_id)
      assert String.length(session_id) > 0
    end

    test "generates unique session IDs" do
      {:ok, id1} = Session.create(%{user_id: 1})
      {:ok, id2} = Session.create(%{user_id: 2})
      assert id1 != id2
    end

    test "stores session data in ETS" do
      {:ok, session_id} = Session.create(%{user_id: 42, role: "admin"})
      [{^session_id, record}] = :ets.lookup(:neutron_sessions, session_id)
      assert record.data == %{user_id: 42, role: "admin"}
      assert is_integer(record.created_at)
      assert is_integer(record.expires_at)
    end

    test "respects custom TTL" do
      {:ok, session_id} = Session.create(%{user_id: 42}, ttl: 60)
      [{^session_id, record}] = :ets.lookup(:neutron_sessions, session_id)
      # expires_at should be approximately created_at + 60
      assert record.expires_at - record.created_at == 60
    end

    test "uses default TTL of 86400" do
      {:ok, session_id} = Session.create(%{user_id: 42})
      [{^session_id, record}] = :ets.lookup(:neutron_sessions, session_id)
      assert record.expires_at - record.created_at == 86_400
    end
  end

  describe "get/1" do
    test "retrieves existing session data" do
      {:ok, session_id} = Session.create(%{user_id: 42, name: "Alice"})
      assert {:ok, data} = Session.get(session_id)
      assert data == %{user_id: 42, name: "Alice"}
    end

    test "returns {:error, :not_found} for non-existent session" do
      assert {:error, :not_found} = Session.get("non-existent-session-id")
    end

    test "returns {:error, :expired} for expired session" do
      {:ok, session_id} = Session.create(%{user_id: 42}, ttl: -1)
      assert {:error, :expired} = Session.get(session_id)
    end

    test "deletes expired session on access" do
      {:ok, session_id} = Session.create(%{user_id: 42}, ttl: -1)
      Session.get(session_id)
      assert :ets.lookup(:neutron_sessions, session_id) == []
    end
  end

  describe "put/2" do
    test "updates session data" do
      {:ok, session_id} = Session.create(%{user_id: 42})
      assert :ok = Session.put(session_id, %{user_id: 42, role: "admin"})
      assert {:ok, %{user_id: 42, role: "admin"}} = Session.get(session_id)
    end

    test "returns {:error, :not_found} for non-existent session" do
      assert {:error, :not_found} = Session.put("non-existent", %{data: "new"})
    end

    test "preserves expiration time" do
      {:ok, session_id} = Session.create(%{user_id: 42}, ttl: 3600)
      [{^session_id, original}] = :ets.lookup(:neutron_sessions, session_id)
      :ok = Session.put(session_id, %{user_id: 42, updated: true})
      [{^session_id, updated}] = :ets.lookup(:neutron_sessions, session_id)
      assert original.expires_at == updated.expires_at
    end
  end

  describe "delete/1" do
    test "removes a session" do
      {:ok, session_id} = Session.create(%{user_id: 42})
      assert :ok = Session.delete(session_id)
      assert {:error, :not_found} = Session.get(session_id)
    end

    test "returns :ok for non-existent session" do
      assert :ok = Session.delete("non-existent-session")
    end
  end

  describe "touch/2" do
    test "extends session TTL" do
      {:ok, session_id} = Session.create(%{user_id: 42}, ttl: 60)
      [{^session_id, before}] = :ets.lookup(:neutron_sessions, session_id)
      :ok = Session.touch(session_id, 7200)
      [{^session_id, after_touch}] = :ets.lookup(:neutron_sessions, session_id)
      assert after_touch.expires_at > before.expires_at
    end

    test "returns {:error, :not_found} for non-existent session" do
      assert {:error, :not_found} = Session.touch("non-existent-session")
    end

    test "uses default TTL when no argument given" do
      {:ok, session_id} = Session.create(%{user_id: 42}, ttl: 60)
      :ok = Session.touch(session_id)
      [{^session_id, record}] = :ets.lookup(:neutron_sessions, session_id)
      # Default TTL is 86400, so expires_at should be about now + 86400
      now = System.system_time(:second)
      assert record.expires_at >= now + 86_300
    end
  end

  describe "cleanup/0" do
    test "removes expired sessions" do
      {:ok, _id1} = Session.create(%{user_id: 1}, ttl: -1)
      {:ok, _id2} = Session.create(%{user_id: 2}, ttl: -1)
      {:ok, id3} = Session.create(%{user_id: 3}, ttl: 3600)

      removed = Session.cleanup()
      assert removed == 2

      # Valid session should still exist
      assert {:ok, %{user_id: 3}} = Session.get(id3)
    end

    test "returns 0 when no expired sessions" do
      {:ok, _id} = Session.create(%{user_id: 1}, ttl: 3600)
      assert Session.cleanup() == 0
    end

    test "returns 0 when table is empty" do
      assert Session.cleanup() == 0
    end
  end
end

defmodule Neutron.Auth.SessionSweeperTest do
  use ExUnit.Case

  alias Neutron.Auth.SessionSweeper

  setup do
    # Ensure the ETS table exists
    try do
      :ets.delete(:neutron_sessions)
    rescue
      ArgumentError -> :ok
    end

    :ets.new(:neutron_sessions, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ok
  end

  test "starts as a GenServer" do
    {:ok, pid} = SessionSweeper.start_link([])
    assert Process.alive?(pid)
    GenServer.stop(pid)
  end

  test "responds to :sweep message" do
    {:ok, pid} = SessionSweeper.start_link([])

    # Insert an expired session
    :ets.insert(:neutron_sessions, {"expired-1", %{
      data: %{user_id: 1},
      created_at: 0,
      expires_at: 0
    }})

    # Send sweep manually
    send(pid, :sweep)
    # Give it a moment to process
    Process.sleep(50)

    # Expired session should be cleaned up
    assert :ets.lookup(:neutron_sessions, "expired-1") == []

    GenServer.stop(pid)
  end

  test "schedules periodic sweep" do
    {:ok, pid} = SessionSweeper.start_link([])
    # The sweeper should have scheduled a :sweep message
    # We can verify it's alive and processing
    assert Process.alive?(pid)
    GenServer.stop(pid)
  end
end
