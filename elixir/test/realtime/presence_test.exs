defmodule Neutron.Realtime.PresenceExpandedTest do
  use ExUnit.Case

  alias Neutron.Realtime.Presence

  setup do
    # Ensure registry exists for broadcast_diff
    case Process.whereis(Neutron.Realtime.Registry) do
      nil ->
        {:ok, _} = Registry.start_link(keys: :duplicate, name: Neutron.Realtime.Registry)

      _ ->
        :ok
    end

    # Start a fresh Presence server
    case Process.whereis(Presence) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end

    {:ok, pid} = Presence.start_link([])

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    :ok
  end

  describe "track/3 adds user to presence list" do
    test "tracks a user with metadata" do
      topic = "room:track:#{System.unique_integer()}"
      assert :ok = Presence.track(topic, "user_1", %{name: "Alice", status: "online"})

      presences = Presence.list(topic)
      assert Map.has_key?(presences, "user_1")
      meta = presences["user_1"]
      assert meta[:name] == "Alice" || meta["name"] == "Alice"
      assert meta[:status] == "online" || meta["status"] == "online"
    end

    test "tracks multiple users in same topic" do
      topic = "room:track_multi:#{System.unique_integer()}"
      :ok = Presence.track(topic, "user_1", %{name: "Alice"})
      :ok = Presence.track(topic, "user_2", %{name: "Bob"})
      :ok = Presence.track(topic, "user_3", %{name: "Carol"})

      presences = Presence.list(topic)
      assert map_size(presences) == 3
      assert Map.has_key?(presences, "user_1")
      assert Map.has_key?(presences, "user_2")
      assert Map.has_key?(presences, "user_3")
    end

    test "tracks same user in different topics" do
      topic_a = "room:cross_a:#{System.unique_integer()}"
      topic_b = "room:cross_b:#{System.unique_integer()}"

      :ok = Presence.track(topic_a, "user_1", %{name: "Alice"})
      :ok = Presence.track(topic_b, "user_1", %{name: "Alice"})

      assert map_size(Presence.list(topic_a)) == 1
      assert map_size(Presence.list(topic_b)) == 1
    end

    test "tracks with default empty metadata" do
      topic = "room:default_meta:#{System.unique_integer()}"
      assert :ok = Presence.track(topic, "user_1")

      presences = Presence.list(topic)
      assert Map.has_key?(presences, "user_1")
      assert presences["user_1"] == %{}
    end

    test "replaces previous entry for same key in same topic" do
      topic = "room:replace:#{System.unique_integer()}"
      Presence.track(topic, "user_1", %{name: "Alice"})
      Presence.track(topic, "user_1", %{name: "Alice Updated"})

      presences = Presence.list(topic)
      assert map_size(presences) == 1
      meta = presences["user_1"]
      name = meta[:name] || meta["name"]
      assert name == "Alice Updated"
    end
  end

  describe "untrack/2 removes user" do
    test "removes a tracked presence from a topic" do
      topic = "room:untrack:#{System.unique_integer()}"
      Presence.track(topic, "user_1", %{name: "Alice"})
      Presence.track(topic, "user_2", %{name: "Bob"})

      assert :ok = Presence.untrack(topic, "user_1")

      presences = Presence.list(topic)
      assert map_size(presences) == 1
      refute Map.has_key?(presences, "user_1")
      assert Map.has_key?(presences, "user_2")
    end

    test "returns :ok when untracking non-tracked key" do
      topic = "room:untrack_none:#{System.unique_integer()}"
      assert :ok = Presence.untrack(topic, "nobody")
    end

    test "untracking a user from one topic does not affect other topics" do
      topic_a = "room:untrack_a:#{System.unique_integer()}"
      topic_b = "room:untrack_b:#{System.unique_integer()}"

      Presence.track(topic_a, "user_1", %{})
      Presence.track(topic_b, "user_1", %{})

      Presence.untrack(topic_a, "user_1")

      assert Presence.count(topic_a) == 0
      assert Presence.count(topic_b) == 1
    end
  end

  describe "list/1 returns all present users" do
    test "returns a map of key => metadata" do
      topic = "room:list:#{System.unique_integer()}"
      Presence.track(topic, "user_1", %{name: "Alice"})
      Presence.track(topic, "user_2", %{name: "Bob"})

      presences = Presence.list(topic)
      assert is_map(presences)
      assert map_size(presences) == 2
      assert Map.has_key?(presences, "user_1")
      assert Map.has_key?(presences, "user_2")
    end

    test "returns empty map for topic with no presences" do
      topic = "room:list_empty:#{System.unique_integer()}"
      assert Presence.list(topic) == %{}
    end

    test "returns empty map for non-existent topic" do
      assert Presence.list("room:nonexistent:#{System.unique_integer()}") == %{}
    end
  end

  describe "count/1" do
    test "returns the number of tracked presences" do
      topic = "room:count:#{System.unique_integer()}"
      Presence.track(topic, "u1", %{})
      Presence.track(topic, "u2", %{})
      assert Presence.count(topic) == 2
    end

    test "returns 0 for empty topic" do
      assert Presence.count("room:count_empty:#{System.unique_integer()}") == 0
    end

    test "count decreases after untrack" do
      topic = "room:count_dec:#{System.unique_integer()}"
      Presence.track(topic, "u1", %{})
      Presence.track(topic, "u2", %{})
      assert Presence.count(topic) == 2

      Presence.untrack(topic, "u1")
      assert Presence.count(topic) == 1
    end
  end

  describe "update/3" do
    test "updates metadata for a tracked presence" do
      topic = "room:update:#{System.unique_integer()}"
      Presence.track(topic, "user_1", %{name: "Alice", status: "online"})
      assert :ok = Presence.update(topic, "user_1", %{name: "Alice", status: "away"})

      presences = Presence.list(topic)
      meta = presences["user_1"]
      status = meta[:status] || meta["status"]
      assert status == "away"
    end

    test "returns {:error, :not_tracked} for untracked key" do
      topic = "room:update_err:#{System.unique_integer()}"
      assert {:error, :not_tracked} = Presence.update(topic, "nobody", %{})
    end
  end

  describe "topics/0" do
    test "returns list of all topics with tracked presences" do
      topic1 = "room:topics_a:#{System.unique_integer()}"
      topic2 = "room:topics_b:#{System.unique_integer()}"
      Presence.track(topic1, "u1", %{})
      Presence.track(topic2, "u2", %{})

      topics = Presence.topics()
      assert topic1 in topics
      assert topic2 in topics
    end
  end

  describe "presence_diff broadcast on join/leave" do
    test "broadcasts presence_diff with joins on track" do
      topic = "room:diff_join:#{System.unique_integer()}"

      # Subscribe to the topic to receive broadcasts
      Registry.register(Neutron.Realtime.Registry, topic, self())

      Presence.track(topic, "user_1", %{name: "Alice"})

      assert_receive {:broadcast, "presence_diff", diff}, 500
      assert Map.has_key?(diff.joins, "user_1") || Map.has_key?(diff[:joins], "user_1")
    end

    test "broadcasts presence_diff with leaves on untrack" do
      topic = "room:diff_leave:#{System.unique_integer()}"
      Presence.track(topic, "user_1", %{name: "Alice"})

      # Subscribe after track to only get the leave diff
      Registry.register(Neutron.Realtime.Registry, topic, self())

      Presence.untrack(topic, "user_1")

      assert_receive {:broadcast, "presence_diff", diff}, 500
      leaves = diff.leaves || diff[:leaves]
      assert Map.has_key?(leaves, "user_1")
    end
  end

  describe "handle_info for DOWN messages (process monitoring)" do
    test "removes presence when tracked process exits normally" do
      topic = "room:monitor_normal:#{System.unique_integer()}"
      test_pid = self()

      pid =
        spawn(fn ->
          Presence.track(topic, "temp_user", %{name: "Temp"})
          send(test_pid, :tracked)

          receive do
            :stop -> :ok
          end
        end)

      assert_receive :tracked, 500
      assert Presence.count(topic) == 1

      send(pid, :stop)
      Process.sleep(100)

      assert Presence.count(topic) == 0
    end

    test "removes presence when tracked process crashes" do
      topic = "room:monitor_crash:#{System.unique_integer()}"
      test_pid = self()

      pid =
        spawn(fn ->
          Presence.track(topic, "crash_user", %{name: "Crashy"})
          send(test_pid, :tracked)

          receive do
            :crash -> raise "intentional crash"
          end
        end)

      assert_receive :tracked, 500
      assert Presence.count(topic) == 1

      Process.exit(pid, :kill)
      Process.sleep(100)

      assert Presence.count(topic) == 0
    end

    test "broadcasts presence_diff with leaves when process dies" do
      topic = "room:monitor_diff:#{System.unique_integer()}"
      test_pid = self()

      # Register to receive broadcasts
      Registry.register(Neutron.Realtime.Registry, topic, self())

      pid =
        spawn(fn ->
          Presence.track(topic, "dying_user", %{name: "Doomed"})
          send(test_pid, :tracked)

          receive do
            :stop -> :ok
          end
        end)

      assert_receive :tracked, 500
      # Drain the join diff message
      assert_receive {:broadcast, "presence_diff", _join_diff}, 500

      Process.exit(pid, :kill)

      assert_receive {:broadcast, "presence_diff", leave_diff}, 500
      leaves = leave_diff.leaves || leave_diff[:leaves]
      assert Map.has_key?(leaves, "dying_user")
    end

    test "does not crash on unknown DOWN messages" do
      # The Presence GenServer should handle unknown :DOWN gracefully
      presence_pid = Process.whereis(Presence)
      assert Process.alive?(presence_pid)

      # Send a fake DOWN with a ref that is not tracked
      fake_ref = make_ref()
      send(presence_pid, {:DOWN, fake_ref, :process, self(), :normal})

      Process.sleep(50)
      assert Process.alive?(presence_pid)
    end
  end
end
