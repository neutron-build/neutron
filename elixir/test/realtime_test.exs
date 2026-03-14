defmodule Neutron.Realtime.SocketTest do
  use ExUnit.Case

  alias Neutron.Realtime.Socket

  describe "init/1" do
    test "initializes state with channels and empty joined map" do
      opts = %{channels: %{"room:*" => SomeModule}}
      assert {:ok, state} = Socket.init(opts)
      assert state.channels == %{"room:*" => SomeModule}
      assert state.joined == %{}
      assert state.params == %{}
    end

    test "initializes with empty channels when none provided" do
      assert {:ok, state} = Socket.init(%{})
      assert state.channels == %{}
      assert state.joined == %{}
    end

    test "separates non-channel params" do
      opts = %{channels: %{"room:*" => SomeModule}, user_id: 42}
      assert {:ok, state} = Socket.init(opts)
      assert state.params == %{user_id: 42}
    end
  end

  describe "handle_in/2 text messages" do
    test "returns error for invalid JSON" do
      {:ok, state} = Socket.init(%{})
      result = Socket.handle_in({"not json", opcode: :text}, state)
      assert {:push, {:text, reply}, ^state} = result
      decoded = Jason.decode!(reply)
      assert decoded["error"] == "invalid JSON"
    end

    test "returns error for message missing required fields" do
      {:ok, state} = Socket.init(%{})
      msg = Jason.encode!(%{only: "partial"})
      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, ^state} = result
      decoded = Jason.decode!(reply)
      assert decoded["error"] == "invalid message format"
    end

    test "returns error when sending to unjoined topic" do
      {:ok, state} = Socket.init(%{channels: %{"room:*" => SomeModule}})
      msg = Jason.encode!(%{"topic" => "room:lobby", "event" => "chat", "payload" => %{}})
      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, _state} = result
      decoded = Jason.decode!(reply)
      assert decoded["payload"]["status"] == "error"
    end
  end

  describe "handle_in/2 binary messages" do
    test "ignores binary messages" do
      {:ok, state} = Socket.init(%{})
      assert {:ok, ^state} = Socket.handle_in({"binary data", opcode: :binary}, state)
    end
  end

  describe "handle_info/2" do
    test "handles :channel_push messages" do
      {:ok, state} = Socket.init(%{})
      msg = {:channel_push, "room:lobby", "new_msg", %{body: "hello"}}
      assert {:push, {:text, reply}, ^state} = Socket.handle_info(msg, state)
      decoded = Jason.decode!(reply)
      assert decoded["topic"] == "room:lobby"
      assert decoded["event"] == "new_msg"
      assert decoded["payload"]["body"] == "hello"
    end

    test "handles :channel_reply messages" do
      {:ok, state} = Socket.init(%{})
      msg = {:channel_reply, "room:lobby", %{data: "ok"}}
      assert {:push, {:text, reply}, ^state} = Socket.handle_info(msg, state)
      decoded = Jason.decode!(reply)
      assert decoded["topic"] == "room:lobby"
      assert decoded["event"] == "phx_reply"
      assert decoded["payload"]["status"] == "ok"
    end

    test "handles :channel_error messages" do
      {:ok, state} = Socket.init(%{})
      msg = {:channel_error, "room:lobby", %{reason: "oops"}}
      assert {:push, {:text, reply}, ^state} = Socket.handle_info(msg, state)
      decoded = Jason.decode!(reply)
      assert decoded["payload"]["status"] == "error"
    end

    test "handles unknown messages" do
      {:ok, state} = Socket.init(%{})
      assert {:ok, ^state} = Socket.handle_info(:unknown, state)
    end
  end

  describe "terminate/2" do
    test "returns :ok when no channels joined" do
      {:ok, state} = Socket.init(%{})
      assert :ok = Socket.terminate(:normal, state)
    end
  end

  describe "pattern matching" do
    test "matches wildcard pattern" do
      {:ok, state} = Socket.init(%{channels: %{"room:*" => SomeModule}})
      # The find_channel function is private, but we can test it through
      # a phx_join event that attempts channel lookup
      msg = Jason.encode!(%{
        "topic" => "room:lobby",
        "event" => "phx_join",
        "payload" => %{}
      })

      # This will fail because DynamicSupervisor isn't running,
      # but it will show that the pattern matched (not "no channel for topic")
      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, _state} = result
      decoded = Jason.decode!(reply)
      # If the channel was found, it would try to start the child and may fail
      # but the error would NOT be "no channel for topic"
      # It will be "join failed" or similar since we don't have the supervisor
      assert decoded["payload"]["status"] == "error"
      assert decoded["payload"]["response"]["reason"] != "no channel for topic"
    end

    test "returns error for unmatched topic" do
      {:ok, state} = Socket.init(%{channels: %{"chat:*" => SomeModule}})
      msg = Jason.encode!(%{
        "topic" => "room:lobby",
        "event" => "phx_join",
        "payload" => %{}
      })

      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, _state} = result
      decoded = Jason.decode!(reply)
      assert decoded["payload"]["response"]["reason"] == "no channel for topic"
    end
  end
end

defmodule Neutron.Realtime.ChannelTest do
  use ExUnit.Case, async: true

  alias Neutron.Realtime.Channel

  describe "broadcast/3" do
    setup do
      # We need the registry running for broadcast
      case Process.whereis(Neutron.Realtime.Registry) do
        nil ->
          {:ok, _} = Registry.start_link(keys: :duplicate, name: Neutron.Realtime.Registry)
        _ ->
          :ok
      end

      :ok
    end

    test "broadcasts to registered processes" do
      topic = "test:broadcast:#{System.unique_integer()}"
      Registry.register(Neutron.Realtime.Registry, topic, self())

      Channel.broadcast(topic, "event", %{data: "hello"})

      assert_receive {:broadcast, "event", %{data: "hello"}}, 100
    end

    test "returns :ok" do
      assert :ok = Channel.broadcast("no-subscribers", "event", %{})
    end
  end

  describe "broadcast_from/4" do
    setup do
      case Process.whereis(Neutron.Realtime.Registry) do
        nil ->
          {:ok, _} = Registry.start_link(keys: :duplicate, name: Neutron.Realtime.Registry)
        _ ->
          :ok
      end

      :ok
    end

    test "broadcasts to others but not sender" do
      topic = "test:broadcast_from:#{System.unique_integer()}"

      # Register as a subscriber
      Registry.register(Neutron.Realtime.Registry, topic, self())

      # Broadcast from self — should NOT receive the message
      Channel.broadcast_from(self(), topic, "event", %{data: "hello"})

      refute_receive {:broadcast, "event", _}, 100
    end

    test "returns :ok" do
      assert :ok = Channel.broadcast_from(self(), "topic", "event", %{})
    end
  end
end

defmodule Neutron.Realtime.PresenceTest do
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

  describe "track/3" do
    test "tracks a presence in a topic" do
      topic = "room:#{System.unique_integer()}"
      assert :ok = Presence.track(topic, "user_1", %{name: "Alice"})
      presences = Presence.list(topic)
      assert Map.has_key?(presences, "user_1")
      assert presences["user_1"].name == "Alice" || presences["user_1"]["name"] == "Alice"
    end

    test "replaces previous entry for same key" do
      topic = "room:#{System.unique_integer()}"
      Presence.track(topic, "user_1", %{name: "Alice"})
      Presence.track(topic, "user_1", %{name: "Alice Updated"})
      presences = Presence.list(topic)
      assert map_size(presences) == 1
    end
  end

  describe "untrack/2" do
    test "removes a tracked presence" do
      topic = "room:#{System.unique_integer()}"
      Presence.track(topic, "user_1", %{name: "Alice"})
      assert :ok = Presence.untrack(topic, "user_1")
      presences = Presence.list(topic)
      refute Map.has_key?(presences, "user_1")
    end

    test "returns :ok for non-tracked key" do
      topic = "room:#{System.unique_integer()}"
      assert :ok = Presence.untrack(topic, "nobody")
    end
  end

  describe "update/3" do
    test "updates metadata for a tracked presence" do
      topic = "room:#{System.unique_integer()}"
      Presence.track(topic, "user_1", %{name: "Alice"})
      assert :ok = Presence.update(topic, "user_1", %{name: "Alice V2", status: "away"})
      presences = Presence.list(topic)
      meta = presences["user_1"]
      assert meta[:name] == "Alice V2" || meta["name"] == "Alice V2"
    end

    test "returns {:error, :not_tracked} for untracked key" do
      topic = "room:#{System.unique_integer()}"
      assert {:error, :not_tracked} = Presence.update(topic, "nobody", %{})
    end
  end

  describe "list/1" do
    test "returns presences for a topic" do
      topic = "room:#{System.unique_integer()}"
      Presence.track(topic, "user_1", %{name: "Alice"})
      Presence.track(topic, "user_2", %{name: "Bob"})
      presences = Presence.list(topic)
      assert map_size(presences) == 2
      assert Map.has_key?(presences, "user_1")
      assert Map.has_key?(presences, "user_2")
    end

    test "returns empty map for empty topic" do
      topic = "room:#{System.unique_integer()}"
      assert Presence.list(topic) == %{}
    end
  end

  describe "count/1" do
    test "returns the count of presences" do
      topic = "room:#{System.unique_integer()}"
      Presence.track(topic, "u1", %{})
      Presence.track(topic, "u2", %{})
      Presence.track(topic, "u3", %{})
      assert Presence.count(topic) == 3
    end

    test "returns 0 for empty topic" do
      assert Presence.count("room:empty:#{System.unique_integer()}") == 0
    end
  end

  describe "topics/0" do
    test "returns list of topics with presences" do
      topic1 = "room:topics1:#{System.unique_integer()}"
      topic2 = "room:topics2:#{System.unique_integer()}"
      Presence.track(topic1, "u1", %{})
      Presence.track(topic2, "u2", %{})
      topics = Presence.topics()
      assert topic1 in topics
      assert topic2 in topics
    end
  end

  describe "process monitoring" do
    test "removes presence when tracked process exits" do
      topic = "room:monitor:#{System.unique_integer()}"

      # Spawn a process that tracks itself
      pid = spawn(fn ->
        Presence.track(topic, "temp_user", %{name: "Temp"})
        receive do
          :stop -> :ok
        end
      end)

      Process.sleep(50)
      assert Presence.count(topic) == 1

      # Kill the process
      send(pid, :stop)
      Process.sleep(100)

      # Presence should be cleaned up
      assert Presence.count(topic) == 0
    end
  end
end
