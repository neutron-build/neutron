defmodule Neutron.Realtime.ChannelTest do
  use ExUnit.Case, async: true

  alias Neutron.Realtime.Channel

  setup do
    # Ensure the registry is running for broadcast tests
    case Process.whereis(Neutron.Realtime.Registry) do
      nil ->
        {:ok, _} = Registry.start_link(keys: :duplicate, name: Neutron.Realtime.Registry)

      _ ->
        :ok
    end

    :ok
  end

  # --- Test channel module that accepts all joins ---
  defmodule AcceptChannel do
    use Neutron.Realtime.Channel

    @impl Neutron.Realtime.Channel
    def join(_topic, _params, socket) do
      {:ok, assign(socket, :joined, true)}
    end

    @impl Neutron.Realtime.Channel
    def handle_in("echo", payload, socket) do
      {:reply, {:ok, payload}, socket}
    end

    def handle_in("echo_error", payload, socket) do
      {:reply, {:error, payload}, socket}
    end

    def handle_in("broadcast", payload, socket) do
      broadcast(socket, "broadcasted", payload)
      {:noreply, socket}
    end

    def handle_in("broadcast_from", payload, socket) do
      broadcast_from(socket, "broadcasted_from", payload)
      {:noreply, socket}
    end

    def handle_in("stop", _payload, socket) do
      {:stop, :normal, socket}
    end

    def handle_in(_event, _payload, socket) do
      {:noreply, socket}
    end
  end

  # --- Test channel module that rejects joins ---
  defmodule RejectChannel do
    use Neutron.Realtime.Channel

    @impl Neutron.Realtime.Channel
    def join(_topic, _params, _socket) do
      {:error, %{reason: "unauthorized"}}
    end

    @impl Neutron.Realtime.Channel
    def handle_in(_event, _payload, socket) do
      {:noreply, socket}
    end
  end

  # --- Test channel with selective topic join ---
  defmodule SelectiveChannel do
    use Neutron.Realtime.Channel

    @impl Neutron.Realtime.Channel
    def join("allowed:" <> _rest, _params, socket) do
      {:ok, socket}
    end

    def join(_topic, _params, _socket) do
      {:error, %{reason: "topic not allowed"}}
    end

    @impl Neutron.Realtime.Channel
    def handle_in(_event, _payload, socket) do
      {:noreply, socket}
    end
  end

  describe "join (valid topic, returns {:ok, state})" do
    test "channel starts and returns {:ok, socket} on valid join" do
      topic = "test:join:valid:#{System.unique_integer()}"

      {:ok, pid} =
        AcceptChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      assert Process.alive?(pid)
      GenServer.stop(pid, :normal)
    end

    test "channel assigns are set after successful join" do
      topic = "test:join:assigns:#{System.unique_integer()}"

      {:ok, pid} =
        AcceptChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      # The channel set :joined => true in its join callback
      state = :sys.get_state(pid)
      assert state.assigns[:joined] == true

      GenServer.stop(pid, :normal)
    end

    test "channel registers itself in the Registry" do
      topic = "test:join:registry:#{System.unique_integer()}"

      {:ok, pid} =
        AcceptChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      entries = Registry.lookup(Neutron.Realtime.Registry, topic)
      assert length(entries) > 0

      GenServer.stop(pid, :normal)
    end
  end

  describe "join rejection (invalid topic)" do
    test "channel stops when join returns {:error, reason}" do
      topic = "test:reject:#{System.unique_integer()}"

      result =
        RejectChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      # GenServer.start_link returns {:error, ...} when init returns {:stop, ...}
      assert {:error, {:join_rejected, %{reason: "unauthorized"}}} = result
    end

    test "selective channel rejects disallowed topics" do
      topic = "denied:#{System.unique_integer()}"

      result =
        SelectiveChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      assert {:error, {:join_rejected, %{reason: "topic not allowed"}}} = result
    end

    test "selective channel accepts allowed topics" do
      topic = "allowed:#{System.unique_integer()}"

      {:ok, pid} =
        SelectiveChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      assert Process.alive?(pid)
      GenServer.stop(pid, :normal)
    end
  end

  describe "handle_in dispatches to correct handler" do
    setup do
      topic = "test:handle_in:#{System.unique_integer()}"

      {:ok, pid} =
        AcceptChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid, :normal)
      end)

      {:ok, pid: pid, topic: topic}
    end

    test "echo event replies with :ok and payload", %{pid: pid} do
      GenServer.cast(pid, {:incoming, "echo", %{message: "hello"}})

      assert_receive {:channel_reply, _topic, %{message: "hello"}}, 500
    end

    test "echo_error event replies with :error", %{pid: pid} do
      GenServer.cast(pid, {:incoming, "echo_error", %{reason: "bad"}})

      assert_receive {:channel_error, _topic, %{reason: "bad"}}, 500
    end

    test "unknown event is handled without crash", %{pid: pid} do
      GenServer.cast(pid, {:incoming, "unknown_event", %{}})

      # Channel should still be alive
      Process.sleep(50)
      assert Process.alive?(pid)
    end

    test "stop event terminates the channel", %{pid: pid} do
      ref = Process.monitor(pid)
      GenServer.cast(pid, {:incoming, "stop", %{}})

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 500
    end
  end

  describe "broadcast sends to all subscribers" do
    test "broadcast/3 sends to all registered processes for a topic" do
      topic = "test:broadcast:all:#{System.unique_integer()}"

      # Register the test process as a subscriber
      Registry.register(Neutron.Realtime.Registry, topic, self())

      Channel.broadcast(topic, "my_event", %{data: "hello"})

      assert_receive {:broadcast, "my_event", %{data: "hello"}}, 200
    end

    test "broadcast/3 sends to multiple subscribers" do
      topic = "test:broadcast:multi:#{System.unique_integer()}"

      # Register this process twice (simulating two subscribers)
      Registry.register(Neutron.Realtime.Registry, topic, self())

      # Spawn another process that registers and forwards messages
      test_pid = self()

      other_pid =
        spawn(fn ->
          Registry.register(Neutron.Realtime.Registry, topic, self())
          send(test_pid, :registered)

          receive do
            msg -> send(test_pid, {:other_received, msg})
          end
        end)

      assert_receive :registered, 200

      Channel.broadcast(topic, "multi_event", %{})

      assert_receive {:broadcast, "multi_event", %{}}, 200
      assert_receive {:other_received, {:broadcast, "multi_event", %{}}}, 200

      Process.exit(other_pid, :kill)
    end

    test "broadcast/3 returns :ok when no subscribers exist" do
      assert :ok = Channel.broadcast("nonexistent:topic", "event", %{})
    end

    test "channel's broadcast helper sends to topic subscribers", %{} do
      topic = "test:broadcast:helper:#{System.unique_integer()}"

      # Register a "listener" process
      test_pid = self()

      listener =
        spawn(fn ->
          Registry.register(Neutron.Realtime.Registry, topic, self())
          send(test_pid, :listener_ready)

          receive do
            msg -> send(test_pid, {:listener_got, msg})
          end
        end)

      assert_receive :listener_ready, 200

      {:ok, channel_pid} =
        AcceptChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      GenServer.cast(channel_pid, {:incoming, "broadcast", %{text: "hi"}})

      assert_receive {:listener_got, {:broadcast, "broadcasted", %{text: "hi"}}}, 500

      Process.exit(listener, :kill)
      GenServer.stop(channel_pid, :normal)
    end
  end

  describe "broadcast_from excludes sender" do
    test "broadcast_from/4 excludes sender pid" do
      topic = "test:from:exclude:#{System.unique_integer()}"
      Registry.register(Neutron.Realtime.Registry, topic, self())

      Channel.broadcast_from(self(), topic, "event", %{data: "test"})

      refute_receive {:broadcast, "event", _}, 100
    end

    test "broadcast_from/4 sends to non-sender subscribers" do
      topic = "test:from:others:#{System.unique_integer()}"
      test_pid = self()

      other_pid =
        spawn(fn ->
          Registry.register(Neutron.Realtime.Registry, topic, self())
          send(test_pid, :ready)

          receive do
            msg -> send(test_pid, {:other_got, msg})
          end
        end)

      assert_receive :ready, 200

      # Sender is self(), other_pid should still get it
      Registry.register(Neutron.Realtime.Registry, topic, self())
      Channel.broadcast_from(self(), topic, "event", %{data: "test"})

      # self() should NOT receive
      refute_receive {:broadcast, "event", _}, 100
      # other should receive
      assert_receive {:other_got, {:broadcast, "event", %{data: "test"}}}, 200

      Process.exit(other_pid, :kill)
    end

    test "broadcast_from/4 returns :ok" do
      assert :ok = Channel.broadcast_from(self(), "no-topic", "event", %{})
    end
  end

  describe "leave cleans up state" do
    test "channel cleans up when transport process exits" do
      topic = "test:leave:#{System.unique_integer()}"

      # Spawn a fake transport process
      transport =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      {:ok, channel_pid} =
        AcceptChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: transport
        )

      ref = Process.monitor(channel_pid)
      assert Process.alive?(channel_pid)

      # Kill the transport -- channel should stop via :DOWN monitor
      Process.exit(transport, :kill)

      assert_receive {:DOWN, ^ref, :process, ^channel_pid, :normal}, 500
    end

    test "channel stops when GenServer.stop is called" do
      topic = "test:leave:stop:#{System.unique_integer()}"

      {:ok, pid} =
        AcceptChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      ref = Process.monitor(pid)
      GenServer.stop(pid, :normal)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 500
    end
  end

  describe "assign/3" do
    test "sets a key-value pair in socket assigns" do
      socket = %{assigns: %{}, topic: "t", transport_pid: self(), channel: AcceptChannel, serializer: Jason}
      updated = AcceptChannel.assign(socket, :user_id, 42)
      assert updated.assigns.user_id == 42
    end

    test "overwrites existing assign" do
      socket = %{assigns: %{name: "old"}, topic: "t", transport_pid: self(), channel: AcceptChannel, serializer: Jason}
      updated = AcceptChannel.assign(socket, :name, "new")
      assert updated.assigns.name == "new"
    end
  end

  describe "push/3" do
    test "sends a message directly to the transport process" do
      topic = "test:push:#{System.unique_integer()}"

      {:ok, pid} =
        AcceptChannel.start_link(
          topic: topic,
          params: %{},
          transport_pid: self()
        )

      state = :sys.get_state(pid)
      AcceptChannel.push(state, "direct_event", %{data: "pushed"})

      assert_receive {:channel_push, ^topic, "direct_event", %{data: "pushed"}}, 200

      GenServer.stop(pid, :normal)
    end
  end
end
