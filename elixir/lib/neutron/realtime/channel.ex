defmodule Neutron.Realtime.Channel do
  @moduledoc """
  Phoenix Channel-style pub/sub with topic routing.

  Channels provide real-time communication over WebSockets. Each channel
  is a GenServer process that manages subscriptions, broadcasts, and
  per-topic message routing.

  ## Defining a Channel

      defmodule MyApp.RoomChannel do
        use Neutron.Realtime.Channel

        @impl true
        def join("room:" <> room_id, params, socket) do
          # Authorize the join
          if authorized?(params) do
            {:ok, assign(socket, :room_id, room_id)}
          else
            {:error, %{reason: "unauthorized"}}
          end
        end

        @impl true
        def handle_in("new_message", payload, socket) do
          broadcast(socket, "new_message", payload)
          {:noreply, socket}
        end

        @impl true
        def handle_in("typing", payload, socket) do
          broadcast_from(socket, "typing", payload)
          {:noreply, socket}
        end
      end

  ## Socket State

  Each channel has a `socket` struct with:
  - `assigns` — user-defined state
  - `topic` — the subscribed topic
  - `transport_pid` — the WebSocket transport process
  """

  @type socket :: %{
          assigns: map(),
          topic: String.t(),
          transport_pid: pid(),
          channel: module(),
          serializer: module()
        }

  @callback join(topic :: String.t(), params :: map(), socket()) ::
              {:ok, socket()} | {:error, map()}

  @callback handle_in(event :: String.t(), payload :: map(), socket()) ::
              {:noreply, socket()}
              | {:reply, {:ok, map()} | {:error, map()}, socket()}
              | {:stop, term(), socket()}

  @callback handle_info(msg :: term(), socket()) ::
              {:noreply, socket()} | {:stop, term(), socket()}

  @callback terminate(reason :: term(), socket()) :: term()

  @optional_callbacks [handle_info: 2, terminate: 2]

  defmacro __using__(_opts) do
    quote do
      @behaviour Neutron.Realtime.Channel
      use GenServer
      require Logger

      @impl Neutron.Realtime.Channel
      def handle_info(_msg, socket), do: {:noreply, socket}

      @impl Neutron.Realtime.Channel
      def terminate(_reason, _socket), do: :ok

      defoverridable handle_info: 2, terminate: 2

      # --- GenServer Implementation ---

      def start_link(opts) do
        GenServer.start_link(__MODULE__, opts)
      end

      @impl GenServer
      def init(opts) do
        topic = Keyword.fetch!(opts, :topic)
        params = Keyword.get(opts, :params, %{})
        transport_pid = Keyword.fetch!(opts, :transport_pid)

        socket = %{
          assigns: %{},
          topic: topic,
          transport_pid: transport_pid,
          channel: __MODULE__,
          serializer: Jason
        }

        # Subscribe to the topic via Registry
        Registry.register(Neutron.Realtime.Registry, topic, self())

        # Monitor the transport process
        Process.monitor(transport_pid)

        case join(topic, params, socket) do
          {:ok, socket} ->
            {:ok, socket}

          {:error, reason} ->
            {:stop, {:join_rejected, reason}}
        end
      end

      @impl GenServer
      def handle_cast({:incoming, event, payload}, socket) do
        case handle_in(event, payload, socket) do
          {:noreply, socket} ->
            {:noreply, socket}

          {:reply, {:ok, reply}, socket} ->
            send(socket.transport_pid, {:channel_reply, socket.topic, reply})
            {:noreply, socket}

          {:reply, {:error, reply}, socket} ->
            send(socket.transport_pid, {:channel_error, socket.topic, reply})
            {:noreply, socket}

          {:stop, reason, socket} ->
            {:stop, reason, socket}
        end
      end

      @impl GenServer
      def handle_info({:DOWN, _ref, :process, pid, _reason}, %{transport_pid: pid} = socket) do
        # Transport disconnected — shut down the channel
        {:stop, :normal, socket}
      end

      def handle_info(msg, socket) do
        case __MODULE__.handle_info(msg, socket) do
          {:noreply, socket} -> {:noreply, socket}
          {:stop, reason, socket} -> {:stop, reason, socket}
        end
      end

      # --- Channel Helpers ---

      @doc "Assigns a key-value pair to the socket."
      def assign(socket, key, value) do
        %{socket | assigns: Map.put(socket.assigns, key, value)}
      end

      @doc "Broadcasts a message to all subscribers of the socket's topic."
      def broadcast(socket, event, payload) do
        Neutron.Realtime.Channel.broadcast(socket.topic, event, payload)
      end

      @doc "Broadcasts a message to all subscribers except the sender."
      def broadcast_from(socket, event, payload) do
        Neutron.Realtime.Channel.broadcast_from(self(), socket.topic, event, payload)
      end

      @doc "Pushes a message directly to this socket's transport."
      def push(socket, event, payload) do
        send(socket.transport_pid, {:channel_push, socket.topic, event, payload})
      end
    end
  end

  @doc """
  Broadcasts an event to all processes subscribed to a topic.
  """
  @spec broadcast(String.t(), String.t(), map()) :: :ok
  def broadcast(topic, event, payload) do
    Registry.dispatch(Neutron.Realtime.Registry, topic, fn entries ->
      for {_pid, channel_pid} <- entries do
        send(channel_pid, {:broadcast, event, payload})
      end
    end)

    :ok
  end

  @doc """
  Broadcasts an event to all subscribers except the sender.
  """
  @spec broadcast_from(pid(), String.t(), String.t(), map()) :: :ok
  def broadcast_from(sender_pid, topic, event, payload) do
    Registry.dispatch(Neutron.Realtime.Registry, topic, fn entries ->
      for {_pid, channel_pid} <- entries, channel_pid != sender_pid do
        send(channel_pid, {:broadcast, event, payload})
      end
    end)

    :ok
  end
end
