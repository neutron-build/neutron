defmodule Neutron.Realtime.Socket do
  @moduledoc """
  WebSocket handler for real-time channel communication.

  Handles WebSocket connections and routes messages to appropriate channels.
  Uses the WebSock behaviour for compatibility with both Bandit and Cowboy.

  ## Protocol

  Messages are JSON-encoded with the following structure:

      # Join a channel
      {"topic": "room:lobby", "event": "phx_join", "payload": {}}

      # Send a message
      {"topic": "room:lobby", "event": "new_message", "payload": {"body": "hello"}}

      # Leave a channel
      {"topic": "room:lobby", "event": "phx_leave", "payload": {}}

  ## Usage

  Add the WebSocket route to your router:

      defmodule MyApp.Router do
        use Neutron.Router

        get "/ws" do
          conn
          |> WebSockAdapter.upgrade(Neutron.Realtime.Socket, %{
            channels: %{
              "room:*" => MyApp.RoomChannel,
              "user:*" => MyApp.UserChannel
            }
          }, timeout: 60_000)
        end
      end
  """

  @behaviour WebSock
  require Logger

  @type state :: %{
          channels: %{String.t() => module()},
          joined: %{String.t() => pid()},
          params: map()
        }

  @impl WebSock
  def init(opts) do
    channels = Map.get(opts, :channels, %{})

    state = %{
      channels: channels,
      joined: %{},
      params: Map.delete(opts, :channels)
    }

    {:ok, state}
  end

  @impl WebSock
  def handle_in({text, opcode: :text}, state) do
    case Jason.decode(text) do
      {:ok, %{"topic" => topic, "event" => event, "payload" => payload}} ->
        handle_event(topic, event, payload, state)

      {:ok, _} ->
        reply = Jason.encode!(%{error: "invalid message format"})
        {:push, {:text, reply}, state}

      {:error, _} ->
        reply = Jason.encode!(%{error: "invalid JSON"})
        {:push, {:text, reply}, state}
    end
  end

  def handle_in({_data, opcode: :binary}, state) do
    {:ok, state}
  end

  @impl WebSock
  def handle_info({:channel_push, topic, event, payload}, state) do
    msg = Jason.encode!(%{topic: topic, event: event, payload: payload})
    {:push, {:text, msg}, state}
  end

  def handle_info({:channel_reply, topic, payload}, state) do
    msg = Jason.encode!(%{topic: topic, event: "phx_reply", payload: %{status: "ok", response: payload}})
    {:push, {:text, msg}, state}
  end

  def handle_info({:channel_error, topic, payload}, state) do
    msg = Jason.encode!(%{topic: topic, event: "phx_reply", payload: %{status: "error", response: payload}})
    {:push, {:text, msg}, state}
  end

  def handle_info({:broadcast, event, payload}, state) do
    # Broadcast from a channel — relay to all joined channels
    Enum.each(state.joined, fn {topic, _pid} ->
      msg = Jason.encode!(%{topic: topic, event: event, payload: payload})
      {:push, {:text, msg}, state}
    end)

    {:ok, state}
  end

  def handle_info(_msg, state) do
    {:ok, state}
  end

  @impl WebSock
  def terminate(_reason, state) do
    # Stop all joined channels
    Enum.each(state.joined, fn {_topic, pid} ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal)
    end)

    :ok
  end

  # --- Internal ---

  defp handle_event(topic, "phx_join", payload, state) do
    case find_channel(topic, state.channels) do
      nil ->
        reply = Jason.encode!(%{
          topic: topic,
          event: "phx_reply",
          payload: %{status: "error", response: %{reason: "no channel for topic"}}
        })

        {:push, {:text, reply}, state}

      channel_module ->
        case DynamicSupervisor.start_child(
               Neutron.Realtime.Supervisor,
               {channel_module,
                topic: topic, params: payload, transport_pid: self()}
             ) do
          {:ok, pid} ->
            joined = Map.put(state.joined, topic, pid)

            reply = Jason.encode!(%{
              topic: topic,
              event: "phx_reply",
              payload: %{status: "ok", response: %{}}
            })

            {:push, {:text, reply}, %{state | joined: joined}}

          {:error, {:join_rejected, reason}} ->
            reply = Jason.encode!(%{
              topic: topic,
              event: "phx_reply",
              payload: %{status: "error", response: reason}
            })

            {:push, {:text, reply}, state}

          {:error, reason} ->
            Logger.error("Channel join failed: #{inspect(reason)}")

            reply = Jason.encode!(%{
              topic: topic,
              event: "phx_reply",
              payload: %{status: "error", response: %{reason: "join failed"}}
            })

            {:push, {:text, reply}, state}
        end
    end
  end

  defp handle_event(topic, "phx_leave", _payload, state) do
    case Map.pop(state.joined, topic) do
      {nil, _} ->
        {:ok, state}

      {pid, joined} ->
        if Process.alive?(pid), do: GenServer.stop(pid, :normal)

        reply = Jason.encode!(%{
          topic: topic,
          event: "phx_reply",
          payload: %{status: "ok", response: %{}}
        })

        {:push, {:text, reply}, %{state | joined: joined}}
    end
  end

  defp handle_event(topic, event, payload, state) do
    case Map.get(state.joined, topic) do
      nil ->
        reply = Jason.encode!(%{
          topic: topic,
          event: "phx_reply",
          payload: %{status: "error", response: %{reason: "not joined"}}
        })

        {:push, {:text, reply}, state}

      pid ->
        GenServer.cast(pid, {:incoming, event, payload})
        {:ok, state}
    end
  end

  defp find_channel(topic, channels) do
    # Try exact match first
    case Map.get(channels, topic) do
      nil ->
        # Try wildcard match: "room:*" matches "room:lobby"
        Enum.find_value(channels, fn {pattern, module} ->
          if matches_pattern?(topic, pattern), do: module
        end)

      module ->
        module
    end
  end

  defp matches_pattern?(topic, pattern) do
    case String.split(pattern, "*", parts: 2) do
      [prefix, ""] -> String.starts_with?(topic, prefix)
      [^topic] -> true
      _ -> false
    end
  end
end
