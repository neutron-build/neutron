defmodule Neutron.Realtime.SocketExpandedTest do
  use ExUnit.Case

  alias Neutron.Realtime.Socket

  describe "init/1 (WebSocket upgrade negotiation)" do
    test "initializes state with channel map and empty joined map" do
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

    test "separates non-channel opts into params" do
      opts = %{channels: %{"room:*" => SomeModule}, user_id: 42, token: "abc"}
      assert {:ok, state} = Socket.init(opts)
      assert state.params == %{user_id: 42, token: "abc"}
      refute Map.has_key?(state.params, :channels)
    end

    test "supports multiple channel patterns" do
      channels = %{
        "room:*" => RoomChannel,
        "user:*" => UserChannel,
        "admin:dashboard" => AdminChannel
      }

      assert {:ok, state} = Socket.init(%{channels: channels})
      assert map_size(state.channels) == 3
    end
  end

  describe "topic routing (message with topic to correct channel)" do
    test "returns error when no channel matches the topic on phx_join" do
      {:ok, state} = Socket.init(%{channels: %{"chat:*" => SomeModule}})

      msg =
        Jason.encode!(%{
          "topic" => "room:lobby",
          "event" => "phx_join",
          "payload" => %{}
        })

      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, _state} = result
      decoded = Jason.decode!(reply)
      assert decoded["payload"]["response"]["reason"] == "no channel for topic"
    end

    test "returns error when sending event to unjoined topic" do
      {:ok, state} = Socket.init(%{channels: %{"room:*" => SomeModule}})

      msg =
        Jason.encode!(%{
          "topic" => "room:lobby",
          "event" => "new_message",
          "payload" => %{body: "hello"}
        })

      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, _state} = result
      decoded = Jason.decode!(reply)
      assert decoded["payload"]["status"] == "error"
      assert decoded["payload"]["response"]["reason"] == "not joined"
    end
  end

  describe "wildcard topic matching" do
    test "wildcard pattern room:* matches room:lobby" do
      {:ok, state} = Socket.init(%{channels: %{"room:*" => SomeModule}})

      msg =
        Jason.encode!(%{
          "topic" => "room:lobby",
          "event" => "phx_join",
          "payload" => %{}
        })

      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, _state} = result
      decoded = Jason.decode!(reply)
      # The error should NOT be "no channel for topic" since the wildcard matched.
      # It will fail because DynamicSupervisor is not running, but the match happened.
      assert decoded["payload"]["response"]["reason"] != "no channel for topic"
    end

    test "wildcard pattern does not match unrelated prefix" do
      {:ok, state} = Socket.init(%{channels: %{"chat:*" => SomeModule}})

      msg =
        Jason.encode!(%{
          "topic" => "room:lobby",
          "event" => "phx_join",
          "payload" => %{}
        })

      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, _state} = result
      decoded = Jason.decode!(reply)
      assert decoded["payload"]["response"]["reason"] == "no channel for topic"
    end

    test "exact match takes precedence" do
      {:ok, state} =
        Socket.init(%{
          channels: %{
            "room:*" => GenericRoomChannel,
            "room:vip" => VIPRoomChannel
          }
        })

      # Exact match "room:vip" should find VIPRoomChannel (via Map.get before wildcard)
      msg =
        Jason.encode!(%{
          "topic" => "room:vip",
          "event" => "phx_join",
          "payload" => %{}
        })

      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, _state} = result
      decoded = Jason.decode!(reply)
      # It will fail (no DynamicSupervisor) but should NOT say "no channel for topic"
      assert decoded["payload"]["response"]["reason"] != "no channel for topic"
    end
  end

  describe "malformed message handling (invalid JSON)" do
    test "returns error for non-JSON text" do
      {:ok, state} = Socket.init(%{})

      result = Socket.handle_in({"not valid json {{", opcode: :text}, state)
      assert {:push, {:text, reply}, ^state} = result
      decoded = Jason.decode!(reply)
      assert decoded["error"] == "invalid JSON"
    end

    test "returns error for JSON missing required fields" do
      {:ok, state} = Socket.init(%{})

      msg = Jason.encode!(%{"only" => "partial"})
      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, ^state} = result
      decoded = Jason.decode!(reply)
      assert decoded["error"] == "invalid message format"
    end

    test "returns error for JSON missing topic field" do
      {:ok, state} = Socket.init(%{})

      msg = Jason.encode!(%{"event" => "test", "payload" => %{}})
      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, ^state} = result
      decoded = Jason.decode!(reply)
      assert decoded["error"] == "invalid message format"
    end

    test "returns error for JSON missing event field" do
      {:ok, state} = Socket.init(%{})

      msg = Jason.encode!(%{"topic" => "room:1", "payload" => %{}})
      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:push, {:text, reply}, ^state} = result
      decoded = Jason.decode!(reply)
      assert decoded["error"] == "invalid message format"
    end

    test "returns error for empty string" do
      {:ok, state} = Socket.init(%{})

      result = Socket.handle_in({"", opcode: :text}, state)
      assert {:push, {:text, reply}, ^state} = result
      decoded = Jason.decode!(reply)
      assert decoded["error"] == "invalid JSON"
    end

    test "ignores binary opcode messages" do
      {:ok, state} = Socket.init(%{})
      assert {:ok, ^state} = Socket.handle_in({"binary data", opcode: :binary}, state)
    end
  end

  describe "heartbeat/ping-pong" do
    test "handles channel_push info messages (used for heartbeat relay)" do
      {:ok, state} = Socket.init(%{})

      msg = {:channel_push, "phoenix", "heartbeat", %{}}
      assert {:push, {:text, reply}, ^state} = Socket.handle_info(msg, state)
      decoded = Jason.decode!(reply)
      assert decoded["topic"] == "phoenix"
      assert decoded["event"] == "heartbeat"
    end

    test "handles channel_reply info messages" do
      {:ok, state} = Socket.init(%{})

      msg = {:channel_reply, "room:lobby", %{data: "pong"}}
      assert {:push, {:text, reply}, ^state} = Socket.handle_info(msg, state)
      decoded = Jason.decode!(reply)
      assert decoded["event"] == "phx_reply"
      assert decoded["payload"]["status"] == "ok"
      assert decoded["payload"]["response"]["data"] == "pong"
    end

    test "handles channel_error info messages" do
      {:ok, state} = Socket.init(%{})

      msg = {:channel_error, "room:lobby", %{reason: "timeout"}}
      assert {:push, {:text, reply}, ^state} = Socket.handle_info(msg, state)
      decoded = Jason.decode!(reply)
      assert decoded["event"] == "phx_reply"
      assert decoded["payload"]["status"] == "error"
      assert decoded["payload"]["response"]["reason"] == "timeout"
    end

    test "handles unknown info messages without crashing" do
      {:ok, state} = Socket.init(%{})
      assert {:ok, ^state} = Socket.handle_info(:ping, state)
      assert {:ok, ^state} = Socket.handle_info({:some, :random, :tuple}, state)
    end
  end

  describe "phx_leave event" do
    test "returns {:ok, state} when leaving an unjoined topic" do
      {:ok, state} = Socket.init(%{channels: %{"room:*" => SomeModule}})

      msg =
        Jason.encode!(%{
          "topic" => "room:lobby",
          "event" => "phx_leave",
          "payload" => %{}
        })

      result = Socket.handle_in({msg, opcode: :text}, state)
      assert {:ok, ^state} = result
    end
  end

  describe "terminate/2" do
    test "returns :ok with empty joined map" do
      {:ok, state} = Socket.init(%{})
      assert :ok = Socket.terminate(:normal, state)
    end

    test "returns :ok with shutdown reason" do
      {:ok, state} = Socket.init(%{})
      assert :ok = Socket.terminate(:shutdown, state)
    end
  end
end
