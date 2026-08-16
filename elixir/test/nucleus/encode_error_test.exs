defmodule Nucleus.EncodeErrorTest do
  @moduledoc """
  A client-side parameter encode failure must not kill the client process.

  Postgrex RETURNS server errors as `{:error, %Postgrex.Error{}}` but RAISES
  encode failures — parameter encoding happens before anything reaches the
  socket, so a value its type module cannot encode throws
  `DBConnection.EncodeError` in the caller.

  `Nucleus.Client` is a GenServer, and that raise inside `handle_call/3`
  terminated it, taking the connection down. In a Phoenix application one bad
  parameter on one request killed the client process every other request shares.
  It also broke the documented `{:ok, _} | {:error, _}` contract: the caller
  asked for a tuple and instead had their own process exit through the link.

  Found on 2026-08-15, when fixing the engine's `pg_type` I/O function names
  made Postgrex strict about UUIDs. Before that no extension matched `uuid`,
  every UUID fell through as text, and nothing ever raised here.

  These tests need a live engine (`NEUTRON_TEST_DATABASE_URL`) because the
  failure only exists once a real connection is established — a mock cannot
  raise a real EncodeError. They skip rather than fail when it is absent, and
  the live conformance suite is what guarantees they actually run.
  """
  use ExUnit.Case, async: false

  @url System.get_env("NEUTRON_TEST_DATABASE_URL")
  @moduletag :live

  setup_all do
    if @url in [nil, ""] do
      :ok
    else
      {:ok, client} = Nucleus.Client.start_link(url: @url, name: nil, pool_size: 1)
      {:ok, client: client}
    end
  end

  describe "a parameter the driver cannot encode" do
    @tag :skip_without_live
    test "is returned as an error rather than raised", context do
      client = context[:client]

      if client do
        # Postgrex's UUID extension encodes the raw 16 bytes; a dashed string
        # is the single most likely thing a caller passes instead.
        assert {:error, _} =
                 Nucleus.Client.query(client, "SELECT $1::uuid", [
                   "00000000-0000-0000-0000-000000000000"
                 ])
      end
    end

    test "leaves the client process alive", context do
      client = context[:client]

      if client do
        Nucleus.Client.query(client, "SELECT $1::uuid", ["not-a-uuid-at-all"])
        assert Process.alive?(client), "the encode error killed the client GenServer"
      end
    end

    test "leaves the connection usable for the next query", context do
      client = context[:client]

      if client do
        Nucleus.Client.query(client, "SELECT $1::uuid", ["still-not-a-uuid"])

        assert {:ok, %{rows: [[1]]}} = Nucleus.Client.query(client, "SELECT 1", []),
               "the connection did not survive an encode error"
      end
    end
  end
end
