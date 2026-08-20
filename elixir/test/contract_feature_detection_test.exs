defmodule Neutron.Contract.FeatureDetectionTest do
  # FRAMEWORK_CONTRACT §1: connect over pgwire, run SELECT VERSION(), decide
  # Nucleus vs plain PostgreSQL, and behave correctly either way.
  #
  # The SELECT VERSION() round-trip needs a live server; the PARSING is pure
  # and is what this file pins. It drives `Nucleus.Client.parse_version/1`,
  # the function `detect_features/1` feeds the pgwire result into — not a
  # re-implementation in the test.
  use ExUnit.Case, async: false

  alias Nucleus.Client
  alias Nucleus.Client.Features

  @nucleus_version_string "PostgreSQL 16.0 (Nucleus 0.9.2 — The Definitive Database)"
  @plain_pg_version_string "PostgreSQL 16.4 (Debian 16.4-1.pgdg120+1)"

  @model_caps ~w(kv vector ts document graph fts geo blob streams columnar datalog cdc pubsub)a

  # Serves the same :is_nucleus? GenServer.call the real client does, backed
  # by a parsed plain-PG feature set. Lets require_nucleus/2 run its real path
  # without a database.
  defmodule PlainPgStub do
    use GenServer, restart: :temporary

    def start_link(opts),
      do: GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :version), name: __MODULE__)

    @impl true
    def init(version_string),
      do: {:ok, Client.parse_version(version_string)}

    @impl true
    def handle_call(:is_nucleus?, _from, %Features{} = features),
      do: {:reply, features.is_nucleus, features}
  end

  describe "parse_version/1 — Nucleus peer" do
    test "sets is_nucleus and every model capability" do
      features = Client.parse_version(@nucleus_version_string)

      assert features.is_nucleus == true

      for cap <- @model_caps do
        assert Map.fetch!(features, :"has_#{cap}") == true, "has_#{cap} should be true"
      end
    end

    test "extracts the Nucleus version from the string (contract: 'extract the Nucleus version')" do
      features = Client.parse_version(@nucleus_version_string)
      assert features.nucleus_version == "0.9.2"
      assert features.version == @nucleus_version_string
    end
  end

  describe "parse_version/1 — plain PostgreSQL peer" do
    test "is SQL-only: is_nucleus false, every model capability false" do
      features = Client.parse_version(@plain_pg_version_string)

      assert features.is_nucleus == false
      assert features.nucleus_version == nil

      for cap <- @model_caps do
        assert Map.fetch!(features, :"has_#{cap}") == false
      end
    end

    test "a generic PostgreSQL banner is not misread as Nucleus" do
      features = Client.parse_version("PostgreSQL 16.4 on x86_64-pc-linux-gnu, compiled by gcc")
      assert features.is_nucleus == false
      assert features.nucleus_version == nil
    end
  end

  describe "parse_version/1 — unusable answers" do
    test "an error placeholder degrades to SQL-only, not to Nucleus" do
      features = Client.parse_version("unknown")
      assert features.is_nucleus == false
      assert features.nucleus_version == nil
    end
  end

  describe "behaving correctly against a plain PostgreSQL peer (§1 second half)" do
    test "require_nucleus/2 returns a clear 501 error through the real code path" do
      {:ok, _pid} = PlainPgStub.start_link(version: @plain_pg_version_string)
      on_exit(fn -> if Process.whereis(PlainPgStub), do: GenServer.stop(PlainPgStub) end)

      assert {:error, %Neutron.Error{} = error} =
               Client.require_nucleus(PlainPgStub, "KV.get")

      assert error.status == 501
      assert error.type == "https://neutron.dev/errors/nucleus-required"
      assert error.detail =~ "KV.get"
      assert error.detail =~ "plain PostgreSQL"
    end
  end
end
