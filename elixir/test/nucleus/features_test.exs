defmodule Nucleus.Client.FeaturesTest do
  use ExUnit.Case, async: true

  alias Nucleus.Client.Features

  describe "Features struct" do
    test "has default values" do
      features = %Features{}
      assert features.is_nucleus == false
      assert features.has_kv == false
      assert features.has_vector == false
      assert features.has_ts == false
      assert features.has_document == false
      assert features.has_graph == false
      assert features.has_fts == false
      assert features.has_geo == false
      assert features.has_blob == false
      assert features.has_streams == false
      assert features.has_columnar == false
      assert features.has_datalog == false
      assert features.has_cdc == false
      assert features.has_pubsub == false
      assert features.version == nil
    end

    test "can be constructed with all features enabled" do
      features = %Features{
        version: "Nucleus 0.1.0",
        is_nucleus: true,
        has_kv: true,
        has_vector: true,
        has_ts: true,
        has_document: true,
        has_graph: true,
        has_fts: true,
        has_geo: true,
        has_blob: true,
        has_streams: true,
        has_columnar: true,
        has_datalog: true,
        has_cdc: true,
        has_pubsub: true
      }

      assert features.is_nucleus == true
      assert features.has_kv == true
      assert features.version == "Nucleus 0.1.0"
    end
  end

  describe "Nucleus.Client exports" do
    test "exports start_link/1" do
      assert function_exported?(Nucleus.Client, :start_link, 1)
    end

    test "exports child_spec/1" do
      assert function_exported?(Nucleus.Client, :child_spec, 1)
    end

    test "exports query/2 and query/3" do
      assert function_exported?(Nucleus.Client, :query, 2)
      assert function_exported?(Nucleus.Client, :query, 3)
    end

    test "exports query!/2 and query!/3" do
      assert function_exported?(Nucleus.Client, :query!, 2)
      assert function_exported?(Nucleus.Client, :query!, 3)
    end

    test "exports is_nucleus?/1" do
      assert function_exported?(Nucleus.Client, :is_nucleus?, 1)
    end

    test "exports features/1" do
      assert function_exported?(Nucleus.Client, :features, 1)
    end

    test "exports ping/1" do
      assert function_exported?(Nucleus.Client, :ping, 1)
    end

    test "exports pool/1" do
      assert function_exported?(Nucleus.Client, :pool, 1)
    end

    test "exports require_nucleus/2" do
      assert function_exported?(Nucleus.Client, :require_nucleus, 2)
    end

    test "child_spec returns valid spec" do
      spec = Nucleus.Client.child_spec(url: "postgres://localhost/test")
      assert spec.id == Nucleus.Client
      assert spec.type == :worker
      assert spec.restart == :permanent
    end

    test "child_spec uses custom name as id" do
      spec = Nucleus.Client.child_spec(url: "postgres://localhost/test", name: :my_client)
      assert spec.id == :my_client
    end
  end
end
