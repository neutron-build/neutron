defmodule Nucleus.ClientTest do
  use ExUnit.Case, async: true

  alias Nucleus.Client.Features

  describe "Features struct" do
    test "defaults to all false" do
      features = %Features{}
      refute features.is_nucleus
      refute features.has_kv
      refute features.has_vector
      refute features.has_ts
      refute features.has_document
      refute features.has_graph
      refute features.has_fts
      refute features.has_geo
      refute features.has_blob
      refute features.has_streams
      refute features.has_columnar
      refute features.has_datalog
      refute features.has_cdc
      refute features.has_pubsub
      assert features.version == nil
    end

    test "Nucleus-detected features are all true" do
      features = %Features{
        version: "PostgreSQL 16.0 (Nucleus 0.1.0 — The Definitive Database)",
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

      assert features.is_nucleus
      assert features.has_kv
      assert features.has_vector
      assert features.version =~ "Nucleus"
    end
  end

  describe "URL parsing" do
    test "child_spec creates valid spec" do
      spec = Nucleus.Client.child_spec(url: "postgres://user:pass@localhost:5432/testdb")
      assert spec.id == Nucleus.Client
      assert spec.type == :worker
    end
  end
end
