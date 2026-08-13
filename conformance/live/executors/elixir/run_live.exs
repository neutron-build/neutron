#!/usr/bin/env elixir
# Elixir executor for the Nucleus live data-model conformance spec.
#
# Reads ../../spec.json, runs every case against a live engine through the real
# in-repo Elixir client (elixir/lib/nucleus), and prints one JSON result
# document to stdout. It asserts nothing a mock could assert: only that a call
# reaches the engine, is accepted over the wire, and comes back with the right
# value.
#
#     NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
#         elixir run_live.exs
#
# Exit codes: 0 all cases behaved as the spec says, 1 otherwise. An `xfail` case
# that PASSES is a failure — otherwise a fix lands and the note explaining why
# the case is expected to fail quietly becomes a lie.
#
# Everything on stdout is the report. Diagnostics go to stderr, because the
# orchestrator parses stdout.
#
# There is no mix project here on purpose. The script compiles the in-repo
# `elixir/` app with its own mix and loads it off `_build`, so the client under
# test is byte-for-byte the one the SDK ships rather than a copy resolved from
# Hex. The bootstrap runs BEFORE the rest of the script is compiled, so the
# modules below are compiled against the real client and a missing function is
# a compile-time error here rather than a silent skip.

defmodule Live.Bootstrap do
  @moduledoc false

  def load! do
    # Logger's default handler writes to stdout, and stdout is the report. The
    # handler's device cannot be changed in place, so it is replaced.
    :logger.remove_handler(:default)

    :logger.add_handler(:conformance_stderr, :logger_std_h, %{
      level: :warning,
      config: %{type: :standard_error}
    })

    app = Path.expand("../../../../elixir", __DIR__)
    compile(app)

    build = Path.join(app, "_build/dev/lib")

    unless File.dir?(build) do
      die("no compiled build at #{build} — run `mix compile` in #{app}")
    end

    for dep <- File.ls!(build) do
      Code.prepend_path(Path.join([build, dep, "ebin"]))
    end

    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:jason)

    unless Code.ensure_loaded?(Nucleus.Client) do
      die("Nucleus.Client did not load from #{build}")
    end

    :ok
  end

  def die(msg) do
    IO.puts(:stderr, "::error::#{msg}")
    System.stop(1)
    Process.sleep(:infinity)
  end

  defp compile(app) do
    case System.cmd("mix", ["compile"], cd: app, stderr_to_stdout: true) do
      {_out, 0} -> :ok
      {out, code} -> IO.puts(:stderr, "mix compile failed (#{code}) in #{app}:\n#{out}")
    end
  rescue
    e ->
      IO.puts(:stderr, "could not run mix (#{inspect(e)}); using whatever is in _build")
      :ok
  end
end

if System.get_env("NEUTRON_TEST_DATABASE_URL") in [nil, ""] do
  Live.Bootstrap.die(
    "NEUTRON_TEST_DATABASE_URL is not set. This suite is only meaningful " <>
      "against a live engine; refusing to report a green run for zero executed cases."
  )
end

Live.Bootstrap.load!()

defmodule Live.Failed do
  @moduledoc false
  defexception [:message]
end

defmodule Live.Check do
  @moduledoc false

  # The expectation vocabulary is the spec's, not this executor's. Every branch
  # here corresponds to one key documented in spec.json's `expectations` block.

  def check(result, expect) do
    actual = if Map.has_key?(expect, "key"), do: read_key(result, expect["key"]), else: result

    actual =
      if Map.has_key?(expect, "index"), do: read_index(actual, expect["index"]), else: actual

    actual = if expect["jsonDecode"], do: Jason.decode!(actual), else: actual

    if expect["notNull"] && is_nil(actual), do: fail("expected a value, got nil")

    if expect["isNull"] && not is_nil(actual),
      do: fail("expected nil, got #{inspect(actual)}")

    if expect["nonEmpty"] && empty?(actual),
      do: fail("expected a non-empty collection, got #{inspect(actual)}")

    if Map.has_key?(expect, "length") do
      len = length_of(actual)

      if len != expect["length"],
        do: fail("expected #{expect["length"]} elements, got #{len}: #{inspect(actual)}")
    end

    if Map.has_key?(expect, "type") do
      unless type?(actual, expect["type"]),
        do: fail("expected #{expect["type"]}, got #{inspect(actual)}")
    end

    if Map.has_key?(expect, "equals") do
      want = expect["equals"]

      unless equal?(actual, want),
        do: fail("expected #{inspect(want)}, got #{inspect(actual)}")
    end

    :ok
  end

  defp fail(msg), do: raise(Live.Failed, message: msg)

  defp read_key(nil, key), do: fail("expected a map with key #{inspect(key)}, got nil")

  defp read_key(map, key) when is_map(map) do
    if Map.has_key?(map, key) do
      Map.get(map, key)
    else
      # Some client functions return atom-keyed structs (FTS results, vector
      # hits); the spec names keys as strings.
      map |> Map.new(fn {k, v} -> {to_string(k), v} end) |> Map.get(key)
    end
  end

  defp read_key(other, key),
    do: fail("expected a map with key #{inspect(key)}, got #{inspect(other)}")

  defp read_index(list, i) when is_list(list), do: Enum.at(list, i)
  defp read_index(other, i), do: fail("expected a list to index at #{i}, got #{inspect(other)}")

  defp empty?(nil), do: true
  defp empty?(false), do: true
  defp empty?([]), do: true
  defp empty?(""), do: true
  defp empty?(0), do: true
  defp empty?(m) when is_map(m), do: map_size(m) == 0
  defp empty?(_), do: false

  defp length_of(l) when is_list(l), do: length(l)
  defp length_of(m) when is_map(m), do: map_size(m)
  defp length_of(s) when is_binary(s), do: String.length(s)
  defp length_of(other), do: fail("expected a collection, got #{inspect(other)}")

  defp type?(v, "list"), do: is_list(v)
  defp type?(v, "map"), do: is_map(v)
  defp type?(v, "string"), do: is_binary(v)
  defp type?(v, "int"), do: is_integer(v)
  defp type?(v, "float"), do: is_number(v)
  defp type?(v, "bool"), do: is_boolean(v)
  defp type?(v, "bytes"), do: is_binary(v)
  defp type?(_, other), do: fail("unknown type in spec: #{other}")

  # Floats compare loosely; everything else exactly.
  defp equal?(a, b) when is_number(a) and is_number(b) do
    if is_float(a) or is_float(b),
      do: abs(a * 1.0 - b * 1.0) < 1.0e-9,
      else: a == b
  end

  defp equal?(a, b) when is_list(a) and is_list(b) do
    length(a) == length(b) and a |> Enum.zip(b) |> Enum.all?(fn {x, y} -> equal?(x, y) end)
  end

  defp equal?(a, b), do: a == b
end

defmodule Live.Ops do
  @moduledoc false

  alias Nucleus.Models.{Blob, CDC, Datalog, Document, FTS, Graph, KV, Streams, TimeSeries, Vector}

  # The instant the spec's time-series millisecond offsets are measured from:
  # 2026-08-11T12:00:00Z. Fixed so the cases are deterministic and comparable
  # across SDKs.
  @ts_base_iso "2026-08-11T12:00:00Z"

  def ts_base_ms do
    {:ok, dt, 0} = DateTime.from_iso8601(@ts_base_iso)
    DateTime.to_unix(dt, :millisecond)
  end

  def call(client, op, args), do: dispatch(client, op, args)

  # ── core ─────────────────────────────────────────────────────────────
  defp dispatch(c, "features.isNucleus", []), do: Nucleus.Client.is_nucleus?(c)

  defp dispatch(_c, "connection.closeAndReconnect", []) do
    url = System.get_env("NEUTRON_TEST_DATABASE_URL")
    {:ok, probe} = Nucleus.Client.start_link(url: url, name: nil, pool_size: 1)
    :ok = Nucleus.Client.ping(probe)

    # Hung forever before N25: the server ignored Terminate and never closed the
    # socket. A bounded stop turns that hang into a failure instead.
    task = Task.async(fn -> GenServer.stop(probe, :normal, 15_000) end)

    case Task.yield(task, 20_000) || Task.shutdown(task, :brutal_kill) do
      {:ok, :ok} -> true
      other -> raise Live.Failed, message: "close did not complete: #{inspect(other)}"
    end
  end

  # ── document ─────────────────────────────────────────────────────────
  defp dispatch(c, "document.insert", [coll, doc]), do: unwrap(Document.insert_in(c, coll, doc))
  defp dispatch(c, "document.get", [id]), do: unwrap(Document.get(c, id))

  defp dispatch(c, "document.getIn", [coll, id]),
    do: unwrap(Document.get_in_collection(c, coll, id))

  defp dispatch(c, "document.countIn", [coll]), do: unwrap(Document.count_in(c, coll))

  # Document.query_in is the client's filter surface. It answers with matching
  # ids rather than documents; `find_one` cannot be synthesised from it without
  # this executor becoming the client, so that op is left unmapped.
  defp dispatch(c, "document.find", [coll, filter]), do: unwrap(Document.query_in(c, coll, filter))

  # ── graph ────────────────────────────────────────────────────────────
  # Graph.add_node takes ONE label where the spec passes a label list; the first
  # label is used, because a multi-label node has no Elixir surface.
  defp dispatch(c, "graph.addNode", [labels, props]),
    do: unwrap(Graph.add_node(c, hd(labels), props))

  # Spec order is (type, from, to); the Elixir signature is (from, to, type).
  defp dispatch(c, "graph.addEdge", [type, from, to]), do: unwrap(Graph.add_edge(c, from, to, type))

  defp dispatch(c, "graph.neighbors", [id, direction]),
    do: unwrap(Graph.neighbors(c, id, String.to_atom(direction)))

  defp dispatch(c, "graph.shortestPath", [a, b]), do: unwrap(Graph.shortest_path(c, a, b))
  defp dispatch(c, "graph.nodeCount", []), do: unwrap(Graph.node_count(c))
  defp dispatch(c, "graph.edgeCount", []), do: unwrap(Graph.edge_count(c))
  defp dispatch(c, "graph.deleteNode", [id]), do: unwrap(Graph.delete_node(c, id))

  # ── key/value ────────────────────────────────────────────────────────
  defp dispatch(c, "kv.set", [k, v]), do: unwrap(KV.set(c, k, v))
  defp dispatch(c, "kv.get", [k]), do: unwrap(KV.get(c, k))
  defp dispatch(c, "kv.exists", [k]), do: unwrap(KV.exists?(c, k))
  defp dispatch(c, "kv.delete", [k]), do: unwrap(KV.del(c, k))
  defp dispatch(c, "kv.expire", [k, ttl]), do: unwrap(KV.expire(c, k, ttl))
  defp dispatch(c, "kv.ttl", [k]), do: unwrap(KV.ttl(c, k))
  defp dispatch(c, "kv.incr", [k, by]), do: unwrap(KV.incr(c, k, by))
  defp dispatch(c, "kv.rpush", [k, v]), do: unwrap(KV.rpush(c, k, v))
  defp dispatch(c, "kv.llen", [k]), do: unwrap(KV.llen(c, k))
  defp dispatch(c, "kv.lrange", [k, a, b]), do: unwrap(KV.lrange(c, k, a, b))
  defp dispatch(c, "kv.lindex", [k, i]), do: unwrap(KV.lindex(c, k, i))
  defp dispatch(c, "kv.zadd", [k, score, m]), do: unwrap(KV.zadd(c, k, score * 1.0, m))
  defp dispatch(c, "kv.zrange", [k, a, b]), do: unwrap(KV.zrange(c, k, a, b))
  defp dispatch(c, "kv.hset", [k, f, v]), do: unwrap(KV.hset(c, k, f, v))
  defp dispatch(c, "kv.hget", [k, f]), do: unwrap(KV.hget(c, k, f))
  defp dispatch(c, "kv.hexists", [k, f]), do: unwrap(KV.hexists?(c, k, f))
  defp dispatch(c, "kv.hgetall", [k]), do: unwrap(KV.hgetall(c, k))
  defp dispatch(c, "kv.hlen", [k]), do: unwrap(KV.hlen(c, k))
  defp dispatch(c, "kv.hdel", [k, f]), do: unwrap(KV.hdel(c, k, f))
  defp dispatch(c, "kv.sadd", [k, m]), do: unwrap(KV.sadd(c, k, m))
  defp dispatch(c, "kv.srem", [k, m]), do: unwrap(KV.srem(c, k, m))
  defp dispatch(c, "kv.smembers", [k]), do: unwrap(KV.smembers(c, k))

  # ── time series ──────────────────────────────────────────────────────
  # TimeSeries.insert is per point; the spec writes a batch.
  defp dispatch(c, "timeseries.write", [series, points]) do
    Enum.each(points, fn p ->
      unwrap(TimeSeries.insert(c, series, ts_base_ms() + p["t"], p["v"] * 1.0))
    end)

    :ok
  end

  defp dispatch(c, "timeseries.count", [series]), do: unwrap(TimeSeries.count(c, series))
  defp dispatch(c, "timeseries.last", [series]), do: unwrap(TimeSeries.last(c, series))

  # ── streams ──────────────────────────────────────────────────────────
  defp dispatch(c, "streams.xadd", [s, fields]), do: unwrap(Streams.xadd(c, s, fields))
  defp dispatch(c, "streams.xlen", [s]), do: unwrap(Streams.xlen(c, s))
  defp dispatch(c, "streams.xrange", [s, a, b, n]), do: unwrap(Streams.xrange(c, s, a, b, n))
  defp dispatch(c, "streams.xread", [s, after_id, n]), do: unwrap(Streams.xread(c, s, after_id, n))

  defp dispatch(c, "streams.xgroupCreate", [s, g, start]),
    do: unwrap(Streams.xgroup_create(c, s, g, start))

  defp dispatch(c, "streams.xreadgroup", [s, g, consumer, n]),
    do: unwrap(Streams.xreadgroup(c, s, g, consumer, n))

  # xadd returns one 'ms-seq' string; xack's signature is (stream, group, id_ms,
  # id_seq). The natural round trip is made verbatim — whether the two halves
  # compose is the assertion — through `apply`, so the mismatch is recorded as a
  # result rather than stopping this script from compiling.
  defp dispatch(c, "streams.xack", [s, g, entry_id]),
    do: unwrap(apply(Streams, :xack, [c, s, g, entry_id]))

  # ── blobs ────────────────────────────────────────────────────────────
  # Blob.store/get take a key only — there is no bucket dimension in the Elixir
  # client — and carry the payload hex-encoded rather than as bytes.
  defp dispatch(c, "blob.put", [_bucket, key, payload_b64]) do
    hex = payload_b64 |> Base.decode64!() |> Base.encode16(case: :lower)
    unwrap(Blob.store(c, key, hex))
  end

  defp dispatch(c, "blob.get", [_bucket, key]) do
    case unwrap(Blob.get(c, key)) do
      nil ->
        nil

      hex when is_binary(hex) ->
        case Base.decode16(hex, case: :mixed) do
          {:ok, bin} ->
            Base.encode64(bin)

          :error ->
            raise Live.Failed,
              message: "BLOB_GET returned a value the client cannot hex-decode: #{inspect(hex)}"
        end
    end
  end

  defp dispatch(c, "blob.getMeta", [_bucket, key]), do: unwrap(Blob.meta(c, key))
  defp dispatch(c, "blob.delete", [_bucket, key]), do: unwrap(Blob.delete(c, key))

  # ── cdc ──────────────────────────────────────────────────────────────
  defp dispatch(c, "cdc.read", [after_seq, limit]), do: unwrap(CDC.read(c, after_seq, limit))
  defp dispatch(c, "cdc.count", []), do: unwrap(CDC.count(c))

  # ── datalog ──────────────────────────────────────────────────────────
  defp dispatch(c, "datalog.assertFact", [fact]), do: unwrap(Datalog.assert(c, fact))
  defp dispatch(c, "datalog.query", [q]), do: unwrap(Datalog.query(c, q))
  defp dispatch(c, "datalog.clear", [pred]), do: unwrap(Datalog.clear(c, pred))

  # ── full-text search ─────────────────────────────────────────────────
  # The engine keeps ONE global index; the client drops the index name and joins
  # the field values into a single text blob, as the Python and TypeScript
  # executors do.
  defp dispatch(c, "fts.indexDoc", [_index, doc_id, fields]),
    do: unwrap(FTS.index(c, String.to_integer(doc_id), Enum.join(Map.values(fields), " ")))

  defp dispatch(c, "fts.search", [_index, query, limit]), do: unwrap(FTS.search(c, query, limit))

  # ── vector ───────────────────────────────────────────────────────────
  defp dispatch(c, "vector.createCollection", [coll, dim]),
    do: unwrap(Vector.create_collection(c, coll, dim))

  defp dispatch(c, "vector.insert", [coll, id, values]),
    do: unwrap(Vector.insert(c, coll, id, values))

  defp dispatch(c, "vector.search", [coll, values, k]),
    do: unwrap(Vector.search(c, coll, values, limit: k))

  # ── raw sql ──────────────────────────────────────────────────────────
  defp dispatch(c, "sql.queryScalar", [query, params]) do
    case unwrap(Nucleus.Client.query(c, query, params)) do
      %{rows: [row | _]} when is_list(row) and row != [] -> hd(row)
      _ -> nil
    end
  end

  defp dispatch(c, "sql.execute", [query, params]) do
    # Postgrex reports the command tag's row count as num_rows; the spec
    # compares row counts.
    case unwrap(Nucleus.Client.query(c, query, params)) do
      %{num_rows: n} -> n
      other -> other
    end
  end

  defp dispatch(c, "sql.begin", []), do: unwrap(Nucleus.Client.query(c, "BEGIN", []))
  defp dispatch(c, "sql.rollback", []), do: unwrap(Nucleus.Client.query(c, "ROLLBACK", []))

  # Anything the Elixir client has no surface for. Undeclared this is a failure;
  # declared in unsupported.json with a reason it is `unsupported`.
  defp dispatch(_c, op, _args), do: throw({:unsupported, op})

  defp unwrap({:ok, value}), do: value
  defp unwrap(:ok), do: nil
  defp unwrap({:error, reason}), do: raise(Live.Failed, message: "client error: #{inspect(reason)}")
  defp unwrap(other), do: other
end

defmodule Live do
  @moduledoc false

  @fixture_re ~r/@([A-Za-z_][A-Za-z0-9_]*)/
  @case_timeout 120_000

  def main do
    url = System.get_env("NEUTRON_TEST_DATABASE_URL")
    spec = Path.expand("../../spec.json", __DIR__) |> File.read!() |> Jason.decode!()

    declared = read_unsupported(Path.expand("unsupported.json", __DIR__))

    # One connection, so BEGIN and the statements after it land on the same
    # session. A pool would scatter them and the transaction case would be
    # testing nothing.
    {:ok, client} = Nucleus.Client.start_link(url: url, name: nil, pool_size: 1)

    results =
      case preflight(client, url) do
        :ok ->
          Enum.map(spec["cases"], &run_case(&1, client, declared))

        {:error, detail} ->
          # Nothing ran, so nothing may be reported as expected-to-fail: an
          # `xfail` says a case failed for the reason its note gives, and this
          # one failed before the first statement was sent.
          Enum.map(spec["cases"], fn kase ->
            %{
              "id" => kase["id"],
              "model" => kase["model"],
              "status" => "fail",
              "detail" => detail
            }
          end)
      end

    doc = %{"sdk" => "elixir", "specVersion" => spec["specVersion"], "cases" => results}
    IO.puts(Jason.encode!(doc, pretty: true))

    bad = Enum.filter(results, &(&1["status"] in ["fail", "xpass"]))

    for r <- bad, do: IO.puts(:stderr, "::error::#{r["id"]}: #{r["status"]} — #{r["detail"]}")

    IO.puts(:stderr, "elixir: #{inspect(Enum.frequencies_by(results, & &1["status"]))}")

    exit_with(if bad == [], do: 0, else: 1)
  end

  # The client connects lazily and reports a connect failure as "connected to
  # plain PostgreSQL", so an unreachable engine would otherwise be recorded 42
  # times as a feature-detection result. One statement up front tells the two
  # apart, and a second, deliberately synchronous connection recovers the
  # server's actual error, which the pool only ever writes to the log.
  defp preflight(client, url) do
    case Nucleus.Client.query(client, "SELECT 1", []) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        {:error,
         "the Elixir client never reached the engine, so no case ran: " <>
           (connect_error(url) || inspect(reason))}
    end
  end

  defp connect_error(url) do
    uri = URI.parse(url)
    [username | rest] = String.split(uri.userinfo || "postgres", ":", parts: 2)

    opts =
      [
        hostname: uri.host || "localhost",
        port: uri.port || 5432,
        username: username,
        database: String.trim_leading(uri.path || "/postgres", "/"),
        pool_size: 1,
        sync_connect: true,
        backoff_type: :stop
      ] ++ if(rest == [], do: [], else: [password: hd(rest)])

    case Postgrex.start_link(opts) do
      {:ok, pid} ->
        GenServer.stop(pid)
        nil

      {:error, %{__exception__: true} = error} ->
        Exception.message(error)

      {:error, reason} ->
        inspect(reason)
    end
  end

  defp read_unsupported(path) do
    if File.exists?(path) do
      path |> File.read!() |> Jason.decode!() |> Map.get("cases", %{})
    else
      %{}
    end
  end

  defp run_case(kase, client, declared) do
    expected_fail = Map.has_key?(kase, "xfail")
    base = %{"id" => kase["id"], "model" => kase["model"]}

    case execute(kase, client) do
      :ok when expected_fail ->
        Map.merge(base, %{
          "status" => "xpass",
          "detail" =>
            "case is marked xfail but passed — the underlying bug is fixed and " <>
              "the xfail note is now false"
        })

      :ok ->
        Map.put(base, "status", "pass")

      {:unsupported, op} ->
        case Map.get(declared, kase["id"]) do
          nil ->
            Map.merge(base, %{
              "status" => "fail",
              "detail" =>
                "op #{op} has no mapping and the case is not declared unsupported " <>
                  "in unsupported.json"
            })

          reason ->
            Map.merge(base, %{"status" => "unsupported", "detail" => reason})
        end

      {:error, detail} ->
        Map.merge(base, %{
          "status" => if(expected_fail, do: "xfail", else: "fail"),
          "detail" => detail
        })
    end
  end

  # A hang is a finding, but a hung run reports nothing at all, so it is turned
  # into a failure with a name attached.
  defp execute(kase, client) do
    task = Task.async(fn -> execute_steps(kase, client) end)

    case Task.yield(task, @case_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, "timed out after #{div(@case_timeout, 1000)}s"}
      {:exit, reason} -> {:error, "case process exited: #{inspect(reason)}"}
    end
  end

  defp execute_steps(kase, client) do
    fixtures = :ets.new(:fixtures, [:set, :private])

    try do
      kase["steps"]
      |> Enum.with_index()
      |> Enum.reduce(%{}, fn {step, i}, bound ->
        args = resolve(step["args"] || [], fixtures, bound)
        result = Live.Ops.call(client, step["op"], args)

        if expect = step["expect"] do
          try do
            Live.Check.check(result, expect)
          rescue
            e in Live.Failed ->
              reraise Live.Failed,
                      [message: "step #{i} (#{step["op"]}): #{Exception.message(e)}"],
                      __STACKTRACE__
          end
        end

        if name = step["bind"], do: Map.put(bound, name, result), else: bound
      end)

      :ok
    rescue
      e -> {:error, "#{inspect(e.__struct__)}: #{Exception.message(e)}"}
    catch
      :throw, {:unsupported, op} -> {:unsupported, op}
      kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
    after
      :ets.delete(fixtures)
    end
  end

  # Positional argument resolution, exactly as the spec's `conventions` block
  # describes it: '@name' is a per-case unique fixture, '$name' is a value bound
  # by an earlier step, anything else is a literal.
  defp resolve("$" <> name, _fixtures, bound) do
    case Map.fetch(bound, name) do
      {:ok, value} -> value
      :error -> raise Live.Failed, message: "step references $#{name} before it was bound"
    end
  end

  defp resolve(s, fixtures, _bound) when is_binary(s),
    do: Regex.replace(@fixture_re, s, fn _, name -> fixture(fixtures, name) end)

  defp resolve(l, fixtures, bound) when is_list(l), do: Enum.map(l, &resolve(&1, fixtures, bound))

  defp resolve(m, fixtures, bound) when is_map(m),
    do: Map.new(m, fn {k, v} -> {k, resolve(v, fixtures, bound)} end)

  defp resolve(v, _fixtures, _bound), do: v

  defp fixture(table, name) do
    case :ets.lookup(table, name) do
      [{^name, value}] ->
        value

      [] ->
        value = name <> "_" <> Base.encode16(:crypto.strong_rand_bytes(5), case: :lower)
        :ets.insert(table, {name, value})
        value
    end
  end

  # System.stop rather than System.halt: halt can truncate stdout, and stdout is
  # the report.
  defp exit_with(code) do
    System.stop(code)
    Process.sleep(:infinity)
  end
end

Live.main()
