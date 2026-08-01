defmodule Neutron.MiddlewareExtendedTest do
  use ExUnit.Case
  use Plug.Test

  describe "Neutron.Middleware.RequestLogger" do
    test "registers a before_send callback" do
      conn =
        conn(:get, "/hello")
        |> Plug.Conn.assign(:request_id, "req-123")
        |> Neutron.Middleware.RequestLogger.call([])

      # The conn should have a before_send callback registered
      # We verify by sending the response — it should not crash
      conn =
        conn
        |> Plug.Conn.put_resp_content_type("text/plain")
        |> Plug.Conn.send_resp(200, "OK")

      assert conn.status == 200
    end

    test "does not halt the connection" do
      conn =
        conn(:get, "/test")
        |> Plug.Conn.assign(:request_id, "req-456")
        |> Neutron.Middleware.RequestLogger.call([])

      refute conn.halted
    end
  end

  describe "Neutron.Middleware.Recovery" do
    test "passes through when no exception" do
      conn =
        conn(:get, "/safe")
        |> Neutron.Middleware.Recovery.call([])

      refute conn.halted
      assert conn.status == nil
    end

    # Note: Recovery catches exceptions in downstream plugs, not in its own call.
    # Since it just returns conn, the exception would be caught when the conn
    # is used in a pipeline with downstream plugs that raise. Testing the
    # rescue path requires a pipeline or a raise within the call body.
  end

  describe "Neutron.Middleware.RateLimit" do
    setup do
      # Ensure rate limit ETS table exists
      try do
        :ets.delete(:neutron_rate_limit)
      rescue
        ArgumentError -> :ok
      end

      :ets.new(:neutron_rate_limit, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

      :ok
    end

    test "allows requests under the limit" do
      conn =
        conn(:get, "/api/data")
        |> Map.put(:remote_ip, {192, 168, 1, 1})
        |> Neutron.Middleware.RateLimit.call([])

      refute conn.halted
      # Should have rate limit headers
      [limit] = get_resp_header(conn, "x-ratelimit-limit")
      assert String.to_integer(limit) > 0
      [remaining] = get_resp_header(conn, "x-ratelimit-remaining")
      assert String.to_integer(remaining) >= 0
    end

    test "adds rate limit headers" do
      conn =
        conn(:get, "/api/data")
        |> Map.put(:remote_ip, {10, 0, 0, 1})
        |> Neutron.Middleware.RateLimit.call([])

      assert get_resp_header(conn, "x-ratelimit-limit") != []
      assert get_resp_header(conn, "x-ratelimit-remaining") != []
    end

    test "blocks requests over the limit with 429" do
      # Simulate many requests from the same IP
      ip = {10, 0, 0, 99}
      now = System.system_time(:second)

      # Insert enough requests to exceed the default 100 RPM
      for i <- 0..105 do
        :ets.insert(:neutron_rate_limit, {{format_ip(ip), now}, i + 1})
      end

      conn =
        conn(:get, "/api/data")
        |> Map.put(:remote_ip, ip)
        |> Neutron.Middleware.RateLimit.call([])

      assert conn.halted
      assert conn.status == 429
      assert get_resp_header(conn, "retry-after") != []
    end

    test "tracks different IPs separately" do
      conn1 =
        conn(:get, "/api/data")
        |> Map.put(:remote_ip, {172, 16, 0, 1})
        |> Neutron.Middleware.RateLimit.call([])

      conn2 =
        conn(:get, "/api/data")
        |> Map.put(:remote_ip, {172, 16, 0, 2})
        |> Neutron.Middleware.RateLimit.call([])

      refute conn1.halted
      refute conn2.halted
    end

    test "skips gracefully when ETS table does not exist" do
      :ets.delete(:neutron_rate_limit)

      conn =
        conn(:get, "/api/data")
        |> Map.put(:remote_ip, {127, 0, 0, 1})
        |> Neutron.Middleware.RateLimit.call([])

      refute conn.halted

      # Re-create for other tests
      :ets.new(:neutron_rate_limit, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])
    end

    defp format_ip({a, b, c, d}), do: "#{a}.#{b}.#{c}.#{d}"
  end

  describe "Neutron.Middleware.Timeout" do
    test "assigns deadline and watcher to conn" do
      conn =
        conn(:get, "/slow")
        |> Neutron.Middleware.Timeout.call([])

      assert conn.assigns[:request_deadline] != nil
      assert conn.assigns[:timeout_watcher] != nil
      assert is_integer(conn.assigns[:request_deadline])
      assert is_pid(conn.assigns[:timeout_watcher])
    end

    test "watcher is killed when response is sent" do
      conn =
        conn(:get, "/fast")
        |> Neutron.Middleware.Timeout.call([])

      watcher = conn.assigns[:timeout_watcher]
      assert Process.alive?(watcher)

      # Send a response to trigger the before_send callback
      conn
      |> Plug.Conn.put_resp_content_type("text/plain")
      |> Plug.Conn.send_resp(200, "OK")

      # Give a moment for cleanup
      Process.sleep(10)
      refute Process.alive?(watcher)
    end

    test "does not halt the connection" do
      conn =
        conn(:get, "/test")
        |> Neutron.Middleware.Timeout.call([])

      refute conn.halted
    end
  end

  describe "Neutron.Middleware (the assembled pipeline)" do
    defmodule PipelineRouter do
      use Plug.Router
      plug :match
      plug :dispatch

      get "/ping" do
        send_resp(conn, 200, "pong")
      end

      match _ do
        send_resp(conn, 404, "not found")
      end
    end

    # Every other middleware test drives ONE plug directly. That is why the
    # assembled stack could raise `KeyError` on `:router` for every request
    # while the suite stayed green: `Plug.Builder` resolves a bare `plug Mod`
    # at compile time with `init([])`, so Dispatch never saw the router that
    # `Neutron.child_spec/1` passes. This test runs a request through the
    # pipeline Bandit is actually given.
    test "a request reaches the router through the full 10-layer stack" do
      opts = Neutron.Middleware.init(router: PipelineRouter, nucleus: nil)

      conn =
        conn(:get, "/ping")
        |> Neutron.Middleware.call(opts)

      assert conn.status == 200
      assert conn.resp_body == "pong"
    end

    test "the pipeline still applies its own layers" do
      opts = Neutron.Middleware.init(router: PipelineRouter, nucleus: nil)

      conn =
        conn(:get, "/ping")
        |> Neutron.Middleware.call(opts)

      # Layer 1 ran: RequestId sets the header on the way through.
      assert [req_id] = Plug.Conn.get_resp_header(conn, "x-request-id")
      assert req_id != ""
    end

    test "an unmatched route reaches the router's catch-all, not a crash" do
      opts = Neutron.Middleware.init(router: PipelineRouter, nucleus: nil)

      conn =
        conn(:get, "/nope")
        |> Neutron.Middleware.call(opts)

      assert conn.status == 404
    end
  end

  describe "Neutron.Middleware.Dispatch" do
    defmodule TestRouter do
      use Plug.Router
      plug :match
      plug :dispatch

      get "/hello" do
        send_resp(conn, 200, "world")
      end

      match _ do
        send_resp(conn, 404, "not found")
      end
    end

    test "dispatches to the configured router" do
      opts = Neutron.Middleware.Dispatch.init(router: TestRouter)

      conn =
        conn(:get, "/hello")
        |> Neutron.Middleware.Dispatch.call(opts)

      assert conn.status == 200
      assert conn.resp_body == "world"
    end

    test "raises when no router is configured" do
      # Raised from `call/2`, not `init/1`: the router arrives at runtime via
      # `Neutron.Middleware`'s `builder_opts()`, so `init/1` has nothing to
      # validate.
      assert_raise KeyError, fn ->
        conn(:get, "/hello")
        |> Neutron.Middleware.Dispatch.call(Neutron.Middleware.Dispatch.init([]))
      end
    end

    test "passes unmatched routes to the router's catch-all" do
      opts = Neutron.Middleware.Dispatch.init(router: TestRouter)

      conn =
        conn(:get, "/nonexistent")
        |> Neutron.Middleware.Dispatch.call(opts)

      assert conn.status == 404
    end
  end
end
