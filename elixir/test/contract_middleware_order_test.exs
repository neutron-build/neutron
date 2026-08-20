defmodule Neutron.Contract.MiddlewareOrderTest do
  # Not async: mutates NEUTRON_SECRET_KEY and the :neutron_rate_limit ETS table,
  # both of which the assembled pipeline reads.
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  # A router whose responses make each layer's presence and position observable.
  defmodule ProbeRouter do
    use Plug.Router

    plug(:match)
    plug(:dispatch)

    get "/whoami" do
      send_resp(conn, 200, Jason.encode!(%{current_user: conn.assigns[:current_user]}))
    end

    get "/boom" do
      _ = conn
      raise "intentional"
    end

    match _ do
      send_resp(conn, 404, "not found")
    end
  end

  @pipeline_opts Neutron.Middleware.init(router: ProbeRouter, nucleus: nil)

  @contract_order [
    Neutron.Middleware.RequestId,
    Neutron.Middleware.RequestLogger,
    Neutron.Middleware.Recovery,
    Neutron.Middleware.Cors,
    Plug.Head,
    Neutron.Middleware.RateLimit,
    Neutron.Auth.Plug,
    Neutron.Middleware.Timeout,
    Neutron.Middleware.OTel,
    Neutron.Middleware.Dispatch
  ]

  # FRAMEWORK_CONTRACT §5, pinned in full and BY OBSERVATION. A test asserting
  # only the first pair leaves seven layers free to reorder silently — this one
  # traces the real call/2 invocations of every layer during one request and
  # asserts their exact sequence, so any adjacent swap fails it. Layer 5
  # (Compression) lives in Bandit, not the plug list; Plug.Head occupies that
  # slot (HEAD -> GET).
  test "one request invokes the layers in the contract order, outermost first" do
    trace_pipeline_order(fn ->
      conn(:get, "/whoami") |> Neutron.Middleware.call(@pipeline_opts)
    end)
  end

  describe "observed layer behaviour and relative order" do
    test "Recovery runs before Dispatch: a raising route answers 500 problem+json, not a crash" do
      conn = conn(:get, "/boom") |> Neutron.Middleware.call(@pipeline_opts)

      assert conn.status == 500
      assert [content_type] = get_resp_header(conn, "content-type")
      assert content_type =~ "application/problem+json"

      body = Jason.decode!(conn.resp_body)
      assert body["type"] == "https://neutron.dev/errors/internal"
      assert body["status"] == 500
      assert body["title"] == "Internal Server Error"
    end

    test "CORS runs before RateLimit: an over-limit preflight still answers 204, not 429" do
      # The rate-limit table's survival is order-dependent: other suites
      # recreate it with a short-lived test process as owner, and the table
      # dies with that owner. Create it if missing — never delete an existing
      # one — so this test observes ordering, not table luck.
      ensure_rate_limit_table()

      now = System.system_time(:second)
      # Push this client over the 100 rpm limit.
      :ets.insert(:neutron_rate_limit, {{"127.0.0.1", now}, 101})

      conn =
        conn(:options, "/whoami")
        |> put_req_header("origin", "http://example.com")
        |> Neutron.Middleware.call(@pipeline_opts)

      assert conn.status == 204

      # An over-limit non-preflight request does get 429 — proving the seeded
      # counter was really in effect and the 204 above is about ordering.
      blocked =
        conn(:get, "/whoami")
        |> put_req_header("origin", "http://example.com")
        |> Neutron.Middleware.call(@pipeline_opts)

      assert blocked.status == 429
    after
      clear_rate_limit_seed()
    end

    test "Auth runs inside the pipeline (contract layer 7): a bearer token reaches the router" do
      secret = "contract-order-test-secret-that-is-long-enough"
      System.put_env("NEUTRON_SECRET_KEY", secret)

      try do
        {:ok, token} = Neutron.Auth.JWT.sign(%{"user_id" => 42}, secret: secret)

        conn =
          conn(:get, "/whoami")
          |> put_req_header("authorization", "Bearer #{token}")
          |> Neutron.Middleware.call(@pipeline_opts)

        assert conn.status == 200
        assert Jason.decode!(conn.resp_body)["current_user"]["user_id"] == 42
      after
        System.delete_env("NEUTRON_SECRET_KEY")
      end
    end

    test "Auth is a pass-through when no token is present (layer 7, optional)" do
      conn = conn(:get, "/whoami") |> Neutron.Middleware.call(@pipeline_opts)

      assert conn.status == 200
      assert Jason.decode!(conn.resp_body)["current_user"] == nil
    end
  end

  # Runs `thunk` (one request through the assembled pipeline) with call tracing
  # enabled on every layer's call/2, then asserts the observed invocation
  # sequence equals @contract_order. Trace patterns are global in the VM but
  # only THIS process is traced, so concurrent tests are unaffected. Events are
  # collected by a separate tracer process: on this erts (17.x), a process
  # tracing itself with `{:tracer, self()}` never receives its own call events.
  defp trace_pipeline_order(thunk) do
    test_pid = self()
    tracer = spawn_link(fn -> forward_traced_layers(test_pid) end)

    for mod <- @contract_order do
      # trace_pattern only affects LOADED code; a not-yet-loaded module matches
      # 0 functions and the pin would silently observe nothing. Load first and
      # assert the match count.
      {:module, _} = Code.ensure_loaded(mod)
      matched = :erlang.trace_pattern({mod, :call, :_}, [{:_, [], [{:message, mod}]}])
      assert matched > 0, "trace_pattern matched no functions on #{inspect(mod)}"
    end

    :erlang.trace(self(), true, [:call, {:tracer, tracer}])

    thunk.()

    assert collect_layers([]) == @contract_order
  after
    :erlang.trace(self(), false, [:call])
    for mod <- @contract_order, do: :erlang.trace_pattern({mod, :call, :_}, false)
  end

  defp ensure_rate_limit_table do
    if :ets.whereis(:neutron_rate_limit) == :undefined do
      :ets.new(:neutron_rate_limit, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])
    end

    :ok
  end

  defp clear_rate_limit_seed do
    :ets.match_delete(:neutron_rate_limit, {{"127.0.0.1", :_}, :_})
  rescue
    # The table is owned elsewhere; if it is momentarily gone there is
    # nothing to clean.
    ArgumentError -> :ok
  end

  defp forward_traced_layers(test_pid) do
    receive do
      {:trace, _pid, :call, {_mod, :call, _arity}, layer} when is_atom(layer) ->
        send(test_pid, {:layer, layer})
        forward_traced_layers(test_pid)
    after
      5_000 -> :ok
    end
  end

  defp collect_layers(acc) when length(acc) == length(@contract_order), do: Enum.reverse(acc)

  defp collect_layers(acc) do
    receive do
      {:layer, layer} ->
        collect_layers([layer | acc])
    after
      1_000 ->
        flunk(
          "observed #{length(acc)} of #{length(@contract_order)} layer calls; " <>
            "invocation order so far: #{inspect(Enum.reverse(acc))}"
        )
    end
  end
end
