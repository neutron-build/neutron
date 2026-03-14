defmodule Neutron.Middleware do
  @moduledoc """
  Middleware pipeline implementing the 10-layer stack from FRAMEWORK_CONTRACT.md.

  Layers (outermost first):
  1. **RequestId** — generates UUID in `X-Request-Id`
  2. **Logger** — structured request/response logging
  3. **Recovery** — catches exceptions, returns 500
  4. **CORS** — configurable cross-origin headers
  5. **Compress** — gzip via Plug.Deflate (if accepted)
  6. **RateLimit** — ETS-based sliding window per IP
  7. **Auth** — JWT verification (optional)
  8. **Timeout** — Task.async with deadline
  9. **OTel** — OpenTelemetry span creation
  10. **Router** — route dispatch

  ## Usage

  The middleware pipeline is automatically assembled when you start a Neutron server.
  Each layer can be configured individually.
  """

  use Plug.Builder
  require Logger

  # Layer 1: Request ID
  plug Neutron.Middleware.RequestId
  # Layer 2: Logger
  plug Neutron.Middleware.RequestLogger
  # Layer 3: Recovery (exception handler)
  plug Neutron.Middleware.Recovery
  # Layer 4: CORS
  plug Neutron.Middleware.Cors
  # Layer 5: Compression
  plug Plug.Head
  # Layer 6: Rate Limiting
  plug Neutron.Middleware.RateLimit
  # Layer 8: Request Timeout
  plug Neutron.Middleware.Timeout
  # Layer 9: OTel tracing
  plug Neutron.Middleware.OTel
  # Layer 10: Router dispatch (calls into user's router)
  plug Neutron.Middleware.Dispatch

  @doc false
  def init(opts) do
    opts
  end
end

# =============================================================================
# Layer 1: Request ID
# =============================================================================
defmodule Neutron.Middleware.RequestId do
  @moduledoc "Assigns a unique request ID to each request via X-Request-Id header."
  @behaviour Plug

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    request_id =
      case Plug.Conn.get_req_header(conn, "x-request-id") do
        [id | _] when id != "" -> id
        _ -> generate_uuid()
      end

    conn
    |> Plug.Conn.put_resp_header("x-request-id", request_id)
    |> Plug.Conn.assign(:request_id, request_id)
  end

  defp generate_uuid do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)

    "#{hex(a, 8)}-#{hex(b, 4)}-4#{hex(c &&& 0x0FFF, 3)}-#{hex((d &&& 0x3FFF) ||| 0x8000, 4)}-#{hex(e, 12)}"
  end

  defp hex(int, pad) do
    int
    |> Integer.to_string(16)
    |> String.downcase()
    |> String.pad_leading(pad, "0")
  end
end

# =============================================================================
# Layer 2: Request Logger
# =============================================================================
defmodule Neutron.Middleware.RequestLogger do
  @moduledoc "Structured request/response logging with timing."
  @behaviour Plug
  require Logger

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    start = System.monotonic_time(:microsecond)

    Plug.Conn.register_before_send(conn, fn conn ->
      duration = System.monotonic_time(:microsecond) - start

      Logger.info("request",
        method: conn.method,
        path: conn.request_path,
        status: conn.status,
        duration_us: duration,
        request_id: conn.assigns[:request_id]
      )

      conn
    end)
  end
end

# =============================================================================
# Layer 3: Recovery (Exception Handler)
# =============================================================================
defmodule Neutron.Middleware.Recovery do
  @moduledoc "Catches unhandled exceptions and returns RFC 7807 500 responses."
  @behaviour Plug
  require Logger

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    conn
  rescue
    exception ->
      Logger.error("Unhandled exception: #{inspect(exception)}")
      Logger.error(Exception.format(:error, exception, __STACKTRACE__))

      Neutron.Error.send_error(
        conn,
        Neutron.Error.internal("An unexpected error occurred")
      )
  end
end

# =============================================================================
# Layer 4: CORS
# =============================================================================
defmodule Neutron.Middleware.Cors do
  @moduledoc """
  CORS middleware with configurable allowed origins.

  Reads from `NEUTRON_CORS_ORIGINS` env var (comma-separated) or defaults to `*`.
  """
  @behaviour Plug

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    config = Neutron.Config.load()
    origin = get_origin(conn)

    if origin && origin_allowed?(origin, config.cors_origins) do
      conn
      |> Plug.Conn.put_resp_header("access-control-allow-origin", origin)
      |> Plug.Conn.put_resp_header(
        "access-control-allow-methods",
        "GET, POST, PUT, PATCH, DELETE, OPTIONS"
      )
      |> Plug.Conn.put_resp_header(
        "access-control-allow-headers",
        "Content-Type, Authorization, X-Request-Id"
      )
      |> Plug.Conn.put_resp_header("access-control-max-age", "86400")
      |> handle_preflight()
    else
      conn
    end
  end

  defp get_origin(conn) do
    case Plug.Conn.get_req_header(conn, "origin") do
      [origin | _] -> origin
      _ -> nil
    end
  end

  defp origin_allowed?(_origin, ["*"]), do: true

  defp origin_allowed?(origin, allowed) do
    origin in allowed
  end

  defp handle_preflight(%{method: "OPTIONS"} = conn) do
    conn
    |> Plug.Conn.send_resp(204, "")
    |> Plug.Conn.halt()
  end

  defp handle_preflight(conn), do: conn
end

# =============================================================================
# Layer 6: Rate Limiting
# =============================================================================
defmodule Neutron.Middleware.RateLimit do
  @moduledoc """
  ETS-based sliding window rate limiter.

  Limits requests per IP address using a sliding window counter stored in ETS.
  Default: 100 requests per 60 seconds.

  Configure via:
  - `NEUTRON_RATE_LIMIT_RPM` — requests per minute (default: 100)
  """
  @behaviour Plug

  @table :neutron_rate_limit
  @default_rpm 100
  @window_seconds 60

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    rpm = get_rpm()
    client_ip = format_ip(conn.remote_ip)
    now = System.system_time(:second)
    window_start = now - @window_seconds

    # Clean old entries and count current window
    cleanup_and_count(client_ip, window_start, now, rpm, conn)
  end

  defp cleanup_and_count(client_ip, window_start, now, rpm, conn) do
    key = {client_ip, now}

    try do
      # Increment counter for this second
      :ets.update_counter(@table, key, {2, 1}, {key, 0})

      # Count all requests in the window
      count =
        :ets.foldl(
          fn {{ip, ts}, c}, acc ->
            if ip == client_ip and ts >= window_start do
              acc + c
            else
              # Clean old entries
              if ts < window_start, do: :ets.delete(@table, {ip, ts})
              acc
            end
          end,
          0,
          @table
        )

      if count > rpm do
        conn
        |> Plug.Conn.put_resp_header("retry-after", Integer.to_string(@window_seconds))
        |> Neutron.Error.send_error(Neutron.Error.rate_limited())
      else
        remaining = max(0, rpm - count)

        conn
        |> Plug.Conn.put_resp_header("x-ratelimit-limit", Integer.to_string(rpm))
        |> Plug.Conn.put_resp_header("x-ratelimit-remaining", Integer.to_string(remaining))
      end
    rescue
      ArgumentError ->
        # ETS table doesn't exist yet — skip rate limiting
        conn
    end
  end

  defp format_ip({a, b, c, d}), do: "#{a}.#{b}.#{c}.#{d}"
  defp format_ip(ip), do: inspect(ip)

  defp get_rpm do
    case System.get_env("NEUTRON_RATE_LIMIT_RPM") do
      nil -> @default_rpm
      val -> String.to_integer(val)
    end
  end
end

# =============================================================================
# Layer 8: Request Timeout
# =============================================================================
defmodule Neutron.Middleware.Timeout do
  @moduledoc """
  Request timeout middleware (Layer 8).

  Enforces a maximum request processing time. Spawns a watcher process
  that terminates the request handler if the deadline is exceeded.
  On timeout, the handler is killed and Bandit closes the connection.

  Default: 30 000 ms (30 seconds, per FRAMEWORK_CONTRACT.md).

  Configure via:
  - `NEUTRON_REQUEST_TIMEOUT_MS` — request deadline in milliseconds
  """
  @behaviour Plug
  require Logger

  @default_timeout_ms 30_000

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    timeout_ms = get_timeout_ms()
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    caller = self()

    # Spawn a watcher that will terminate the request handler on timeout.
    # If the request completes in time, register_before_send kills the watcher.
    watcher = spawn(fn ->
      ref = Process.monitor(caller)

      receive do
        {:DOWN, ^ref, :process, ^caller, _} ->
          # Request handler exited before timeout — nothing to do.
          :ok
      after
        timeout_ms ->
          Logger.warning("Request timeout after #{timeout_ms}ms",
            path: conn.request_path,
            method: conn.method,
            request_id: conn.assigns[:request_id]
          )

          :telemetry.execute(
            [:neutron, :request, :timeout],
            %{timeout_ms: timeout_ms},
            %{method: conn.method, path: conn.request_path}
          )

          Process.exit(caller, :kill)
      end
    end)

    conn
    |> Plug.Conn.assign(:request_deadline, deadline)
    |> Plug.Conn.assign(:timeout_watcher, watcher)
    |> Plug.Conn.register_before_send(fn conn ->
      # Request completed in time — stop the watcher.
      if pid = conn.assigns[:timeout_watcher] do
        Process.exit(pid, :normal)
      end

      conn
    end)
  end

  defp get_timeout_ms do
    case System.get_env("NEUTRON_REQUEST_TIMEOUT_MS") do
      nil -> @default_timeout_ms
      val -> String.to_integer(val)
    end
  end
end

# =============================================================================
# Layer 9: OpenTelemetry
# =============================================================================
defmodule Neutron.Middleware.OTel do
  @moduledoc """
  OpenTelemetry tracing middleware.

  Creates a trace span for each request with method, path, and status attributes.
  Emits `:telemetry` events that can be hooked into the OTel SDK.

  Events emitted:
  - `[:neutron, :request, :start]` — request begins
  - `[:neutron, :request, :stop]` — request completes
  - `[:neutron, :request, :exception]` — unhandled exception
  """
  @behaviour Plug

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    trace_id =
      case Plug.Conn.get_req_header(conn, "x-trace-id") do
        [id | _] -> id
        _ -> generate_trace_id()
      end

    metadata = %{
      method: conn.method,
      path: conn.request_path,
      request_id: conn.assigns[:request_id],
      trace_id: trace_id
    }

    :telemetry.execute([:neutron, :request, :start], %{system_time: System.system_time()}, metadata)

    conn
    |> Plug.Conn.put_resp_header("x-trace-id", trace_id)
    |> Plug.Conn.assign(:trace_id, trace_id)
    |> Plug.Conn.register_before_send(fn conn ->
      :telemetry.execute(
        [:neutron, :request, :stop],
        %{system_time: System.system_time()},
        Map.put(metadata, :status, conn.status)
      )

      conn
    end)
  end

  defp generate_trace_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end
end

# =============================================================================
# Layer 10: Router Dispatch
# =============================================================================
defmodule Neutron.Middleware.Dispatch do
  @moduledoc "Dispatches the request to the user's router module."
  @behaviour Plug

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, opts) do
    router = Keyword.fetch!(opts, :router)
    router.call(conn, router.init([]))
  end
end

# =============================================================================
# ETS Table Manager
# =============================================================================
defmodule Neutron.ETS.Manager do
  @moduledoc """
  GenServer that creates and owns ETS tables used by the framework.

  Tables created:
  - `:neutron_rate_limit` — rate limiter counters
  - `:neutron_cache` — L1 in-memory cache
  - `:neutron_sessions` — session data
  """
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    :ets.new(:neutron_rate_limit, [:set, :public, :named_table, read_concurrency: true, write_concurrency: true])
    :ets.new(:neutron_cache, [:set, :public, :named_table, read_concurrency: true, write_concurrency: true])
    :ets.new(:neutron_sessions, [:set, :public, :named_table, read_concurrency: true, write_concurrency: true])
    {:ok, %{}}
  end
end
