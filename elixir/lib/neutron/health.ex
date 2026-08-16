defmodule Neutron.Health do
  @moduledoc """
  Health check endpoint plug.

  Returns `GET /health` per FRAMEWORK_CONTRACT.md §7:

      {
        "status": "ok",
        "nucleus": "connected" | "disconnected" | "unconfigured",
        "version": "0.1.0"
      }

  `nucleus` reports the HEALTH of the nucleus dependency, as a tri-state
  string — not whether the database happens to be a Nucleus instance. That is
  feature detection, and §7 says so outright: "Feature detection (is the
  connected DB a Nucleus instance vs plain Postgres) is §1, not `/health`."

  This used to return a BOOLEAN from `Nucleus.Client.is_nucleus?/1`, which got
  both halves wrong — the type and the meaning — and `test/health_test.exs`
  asserted `is_boolean(body["nucleus"])`, so the test passed and pinned it.

  ## Usage

  Add to your router:

      defmodule MyApp.Router do
        use Neutron.Router

        forward "/health", to: Neutron.Health
      end

  Or use the built-in health route (added automatically if you
  include `Neutron.Health.Plug` in your middleware pipeline):

      plug Neutron.Health.Plug
  """

  @behaviour Plug

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%{method: "GET", request_path: "/health"} = conn, _opts) do
    nucleus_status = detect_nucleus()

    body =
      Jason.encode!(%{
        # §7: `status` degrades when a CONFIGURED dependency is unreachable.
        # An unconfigured nucleus is explicitly "not an error".
        status: if(nucleus_status == "disconnected", do: "degraded", else: "ok"),
        nucleus: nucleus_status,
        version: Neutron.version()
      })

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(200, body)
    |> Plug.Conn.halt()
  end

  def call(conn, _opts), do: conn

  # "connected" | "disconnected" | "unconfigured", per FRAMEWORK_CONTRACT §7.
  #
  # No client process means no nucleus is configured for this service, which
  # the contract calls out as "not an error". A client that is running but
  # cannot answer is configured-but-unhealthy, which is "disconnected".
  defp detect_nucleus do
    case Process.whereis(Nucleus.Client) do
      nil ->
        "unconfigured"

      _pid ->
        try do
          # Any successful answer means the dependency is reachable. What it
          # answers — Nucleus or plain Postgres — is §1 feature detection and
          # deliberately not this field.
          _ = Nucleus.Client.is_nucleus?(Nucleus.Client)
          "connected"
        rescue
          _ -> "disconnected"
        catch
          :exit, _ -> "disconnected"
        end
    end
  end
end
