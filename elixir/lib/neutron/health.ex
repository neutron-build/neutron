defmodule Neutron.Health do
  @moduledoc """
  Health check endpoint plug.

  Returns `GET /health` with Nucleus detection per FRAMEWORK_CONTRACT.md:

      {
        "status": "ok",
        "nucleus": true,
        "version": "0.1.0"
      }

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
        status: "ok",
        nucleus: nucleus_status,
        version: Neutron.version()
      })

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(200, body)
    |> Plug.Conn.halt()
  end

  def call(conn, _opts), do: conn

  defp detect_nucleus do
    case Process.whereis(Nucleus.Client) do
      nil ->
        false

      _pid ->
        try do
          Nucleus.Client.is_nucleus?(Nucleus.Client)
        rescue
          _ -> false
        catch
          :exit, _ -> false
        end
    end
  end
end
