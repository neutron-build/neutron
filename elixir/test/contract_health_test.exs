defmodule Neutron.Contract.HealthTest do
  # FRAMEWORK_CONTRACT §7: `nucleus` is the tri-state HEALTH of the dependency
  # ("connected" | "disconnected" | "unconfigured") and `status` degrades to
  # "degraded" when a configured dependency is unreachable. The unconfigured
  # path is already pinned in test/health_test.exs; this file pins the other
  # two with stub processes registered under the Nucleus.Client name — hence
  # async: false.
  use ExUnit.Case, async: false
  import Plug.Test

  # Answers the same :is_nucleus? GenServer.call the real client does. The
  # `answer` it gives is deliberately arbitrary: §7 says health is not feature
  # detection, so ANY successful answer must read as "connected". Started with
  # start/4, not start_link/4: the real request process is never LINKED to
  # Nucleus.Client, and a link here would route the stub's death straight into
  # the test process as an exit signal, which no try/catch in Health can see.
  defmodule ReachableStub do
    use GenServer, restart: :temporary

    def start(answer),
      do: GenServer.start(__MODULE__, answer, name: Nucleus.Client)

    @impl true
    def init(answer), do: {:ok, answer}

    @impl true
    def handle_call(:is_nucleus?, _from, answer), do: {:reply, answer, answer}
  end

  # Configured but unhealthy: dies on any call.
  defmodule CrashingStub do
    use GenServer, restart: :temporary

    def start(_opts),
      do: GenServer.start(__MODULE__, nil, name: Nucleus.Client)

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call(_msg, _from, _state), do: exit(:unhealthy)
  end

  test "reachable nucleus dependency reads connected, even on plain PostgreSQL" do
    {:ok, _} = ReachableStub.start(false)
    on_exit(fn -> maybe_stop() end)

    body = health_body()

    assert body["nucleus"] == "connected"
    assert body["status"] == "ok"
    assert body["version"] == Neutron.version()
  end

  test "configured but unreachable nucleus degrades the service" do
    {:ok, _} = CrashingStub.start(nil)
    on_exit(fn -> maybe_stop() end)

    body = health_body()

    assert body["nucleus"] == "disconnected"
    assert body["status"] == "degraded"
  end

  defp health_body do
    conn = conn(:get, "/health") |> Neutron.Health.call([])
    assert conn.status == 200
    Jason.decode!(conn.resp_body)
  end

  defp maybe_stop do
    case Process.whereis(Nucleus.Client) do
      nil ->
        :ok

      pid ->
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, _, _, _} -> :ok
        end
    end
  end
end
