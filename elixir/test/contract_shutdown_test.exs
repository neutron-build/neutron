defmodule Neutron.Contract.ShutdownTest do
  # FRAMEWORK_CONTRACT §8: catch SIGTERM/SIGINT, stop accepting, drain
  # in-flight requests with a configurable timeout defaulting to 30s.
  #
  # Two parts:
  #  * configuration wiring — NEUTRON_SHUTDOWN_TIMEOUT (default 30_000) must
  #    reach the HTTP server's drain setting, not sit unused in Config;
  #  * observed behaviour — a real Bandit server started through
  #    Neutron.child_spec/1 lets an in-flight request finish while stopping and
  #    refuses new connections once stopped.
  use ExUnit.Case, async: false
  import Plug.Test

  defmodule SpecRouter do
    use Neutron.Router

    get "/ok" do
      json(conn, 200, %{ok: true})
    end
  end

  # The route signals it is mid-flight, takes longer than the stop needs to
  # propagate, then answers. If the server drained it, the client sees 200.
  defmodule SlowRouter do
    use Neutron.Router

    get "/slow" do
      if probe = Process.whereis(ContractShutdownProbe), do: send(probe, :inflight)
      Process.sleep(400)
      json(conn, 200, %{drained: true})
    end
  end

  describe "child_spec/1 wiring" do
    test "wires the 30s default drain timeout into the HTTP server" do
      spec = Neutron.child_spec(router: SpecRouter)
      assert %{start: {Bandit, :start_link, [bandit_opts]}} = spec

      timeout = bandit_opts[:thousand_island_options][:shutdown_timeout]
      assert timeout == 30_000
    end

    test "NEUTRON_SHUTDOWN_TIMEOUT overrides the drain timeout" do
      System.put_env("NEUTRON_SHUTDOWN_TIMEOUT", "1500")

      try do
        spec = Neutron.child_spec(router: SpecRouter)
        assert %{start: {Bandit, :start_link, [bandit_opts]}} = spec
        assert bandit_opts[:thousand_island_options][:shutdown_timeout] == 1500
      after
        System.delete_env("NEUTRON_SHUTDOWN_TIMEOUT")
      end
    end
  end

  describe "drain, observed against a real server" do
    setup do
      Process.register(self(), ContractShutdownProbe)
      on_exit(fn -> unregister_probe() end)
      :ok
    end

    test "an in-flight request completes and new connections are refused" do
      {:ok, sup} =
        Supervisor.start_link(
          [Neutron.child_spec(router: SlowRouter, port: 0, host: "127.0.0.1")],
          strategy: :one_for_one
        )

      bandit_pid =
        sup
        |> Supervisor.which_children()
        |> Enum.find_value(fn
          {Neutron, pid, _, _} -> pid
          _ -> nil
        end)

      {:ok, {_addr, port}} = ThousandIsland.listener_info(bandit_pid)

      # Raw HTTP/1.1 over gen_tcp: inets' httpc is unusable on this OTP
      # (http_util.timestamp/0 undefined), and the point is observing the
      # connection outliving the shutdown handshake, not the client.
      request =
        Task.async(fn ->
          {:ok, sock} =
            :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])

          :ok =
            :gen_tcp.send(
              sock,
              "GET /slow HTTP/1.1\r\nhost: 127.0.0.1\r\nconnection: close\r\n\r\n"
            )

          recv_all(sock, "")
        end)

      # The request is now inside the handler; begin shutdown.
      assert_receive :inflight, 5_000

      # Blocks until the supervision tree is down — the in-flight request must
      # be drained within the child's shutdown timeout, not killed.
      Supervisor.stop(sup)

      response = Task.await(request)
      assert response =~ "HTTP/1.1 200"
      assert response =~ "drained"

      # Listening socket is gone: new connections are refused.
      assert {:error, _} = :gen_tcp.connect(~c"127.0.0.1", port, [])
    end

    defp recv_all(sock, acc) do
      case :gen_tcp.recv(sock, 0, 5_000) do
        {:ok, data} -> recv_all(sock, acc <> data)
        {:error, :closed} -> acc
        {:error, reason} -> flunk("recv failed during drain: #{inspect(reason)}")
      end
    end

    defp unregister_probe do
      if Process.whereis(ContractShutdownProbe) do
        Process.unregister(ContractShutdownProbe)
      end

      :ok
    rescue
      _ -> :ok
    end
  end

  describe "SIGTERM handling" do
    # The handler is exercised on a PRIVATE gen_event manager. Notifying the
    # VM-global :erl_signal_server from a test would also wake the kernel's
    # default handler, which calls :init.stop/0 — poisoning the rest of the
    # suite with a VM that is shutting down (ETS tables disappearing under
    # unrelated tests). The production wiring in Neutron.App is verified
    # end-to-end by an external SIGTERM run instead.
    test "Neutron.Signals stops the VM on sigterm via the injected stop function" do
      self_pid = self()
      stop_fun = fn -> send(self_pid, :vm_stop_called) end

      {:ok, mgr} = :gen_event.start()
      :ok = :gen_event.add_handler(mgr, Neutron.Signals, stop_fun: stop_fun)
      on_exit(fn -> :gen_event.stop(mgr) end)

      # The same event shape :erl_signal_server forwards to handlers.
      :gen_event.sync_notify(mgr, {:signal, :sigterm, :last})
      assert_receive :vm_stop_called, 1_000
    end

    test "other signals are ignored" do
      self_pid = self()
      stop_fun = fn -> send(self_pid, :vm_stop_called) end

      {:ok, mgr} = :gen_event.start()
      :ok = :gen_event.add_handler(mgr, Neutron.Signals, stop_fun: stop_fun)
      on_exit(fn -> :gen_event.stop(mgr) end)

      :gen_event.sync_notify(mgr, {:signal, :sigusr2, :last})
      refute_receive :vm_stop_called, 200
    end
  end
end
