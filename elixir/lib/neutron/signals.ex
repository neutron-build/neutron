defmodule Neutron.Signals do
  @moduledoc """
  OS signal handling for graceful shutdown (FRAMEWORK_CONTRACT §8).

  Verified behaviour of a plain BEAM: SIGTERM is handled by default — the
  kernel's `:erl_signal_server` logs "SIGTERM received - shutting down" and
  calls `:init.stop/0`, which stops applications in reverse order and lets
  supervisors drain. This handler makes the framework's catch explicit and
  observable: it runs on `:erl_signal_server` and drives `:init.stop/0`
  itself, so shutdown works even if the default handler is later replaced.

  SIGINT is a deliberate GAP in this SDK: the BEAM reserves it for the break
  handler (`:os.set_signal/2` rejects it — "invalid signal name"), and with
  no tty it aborts the VM without a drain. It cannot be rerouted from inside
  a running VM; a deployment that needs SIGINT to drain must arrange for
  SIGTERM instead (e.g. `ExecReload=`/`KillSignal=sigterm` under systemd), or
  start the VM with `+Bi` and send SIGTERM.

  `:init.stop/0` is the graceful shutdown: it terminates applications in
  reverse start order, so the HTTP server (Bandit/ThousandIsland) stops
  accepting, drains in-flight requests up to its `shutdown_timeout`
  (`NEUTRON_SHUTDOWN_TIMEOUT`, default 30 000 ms), and the Nucleus pool
  closes in `Nucleus.Client.terminate/2`.

  The stop function is injectable (`:stop_fun`) so tests can observe the call
  without stopping the test VM.
  """

  @behaviour :gen_event

  @doc """
  Installs the handler on `:erl_signal_server`.

  Options:

    * `:stop_fun` — 0-arity function called on sigterm
      (default: `&:init.stop/0`)
  """
  @spec attach(keyword()) :: :gen_event.on_start()
  def attach(opts \\ []) do
    :gen_event.add_handler(:erl_signal_server, __MODULE__, opts)
  end

  @doc "Removes the handler (idempotent)."
  @spec detach() :: :ok
  def detach do
    :gen_event.delete_handler(:erl_signal_server, __MODULE__, [])
    :ok
  catch
    _, _ -> :ok
  end

  @impl :gen_event
  def init(opts) do
    {:ok, Keyword.get(opts, :stop_fun, &:init.stop/0)}
  end

  @impl :gen_event
  def handle_event({:signal, :sigterm, _last}, stop_fun) do
    require Logger

    Logger.info("[Neutron] SIGTERM received — stopping for graceful shutdown")
    stop_fun.()
    {:ok, stop_fun}
  end

  def handle_event(_event, stop_fun), do: {:ok, stop_fun}

  @impl :gen_event
  def handle_call(_msg, stop_fun), do: {:ok, :ok, stop_fun}

  @impl :gen_event
  def handle_info(_msg, stop_fun), do: {:ok, stop_fun}
end
