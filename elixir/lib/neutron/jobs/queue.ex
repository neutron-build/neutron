defmodule Neutron.Jobs.Queue do
  @moduledoc """
  Oban-style job queue backed by Nucleus.

  Provides reliable background job processing with retries, scheduling,
  and priority queues. Jobs are persisted to Nucleus for durability.

  ## Defining a Worker

      defmodule MyApp.Workers.SendEmail do
        use Neutron.Jobs.Worker

        @impl true
        def perform(%{to: to, subject: subject, body: body}) do
          # Send the email...
          :ok
        end
      end

  ## Enqueuing Jobs

      # Enqueue immediately
      Neutron.Jobs.Queue.enqueue(MyApp.Workers.SendEmail, %{
        to: "user@example.com",
        subject: "Welcome!",
        body: "Hello!"
      })

      # Schedule for later
      Neutron.Jobs.Queue.enqueue(MyApp.Workers.SendEmail, args, schedule_in: 300)

      # With priority (lower = higher priority)
      Neutron.Jobs.Queue.enqueue(MyApp.Workers.SendEmail, args, priority: 1)

  ## Job Lifecycle

  Jobs go through states: `pending` -> `running` -> `completed` | `failed` | `retrying`
  """

  use GenServer
  require Logger

  @poll_interval 5_000
  @max_retries 3
  @default_queue "default"

  @type job :: %{
          id: String.t(),
          worker: module(),
          args: map(),
          queue: String.t(),
          priority: non_neg_integer(),
          state: :pending | :running | :completed | :failed | :retrying,
          attempt: non_neg_integer(),
          max_retries: non_neg_integer(),
          scheduled_at: integer(),
          inserted_at: integer(),
          completed_at: integer() | nil,
          error: String.t() | nil
        }

  # --- Client API ---

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Enqueues a job for processing.

  ## Options

    * `:queue` — queue name (default: "default")
    * `:priority` — job priority, lower = higher (default: 10)
    * `:max_retries` — max retry attempts (default: 3)
    * `:schedule_in` — delay in seconds before the job runs
    * `:scheduled_at` — Unix timestamp when the job should run
  """
  @spec enqueue(module(), map(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def enqueue(worker, args, opts \\ []) do
    now = System.system_time(:second)

    scheduled_at =
      cond do
        Keyword.has_key?(opts, :scheduled_at) -> Keyword.get(opts, :scheduled_at)
        Keyword.has_key?(opts, :schedule_in) -> now + Keyword.get(opts, :schedule_in)
        true -> now
      end

    job = %{
      id: generate_job_id(),
      worker: worker,
      args: args,
      queue: Keyword.get(opts, :queue, @default_queue),
      priority: Keyword.get(opts, :priority, 10),
      state: :pending,
      attempt: 0,
      max_retries: Keyword.get(opts, :max_retries, @max_retries),
      scheduled_at: scheduled_at,
      inserted_at: now,
      completed_at: nil,
      error: nil
    }

    GenServer.call(__MODULE__, {:enqueue, job})
  end

  @doc """
  Returns all jobs in a given state.
  """
  @spec list(atom()) :: [job()]
  def list(state \\ :pending) do
    GenServer.call(__MODULE__, {:list, state})
  end

  @doc """
  Returns the count of jobs by state.
  """
  @spec counts() :: map()
  def counts do
    GenServer.call(__MODULE__, :counts)
  end

  @doc """
  Cancels a pending job by ID.
  """
  @spec cancel(String.t()) :: :ok | {:error, :not_found}
  def cancel(job_id) do
    GenServer.call(__MODULE__, {:cancel, job_id})
  end

  # --- GenServer Implementation ---

  @impl true
  def init(_opts) do
    # Schedule the first poll
    Process.send_after(self(), :poll, @poll_interval)

    {:ok,
     %{
       jobs: %{},
       running: MapSet.new()
     }}
  end

  @impl true
  def handle_call({:enqueue, job}, _from, state) do
    jobs = Map.put(state.jobs, job.id, job)
    persist_job(job)
    {:reply, {:ok, job.id}, %{state | jobs: jobs}}
  end

  @impl true
  def handle_call({:list, filter_state}, _from, state) do
    filtered =
      state.jobs
      |> Map.values()
      |> Enum.filter(&(&1.state == filter_state))
      |> Enum.sort_by(& &1.priority)

    {:reply, filtered, state}
  end

  @impl true
  def handle_call(:counts, _from, state) do
    counts =
      state.jobs
      |> Map.values()
      |> Enum.group_by(& &1.state)
      |> Enum.into(%{}, fn {state, jobs} -> {state, length(jobs)} end)

    {:reply, counts, state}
  end

  @impl true
  def handle_call({:cancel, job_id}, _from, state) do
    case Map.get(state.jobs, job_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      %{state: :pending} = job ->
        updated = %{job | state: :cancelled}
        jobs = Map.put(state.jobs, job_id, updated)
        {:reply, :ok, %{state | jobs: jobs}}

      _ ->
        {:reply, {:error, :not_pending}, state}
    end
  end

  @impl true
  def handle_info(:poll, state) do
    now = System.system_time(:second)

    # Find ready jobs
    ready =
      state.jobs
      |> Map.values()
      |> Enum.filter(fn job ->
        job.state in [:pending, :retrying] and
          job.scheduled_at <= now and
          job.id not in state.running
      end)
      |> Enum.sort_by(& &1.priority)
      |> Enum.take(10)

    # Execute ready jobs
    state = Enum.reduce(ready, state, &execute_job/2)

    # Schedule next poll
    Process.send_after(self(), :poll, @poll_interval)

    {:noreply, state}
  end

  @impl true
  def handle_info({:job_complete, job_id, result}, state) do
    state = complete_job(state, job_id, result)
    {:noreply, state}
  end

  # --- Internal ---

  defp execute_job(job, state) do
    parent = self()
    job_id = job.id

    # Update state to running
    updated_job = %{job | state: :running, attempt: job.attempt + 1}
    jobs = Map.put(state.jobs, job_id, updated_job)
    running = MapSet.put(state.running, job_id)

    # Run in a supervised task
    Task.Supervisor.async_nolink(
      get_or_start_task_supervisor(),
      fn ->
        try do
          result = updated_job.worker.perform(updated_job.args)
          send(parent, {:job_complete, job_id, {:ok, result}})
        rescue
          e ->
            send(parent, {:job_complete, job_id, {:error, Exception.message(e)}})
        catch
          kind, reason ->
            send(parent, {:job_complete, job_id, {:error, "#{kind}: #{inspect(reason)}"}})
        end
      end
    )

    %{state | jobs: jobs, running: running}
  end

  defp complete_job(state, job_id, result) do
    running = MapSet.delete(state.running, job_id)

    case Map.get(state.jobs, job_id) do
      nil ->
        %{state | running: running}

      job ->
        updated_job =
          case result do
            {:ok, _} ->
              %{job | state: :completed, completed_at: System.system_time(:second)}

            {:error, error} ->
              if job.attempt < job.max_retries do
                Logger.warning(
                  "[Neutron.Jobs] Job #{job_id} (#{inspect(job.worker)}) failed (attempt #{job.attempt}/#{job.max_retries}): #{error}"
                )

                # Exponential backoff: 2^attempt seconds
                backoff = :math.pow(2, job.attempt) |> round()

                %{
                  job
                  | state: :retrying,
                    error: error,
                    scheduled_at: System.system_time(:second) + backoff
                }
              else
                Logger.error(
                  "[Neutron.Jobs] Job #{job_id} (#{inspect(job.worker)}) permanently failed after #{job.max_retries} attempts: #{error}"
                )

                %{
                  job
                  | state: :failed,
                    error: error,
                    completed_at: System.system_time(:second)
                }
              end
          end

        persist_job(updated_job)
        jobs = Map.put(state.jobs, job_id, updated_job)
        %{state | jobs: jobs, running: running}
    end
  end

  defp persist_job(job) do
    case Process.whereis(Nucleus.Client) do
      nil ->
        :ok

      _pid ->
        try do
          serialized =
            job
            |> Map.update!(:worker, &inspect/1)
            |> Jason.encode!()

          Nucleus.Models.KV.set(
            Nucleus.Client,
            "neutron:job:#{job.id}",
            serialized,
            ttl: 86_400 * 7
          )
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
    end
  end

  defp generate_job_id do
    :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
  end

  defp get_or_start_task_supervisor do
    case Process.whereis(Neutron.Jobs.TaskSupervisor) do
      nil ->
        {:ok, pid} =
          Task.Supervisor.start_link(name: Neutron.Jobs.TaskSupervisor)

        pid

      pid ->
        pid
    end
  end
end

defmodule Neutron.Jobs.Worker do
  @moduledoc """
  Behaviour for job workers.

  ## Example

      defmodule MyApp.Workers.Cleanup do
        use Neutron.Jobs.Worker

        @impl true
        def perform(%{older_than: days}) do
          # Perform cleanup...
          :ok
        end
      end
  """

  @callback perform(args :: map()) :: :ok | {:ok, term()} | {:error, term()}

  defmacro __using__(_opts) do
    quote do
      @behaviour Neutron.Jobs.Worker
    end
  end
end
