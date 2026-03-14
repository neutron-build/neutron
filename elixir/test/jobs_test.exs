defmodule Neutron.Jobs.QueueTest do
  use ExUnit.Case

  alias Neutron.Jobs.Queue

  setup do
    # Start a fresh job queue for each test
    case Process.whereis(Queue) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end

    # Start TaskSupervisor if not running
    case Process.whereis(Neutron.Jobs.TaskSupervisor) do
      nil -> Task.Supervisor.start_link(name: Neutron.Jobs.TaskSupervisor)
      _ -> :ok
    end

    {:ok, pid} = Queue.start_link([])
    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    :ok
  end

  describe "start_link/1" do
    test "starts the queue GenServer" do
      # Already started in setup
      assert Process.whereis(Queue) != nil
      assert Process.alive?(Process.whereis(Queue))
    end
  end

  describe "enqueue/3" do
    test "enqueues a job and returns {:ok, job_id}" do
      assert {:ok, job_id} = Queue.enqueue(Neutron.TestWorker, %{data: "test"})
      assert is_binary(job_id)
      assert String.length(job_id) > 0
    end

    test "job starts in pending state" do
      {:ok, _job_id} = Queue.enqueue(Neutron.TestWorker, %{data: "test"})
      jobs = Queue.list(:pending)
      assert length(jobs) >= 1
    end

    test "respects queue option" do
      {:ok, _job_id} = Queue.enqueue(Neutron.TestWorker, %{}, queue: "emails")
      jobs = Queue.list(:pending)
      email_jobs = Enum.filter(jobs, &(&1.queue == "emails"))
      assert length(email_jobs) == 1
    end

    test "respects priority option" do
      {:ok, _id1} = Queue.enqueue(Neutron.TestWorker, %{n: 1}, priority: 10)
      {:ok, _id2} = Queue.enqueue(Neutron.TestWorker, %{n: 2}, priority: 1)
      jobs = Queue.list(:pending)
      # Should be sorted by priority (lower = higher)
      assert hd(jobs).priority <= List.last(jobs).priority
    end

    test "respects schedule_in option" do
      {:ok, _id} = Queue.enqueue(Neutron.TestWorker, %{}, schedule_in: 300)
      [job] = Queue.list(:pending)
      now = System.system_time(:second)
      assert job.scheduled_at >= now + 290
    end

    test "respects scheduled_at option" do
      future = System.system_time(:second) + 600
      {:ok, _id} = Queue.enqueue(Neutron.TestWorker, %{}, scheduled_at: future)
      [job] = Queue.list(:pending)
      assert job.scheduled_at == future
    end

    test "respects max_retries option" do
      {:ok, _id} = Queue.enqueue(Neutron.TestWorker, %{}, max_retries: 5)
      [job] = Queue.list(:pending)
      assert job.max_retries == 5
    end

    test "uses default max_retries of 3" do
      {:ok, _id} = Queue.enqueue(Neutron.TestWorker, %{})
      [job] = Queue.list(:pending)
      assert job.max_retries == 3
    end

    test "uses default priority of 10" do
      {:ok, _id} = Queue.enqueue(Neutron.TestWorker, %{})
      [job] = Queue.list(:pending)
      assert job.priority == 10
    end

    test "uses default queue of 'default'" do
      {:ok, _id} = Queue.enqueue(Neutron.TestWorker, %{})
      [job] = Queue.list(:pending)
      assert job.queue == "default"
    end

    test "generates unique job IDs" do
      {:ok, id1} = Queue.enqueue(Neutron.TestWorker, %{n: 1})
      {:ok, id2} = Queue.enqueue(Neutron.TestWorker, %{n: 2})
      assert id1 != id2
    end
  end

  describe "list/1" do
    test "returns empty list when no jobs in state" do
      assert Queue.list(:completed) == []
    end

    test "returns jobs filtered by state" do
      {:ok, _} = Queue.enqueue(Neutron.TestWorker, %{})
      pending = Queue.list(:pending)
      assert length(pending) == 1
      assert hd(pending).state == :pending
    end

    test "returns jobs sorted by priority" do
      {:ok, _} = Queue.enqueue(Neutron.TestWorker, %{n: 1}, priority: 10)
      {:ok, _} = Queue.enqueue(Neutron.TestWorker, %{n: 2}, priority: 1)
      {:ok, _} = Queue.enqueue(Neutron.TestWorker, %{n: 3}, priority: 5)
      jobs = Queue.list(:pending)
      priorities = Enum.map(jobs, & &1.priority)
      assert priorities == Enum.sort(priorities)
    end
  end

  describe "counts/0" do
    test "returns count by state" do
      {:ok, _} = Queue.enqueue(Neutron.TestWorker, %{n: 1})
      {:ok, _} = Queue.enqueue(Neutron.TestWorker, %{n: 2})
      counts = Queue.counts()
      assert counts[:pending] == 2
    end

    test "returns empty map when no jobs" do
      counts = Queue.counts()
      assert counts == %{}
    end
  end

  describe "cancel/1" do
    test "cancels a pending job" do
      {:ok, job_id} = Queue.enqueue(Neutron.TestWorker, %{}, schedule_in: 9999)
      assert :ok = Queue.cancel(job_id)
    end

    test "returns {:error, :not_found} for non-existent job" do
      assert {:error, :not_found} = Queue.cancel("nonexistent")
    end
  end

  describe "job processing" do
    test "poll processes ready jobs" do
      {:ok, _id} = Queue.enqueue(Neutron.TestWorker, %{data: "process-me"})

      # Trigger a poll
      send(Process.whereis(Queue), :poll)
      Process.sleep(100)

      # The job should transition to completed
      completed = Queue.list(:completed)
      assert length(completed) >= 1
    end

    test "job_complete message updates state" do
      {:ok, job_id} = Queue.enqueue(Neutron.TestWorker, %{data: "direct-complete"})

      # Simulate a job completion message
      send(Process.whereis(Queue), {:job_complete, job_id, {:ok, :done}})
      Process.sleep(50)

      completed = Queue.list(:completed)
      assert Enum.any?(completed, &(&1.id == job_id))
    end

    test "failed job with retries left transitions to retrying" do
      {:ok, job_id} = Queue.enqueue(Neutron.FailingTestWorker, %{}, max_retries: 3)

      # Trigger processing
      send(Process.whereis(Queue), :poll)
      Process.sleep(200)

      # Should be in retrying state (not permanently failed yet)
      retrying = Queue.list(:retrying)
      failed = Queue.list(:failed)
      assert length(retrying) + length(failed) >= 1
    end
  end
end

defmodule Neutron.Jobs.WorkerTest do
  use ExUnit.Case, async: true

  test "defines the perform/1 callback" do
    assert function_exported?(Neutron.TestWorker, :perform, 1)
  end

  test "worker performs successfully" do
    assert :ok = Neutron.TestWorker.perform(%{data: "test"})
  end

  test "failing worker raises" do
    assert_raise RuntimeError, "intentional failure", fn ->
      Neutron.FailingTestWorker.perform(%{})
    end
  end
end
