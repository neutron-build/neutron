import { getDataRuntime } from "./data/runtime.js";

interface WorkerContext {
  mode: string;
  args: string[];
  signal: AbortSignal;
  log: (message: string) => void;
}

export async function run(context: WorkerContext): Promise<() => Promise<void>> {
  const runtime = await getDataRuntime();

  await runtime.queue.process("todo.changed", async (job) => {
    context.log(
      `processed job=${job.name} id=${job.id} payload=${JSON.stringify(job.payload)}`
    );
  });

  context.log(
    `ready profile=${runtime.profile} queue=${runtime.drivers.queue} mode=${context.mode}`
  );

  return async () => {
    await runtime.close();
    context.log("shutdown complete");
  };
}
