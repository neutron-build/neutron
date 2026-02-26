import { InMemoryQueueDriver, type QueueDriver } from "../queue/index.js";

export interface JobsOptions {
  driver?: QueueDriver;
}

export function createJobs(options: JobsOptions = {}): QueueDriver {
  return options.driver || new InMemoryQueueDriver();
}

