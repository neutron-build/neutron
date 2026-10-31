import { admitQueue, InMemoryQueueDriver, type QueueCapabilities, type QueueDriver } from "../queue/index.js";

export interface JobsOptions {
  driver?: QueueDriver;
  requiredCapabilities?: Partial<QueueCapabilities>;
}

export function createJobs(options: JobsOptions = {}): QueueDriver {
  return admitQueue(options.driver || new InMemoryQueueDriver(), options.requiredCapabilities ?? {});
}

