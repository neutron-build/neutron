import assert from 'node:assert/strict';
import test from 'node:test';
import { InMemoryQueueDriver, admitQueue } from './index.js';
import { BullMqQueueDriver } from './bullmq.js';
import { PostgresQueueDriver } from './postgres.js';
import { createJobs } from '../jobs/index.js';

test('consumer selection refuses unknown/custom native guarantees and requires explicit queue capabilities', async () => {
  const memory = new InMemoryQueueDriver();
  assert.equal(createJobs({ driver: memory, requiredCapabilities: { durability: 'process', routing: 'registered-names' } }), memory);
  assert.throws(() => createJobs({ driver: memory, requiredCapabilities: { durability: 'backend' } }), /not admitted/);
  const injected = new BullMqQueueDriver({ close: async () => {} } as any, class { async close() {} }, {}, 'q', { quit: async () => {} });
  assert.equal(injected.capabilities.claimFencing, 'unknown');
  assert.throws(() => admitQueue(injected, { durability: 'backend' }), /not admitted/);
  assert.throws(() => admitQueue({} as any, { acknowledgement: 'native' }), /not admitted/);
  await injected.close(); await assert.rejects(() => injected.unschedule('q'), /closed/);
  const pg = new PostgresQueueDriver({ end: async () => {} } as any);
  admitQueue(pg, { acknowledgement: 'fenced-uncertain-stop', errorObservation: 'workerError-and-close' });
  await pg.close(); await assert.rejects(() => pg.add('job', {}), /stopped/);
  memory.close(); await assert.rejects(() => memory.schedule('job', '* * * * *', {}), /closed/);
});
