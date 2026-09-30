import { once } from 'node:events';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { expect, it } from 'vitest';
import { decodeSerializedPayload } from '../core/serialization.js';
import { createServer } from './index.js';

it('shares request scope with cached functions loaded through the real SSR runtime', async () => {
  const root = await fs.mkdtemp(path.join(process.cwd(), '.tmp-neutron-request-scope-'));
  await fs.mkdir(path.join(root, 'src/routes'), { recursive: true });
  await fs.writeFile(path.join(root, 'src/routes/value.ts'), `
    import { cache } from '@neutron-build/core';
    let calls = 0;
    const read = cache(async () => ++calls);
    export const config = { mode: 'app' };
    export async function loader() {
      const first = read(); const second = read();
      return { value: await first, same: first === second };
    }
    export default function Page() { return null; }
  `);
  const running = await createServer({ rootDir: root, host: '127.0.0.1', port: 0, compress: false });
  try {
    if (!running.server.listening) await once(running.server, 'listening');
    const address = running.server.address();
    if (!address || typeof address === 'string') throw new Error('No HTTP port');
    for (const value of [1, 2]) {
      const response = await fetch(`http://127.0.0.1:${address.port}/value`, { headers: { Accept: 'application/json' } });
      const payload = decodeSerializedPayload<Record<string, unknown>>(await response.json());
      expect(Object.values(payload)[0]).toEqual({ value, same: true });
    }
  } finally { await running.close(); await fs.rm(root, { recursive: true, force: true }); }
});
