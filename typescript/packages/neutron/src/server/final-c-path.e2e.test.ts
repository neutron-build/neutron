import { expect, it } from 'vitest';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { once } from 'node:events';
import { createServer, normalizePathname } from './index.js';
it.each(['/a%5c..%5csecret', '/a%00b', '/a%5c..%2fsecret', '/%2e%2e/secret'])('TS-F14 rejects filesystem-unsafe path %s independent of host platform', input => { expect(normalizePathname(input)).toBeNull(); });
it('TS-F14 retains legal dot slugs', () => { expect(normalizePathname('/release1..2')).toBe('/release1..2'); });
it('TS-F14 realpath containment rejects a static HTML directory symlink', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'neutron-final-path-'));
  await fs.mkdir(path.join(root, 'dist')); await fs.mkdir(path.join(root, 'external'));
  await fs.writeFile(path.join(root, 'external/index.html'), 'PRIVATE-OUTSIDE-ROOT');
  await fs.symlink(path.join(root, 'external'), path.join(root, 'dist/leak'), 'dir');
  const running = await createServer({ rootDir: root, port: 0, host: '127.0.0.1', compress: false });
  try {
    if (!running.server.listening) await once(running.server, 'listening');
    const address = running.server.address(); if (!address || typeof address === 'string') throw new Error('no port');
    const response = await fetch(`http://127.0.0.1:${address.port}/leak`); expect(response.status).toBe(404); expect(await response.text()).not.toContain('PRIVATE-OUTSIDE-ROOT');
  } finally { await running.close(); await fs.rm(root, { recursive: true, force: true }); }
});
it('TS-F12/15 transport peer metadata survives independent SSR module evaluation', async () => {
  const { installTransportPeer } = await import('./peer.js');
  const { vi } = await import('vitest');
  const request = new Request('https://example.test/'); installTransportPeer(request, '127.0.0.1');
  vi.resetModules();
  const { transportPeer } = await import('./peer.js');
  expect(transportPeer(request)?.remoteAddress).toBe('127.0.0.1');
});
