import { expect, it } from 'vitest';
import { createRouter } from '../core/router.js';
import { matchRoute, extractParams } from './navigate.js';
import type { Route } from '../core/types.js';
const patterns = ['/docs/*', '/files/*rest.json', '/files/:name.json', '/files/readme.json', '/users/:id', '/users/:id.json'];
const router = createRouter();
for (const routePath of patterns) router.insert({ id: routePath, path: routePath, file: '', params: [], config: { mode: 'app' }, parentId: null } as Route);
it.each(['/docs/a/b', '/files/a/b.json', '/files/a.json', '/files/readme.json', '/users/a%20b', '/users/a.json'])('TS-F17 client matches the server winner/params for %s', pathname => {
  const server = router.match(decodeURIComponent(pathname));
  expect(matchRoute(pathname, patterns)).toBe(server?.route.path);
  expect(extractParams(server!.route.path, pathname)).toEqual(server!.params);
});
