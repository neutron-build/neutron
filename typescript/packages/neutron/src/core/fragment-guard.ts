/**
 * App-mode composition mounts a rendered route/layout inside the shell's
 * `<div id="app">` (the shell owns `<html>`/`<head>`/`<body>`). A route or
 * layout that renders a full document instead of a fragment would be nested
 * inside `#app` — markup browsers silently flatten and hydration then
 * duplicates (the page renders twice). This guard rejects it with an
 * actionable error before it ships.
 *
 * Shared by every compose site that injects into `#app`: the runtime SSR
 * server, the SSG static renderer, the dev-mode SSR path, and the build CLI.
 */
const FULL_DOCUMENT_START = /^\s*<(?:!doctype\s|html[\s/>]|body[\s/>])/i;

export function assertRenderedFragment(rendered: string, sourceFile?: string): void {
  if (!FULL_DOCUMENT_START.test(rendered)) {
    return;
  }
  const where = sourceFile ? ` (in ${sourceFile})` : "";
  throw new Error(
    `route/layout rendered a full document (<html>)${where} — in app mode the shell owns the document; render a fragment instead (move charset/title to index.html or a head() export)`
  );
}

export function decodeChunkStart(chunk: Uint8Array): string {
  return new TextDecoder().decode(chunk.subarray(0, 256));
}
