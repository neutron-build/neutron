/** Bounded cache-side stream capture. Cancellation must not await the client tee. */
export async function captureCacheBody(response: Response, maxBytes: number, timeoutMs: number): Promise<Uint8Array | null> {
  if (!response.body) return new Uint8Array();
  const reader = response.clone().body!.getReader();
  const chunks: Uint8Array[] = [];
  const deadline = performance.now() + timeoutMs;
  let bytes = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<null>(resolve => { timer = setTimeout(() => resolve(null), timeoutMs); });
  try {
    while (true) {
      if (performance.now() >= deadline) return null;
      const result = await Promise.race([reader.read(), expired]);
      if (!result) return null;
      if (result.done) break;
      bytes += result.value.byteLength;
      if (bytes > maxBytes) return null;
      if (result.value.byteLength) chunks.push(result.value);
    }
    const body = new Uint8Array(bytes);
    let offset = 0;
    for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
    return body;
  } finally {
    clearTimeout(timer);
    void reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
