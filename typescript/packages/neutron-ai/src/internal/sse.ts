export interface SSEEvent {
  event?: string;
  data: string;
}

/** Minimal server-sent-events parser over a fetch body stream. */
export async function* parseSSE(body: ReadableStream<Uint8Array>): AsyncGenerator<SSEEvent, void, undefined> {
  const decoder = new TextDecoder();
  const reader = body.getReader();
  let buffer = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let end: { index: number; length: number } | null;
      while ((end = findEventEnd(buffer)) !== null) {
        const block = buffer.slice(0, end.index);
        buffer = buffer.slice(end.index + end.length);
        const event = parseEventBlock(block);
        if (event !== null) yield event;
      }
    }
    buffer += decoder.decode();
    const trailing = parseEventBlock(buffer);
    if (trailing !== null) yield trailing;
  } finally {
    // Covers both exits: a consumer that breaks after a terminal event and a
    // thrown error. releaseLock alone leaves the body unread-but-open — the
    // provider keeps streaming until GC. cancel() through the reader reaches
    // the underlying source even while locked, and is a no-op once drained.
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

function findEventEnd(buffer: string): { index: number; length: number } | null {
  const lf = buffer.indexOf("\n\n");
  const crlf = buffer.indexOf("\r\n\r\n");
  if (lf === -1 && crlf === -1) return null;
  if (crlf !== -1 && (lf === -1 || crlf < lf)) return { index: crlf, length: 4 };
  return { index: lf, length: 2 };
}

function parseEventBlock(block: string): SSEEvent | null {
  let eventName: string | undefined;
  const dataLines: string[] = [];
  for (const rawLine of block.split(/\r?\n/)) {
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    if (line === "" || line.startsWith(":")) continue;
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") eventName = value;
    else if (field === "data") dataLines.push(value);
  }
  if (eventName === undefined && dataLines.length === 0) return null;
  const event: SSEEvent = { data: dataLines.join("\n") };
  if (eventName !== undefined) event.event = eventName;
  return event;
}
