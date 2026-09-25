import assert from "node:assert/strict";
import test from "node:test";
import {
  pgListener,
  postgresJsListener,
  notifyStatement,
  type PgNotifyClient,
  type ListenEvent,
  type ListenLogger,
} from "./listen-notify.js";

// ---------------------------------------------------------------------------
// X05 unit coverage for /listen-notify: statement rendering, validation,
// lifecycle/cancellation, and the borrowed/owned contract — against fake
// clients. Live delivery semantics (commit-time delivery, rollback drops
// notifications, disconnect gaps, session-count assertions) live in
// live.x05.listen-notify.postgres.test.ts.
// ---------------------------------------------------------------------------

test("listen-notify: notifyStatement renders a quoted channel and escaped payload", () => {
  assert.equal(notifyStatement("events"), 'NOTIFY "events"');
  assert.equal(notifyStatement("events", "hello"), `NOTIFY "events", 'hello'`);
  // A single quote inside a DOUBLE-quoted identifier is an ordinary
  // character — only double quotes double inside double quotes.
  assert.equal(notifyStatement("ev'ents", "it's"), `NOTIFY "ev'ents", 'it''s'`);
  assert.equal(notifyStatement('say "hi"', ""), 'NOTIFY "say ""hi"""');
});

test("listen-notify: payload over 8000 bytes is rejected before the wire", () => {
  assert.throws(() => notifyStatement("ch", "x".repeat(8001)), /8000-byte limit/);
  assert.doesNotThrow(() => notifyStatement("ch", "x".repeat(8000)));
});

test("listen-notify: channel validation (non-empty, <=63 bytes, no NUL)", () => {
  assert.throws(() => notifyStatement(""), /non-empty/);
  assert.throws(() => notifyStatement("x".repeat(64)), /63-byte limit/);
  assert.throws(() => notifyStatement("a\0b"), /NUL/);
  // 63 bytes of ASCII is fine; a multibyte name is measured in bytes.
  assert.doesNotThrow(() => notifyStatement("x".repeat(63)));
  assert.throws(() => notifyStatement("é".repeat(32)), /63-byte limit/);
});

function fakePgClient(): PgNotifyClient & { sent: string[]; ended: boolean; emit(n: unknown): void; errorListeners: unknown[]; endListeners: unknown[] } {
  const sent: string[] = [];
  const errLs: unknown[] = [];
  const endLs: unknown[] = [];
  const notifLs: unknown[] = [];
  const client = {
    sent,
    ended: false,
    errorListeners: errLs,
    endListeners: endLs,
    async query(sql: string) {
      sent.push(sql);
      return { rows: [], rowCount: 0 };
    },
    on(event: string, listener: unknown) {
      if (event === "error") errLs.push(listener);
      if (event === "end") endLs.push(listener);
      if (event === "notification") notifLs.push(listener);
    },
    removeListener(_event: string, listener: unknown) {
      const from = [_event === "error" ? errLs : _event === "end" ? endLs : notifLs];
      from[0] = from[0].filter((l) => l !== listener);
    },
    async end() {
      client.ended = true;
    },
    emit(n: unknown) {
      for (const l of notifLs as ((x: unknown) => void)[]) l(n);
    },
  };
  return client as unknown as ReturnType<typeof fakePgClient>;
}

test("listen-notify: borrowed pg listener sends LISTEN with quoted channel, notifies, unlistens its own channels only", async () => {
  const client = fakePgClient();
  const events: ListenEvent[] = [];
  const logger: ListenLogger = (e) => events.push(e);
  const listener = await pgListener({ client, logger });
  const got: { channel: string; payload: string }[] = [];
  await listener.listen(["Alpha", "beta"], (n) => got.push({ channel: n.channel, payload: n.payload }));
  assert.deepEqual(client.sent, ['LISTEN "Alpha"', 'LISTEN "beta"']);
  client.emit({ channel: "Alpha", payload: "one", processId: 7 });
  client.emit({ channel: "beta", payload: "", processId: 7 });
  assert.deepEqual(got, [
    { channel: "Alpha", payload: "one" },
    { channel: "beta", payload: "" },
  ]);
  await listener.notify("Alpha", "out");
  assert.equal(client.sent.at(-1), `NOTIFY "Alpha", 'out'`);
  await listener.unlisten("beta");
  assert.equal(client.sent.at(-1), 'UNLISTEN "beta"');
  // close() unlistens ONLY what this listener subscribed; the borrowed
  // client is never ended.
  await listener.close();
  assert.ok(client.sent.includes('UNLISTEN "Alpha"'));
  assert.equal(client.ended, false, "borrowed client must stay open for its owner");
  assert.deepEqual(
    events.filter((e) => e.kind === "listen-open").map((e) => (e as { channel: string }).channel),
    ["Alpha", "beta"],
  );
  assert.ok(events.some((e) => e.kind === "listener-closed"));
  // Lifecycle events never carry payloads.
  assert.ok(!JSON.stringify(events).includes("one"));
  assert.ok(!JSON.stringify(events).includes("out"));
});

test("listen-notify: borrowed listener normalizes Nucleus quoted channel keys (same as owned)", async () => {
  // Nucleus keys channels by the raw statement text, so a channel subscribed
  // as "Alpha" is reported back as '"Alpha"'. BOTH listener modes must map it
  // onto the subscribed name; a channel this listener never subscribed passes
  // through untouched.
  const client = fakePgClient();
  const listener = await pgListener({ client });
  const got: string[] = [];
  await listener.listen(["Alpha"], (n) => got.push(n.channel));
  client.emit({ channel: '"Alpha"', payload: "nucleus", processId: 7 });
  client.emit({ channel: '"not-subscribed"', payload: "x", processId: 7 });
  client.emit({ channel: "Plain", payload: "y", processId: 7 });
  assert.deepEqual(got, ["Alpha", '"not-subscribed"', "Plain"]);
  await listener.close();
});

test("listen-notify: operations on a closed listener fail closed", async () => {
  const client = fakePgClient();
  const listener = await pgListener({ client });
  await listener.close();
  await assert.rejects(() => listener.listen("ch", () => {}), /closed/);
  await assert.rejects(() => listener.notify("ch", "x"), /closed/);
  await assert.rejects(() => listener.unlisten(), /closed/);
});

test("listen-notify: AbortSignal closes the listener (I02 cancellation reaches the connection)", async () => {
  const client = fakePgClient();
  const controller = new AbortController();
  const listener = await pgListener({ client, signal: controller.signal });
  await listener.listen("ch", () => {});
  controller.abort();
  await new Promise((r) => setTimeout(r, 20));
  assert.ok(client.sent.includes('UNLISTEN "ch"'));
  assert.equal(client.ended, false, "borrowed client is unlistened, never ended");
  await assert.rejects(() => listener.notify("ch", "x"), /closed/);
});

test("listen-notify: pgListener takes exactly one of url or client", async () => {
  await assert.rejects(() => pgListener({}), /exactly one/);
  await assert.rejects(() => pgListener({ url: "postgres://x", client: fakePgClient() }), /exactly one/);
});

test("listen-notify: listen rejects duplicates and empty lists; unlisten of a foreign channel fails", async () => {
  const client = fakePgClient();
  const listener = await pgListener({ client });
  await assert.rejects(() => listener.listen([], () => {}), /at least one/);
  await assert.rejects(() => listener.listen(["a", "a"], () => {}), /duplicate/);
  await listener.listen("a", () => {});
  await assert.rejects(() => listener.listen("a", () => {}), /already listening/);
  await assert.rejects(() => listener.unlisten("nope"), /not listening/);
  await listener.close();
});

test("listen-notify: postgres.js listener maps channels to native handles and never ends the client", async () => {
  const unlistens: string[] = [];
  const sent: string[] = [];
  const handles = new Map<string, { unlisten: () => Promise<void> }>();
  const jsClient = {
    async listen(channel: string, _onNotify: (payload: string) => void) {
      const handle = {
        async unlisten() {
          unlistens.push(channel);
        },
      };
      handles.set(channel, handle);
      return handle;
    },
    async unsafe(sql: string) {
      sent.push(sql);
    },
  };
  const listener = await postgresJsListener({ client: jsClient });
  const got: [string, string][] = [];
  await listener.listen(["a", "b"], (payload, channel) => got.push([channel, payload]));
  assert.deepEqual(listener.channels, ["a", "b"]);
  await listener.notify("a", "hello");
  assert.deepEqual(sent, [`NOTIFY "a", 'hello'`]);
  await listener.unlisten("a");
  assert.deepEqual(unlistens, ["a"]);
  await listener.close();
  assert.deepEqual(unlistens, ["a", "b"]);
  assert.equal("end" in jsClient, false, "the borrowed handle is never ended");
});
