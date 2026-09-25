// ---------------------------------------------------------------------------
// @neutron-build/sql/listen-notify — PostgreSQL LISTEN/NOTIFY (X05)
// ---------------------------------------------------------------------------
// Import as `@neutron-build/sql/listen-notify`. The SQL-only root never loads
// this file; importing it is the explicit opt-in.
//
// LISTEN is SESSION-SCOPED: a channel subscription lives on exactly one
// server connection and dies with it. This module therefore NEVER checks a
// client out of a pool and holds it — that starves the pool (the I02
// poisoned-pool class). Instead:
//
//   - `pgListener({ url })` owns a dedicated connection end to end (created
//     here, closed by `close()`/AbortSignal);
//   - `pgListener({ client })` borrows an externally owned, already-dedicated
//     `pg` client (NOT a pool checkout): `close()` detaches and unlistens
//     but leaves the client open for its owner;
//   - `postgresJsListener({ client })` borrows a postgres.js handle and uses
//     its native `listen()` — postgres.js manages its own dedicated
//     subscription connection (and re-listens on reconnect; messages emitted
//     while disconnected are still lost).
//
// Channel names are rendered as DOUBLE-QUOTED identifiers on both sides
// (LISTEN and NOTIFY). That is case-preserving on PostgreSQL, it matches
// postgres.js's own `listen` implementation exactly, and on Nucleus the
// wire-level registry keys channels by the raw statement text — quoting both
// sides keeps them consistent. A channel name must be non-empty, at most 63
// bytes UTF-8, and free of NUL.
//
// NOTIFY carries no parameters in PostgreSQL: the statement form
// `NOTIFY "chan", 'payload'` is rendered with a validated channel and an
// escaped payload literal (single quotes doubled). The payload limit is 8000
// bytes — enforced client-side with a precise error so a too-large payload
// fails before it reaches the wire.
//
// Delivery semantics (PostgreSQL, documented behavior): notifications are
// delivered ONLY to sessions that are connected and listening at commit
// time. PostgreSQL queues them per session in commit order; they are NOT
// persisted — a disconnect is a gap, and the `pg` listeners here perform NO
// automatic reconnect. A dropped connection surfaces through
// `onDisconnect` (owned mode) and the listener-error event; catch up from
// your source of truth. A NOTIFY executed inside a transaction that rolls
// back is never delivered (notifications fire at commit).
//
// Nucleus divergence (verified live, see conformance/live/orm x05 leg):
// Nucleus delivers pending NotificationResponses around the connection's own
// statement traffic — an idle listener may not observe a notification until
// it runs another statement. `pollIntervalMs` makes the listener issue a
// no-op SELECT on that cadence to flush. Pure PostgreSQL does not need it.
//
// Cancellation (I02 contracts): an `AbortSignal` aborts the listener —
// unlisten, detach handlers, and (owned mode only) end the connection.
// Aborting never closes a borrowed client and never leaves an owned one open.

import { errorSummary } from "./logger.js";

/** A notification delivered on a listening connection. */
export interface PgNotification {
  /** Channel as rendered (double-quoted identifier content — case preserved). */
  channel: string;
  /** Payload string ('' when the sender gave none). */
  payload: string;
  /** PostgreSQL backend process id of the sending connection. */
  processId: number;
}

/** Lifecycle events for a listener. REDACTED: notification payloads are
 * never included in events. */
export type ListenEvent =
  | { kind: "listen-open"; channel: string }
  | { kind: "listen-close"; channel: string }
  | { kind: "notify-sent"; channel: string }
  | { kind: "listener-error"; error: { name: string; message: string } }
  | { kind: "listener-disconnected" }
  | { kind: "listener-closed" };

export type ListenLogger = (event: ListenEvent) => void;

const MAX_CHANNEL_BYTES = 63;
const MAX_PAYLOAD_BYTES = 8000;

/** Validate a channel name and return its double-quoted identifier form. */
function quotedChannel(channel: string): string {
  if (typeof channel !== "string" || channel.length === 0) {
    throw new Error("LISTEN/NOTIFY channel must be a non-empty string");
  }
  if (channel.includes("\0")) {
    throw new Error("LISTEN/NOTIFY channel must not contain NUL");
  }
  if (Buffer.byteLength(channel, "utf8") > MAX_CHANNEL_BYTES) {
    throw new Error(
      `LISTEN/NOTIFY channel exceeds PostgreSQL's ${MAX_CHANNEL_BYTES}-byte limit (${Buffer.byteLength(channel, "utf8")} bytes)`,
    );
  }
  return `"${channel.replace(/"/g, '""')}"`;
}

/** Render a NOTIFY statement for a validated channel and payload. */
export function notifyStatement(channel: string, payload?: string): string {
  const chan = quotedChannel(channel);
  if (payload === undefined || payload === "") {
    return `NOTIFY ${chan}`;
  }
  if (payload.includes("\0")) {
    throw new Error("NOTIFY payload must not contain NUL");
  }
  if (Buffer.byteLength(payload, "utf8") > MAX_PAYLOAD_BYTES) {
    throw new Error(
      `NOTIFY payload exceeds PostgreSQL's ${MAX_PAYLOAD_BYTES}-byte limit (${Buffer.byteLength(payload, "utf8")} bytes)`,
    );
  }
  return `NOTIFY ${chan}, '${payload.replace(/'/g, "''")}'`;
}

/** Minimal duck type for a dedicated `pg` client that can receive
 * notifications. This is a Client, not a pool checkout. */
export interface PgNotifyClient {
  query(sqlText: string): Promise<unknown>;
  on(event: "notification", listener: (n: { channel: string; payload?: string; processId: number }) => void): unknown;
  on(event: "error", listener: (err: Error) => void): unknown;
  on(event: "end", listener: () => void): unknown;
  removeListener(event: string, listener: (...args: never[]) => void): unknown;
  end(): Promise<unknown>;
}

/** Minimal duck type for a postgres.js handle with native LISTEN support. */
export interface PostgresJsListenClient {
  listen(channel: string, onNotify: (payload: string) => void, onListen?: () => void): Promise<{ unlisten: () => Promise<void> }>;
  unsafe(sqlText: string): Promise<unknown>;
}

export interface ListenerOptions {
  /** Structured, redacted lifecycle events. */
  logger?: ListenLogger;
  /** AbortSignal: aborting unlistens everything and closes the listener. */
  signal?: AbortSignal;
}

export interface PgListenerOptions extends ListenerOptions {
  /** OWNED mode: connection URL this listener connects with directly. */
  url?: string;
  /** BORROWED mode: an externally owned, dedicated `pg` client. Mutually
   * exclusive with `url`. */
  client?: PgNotifyClient;
  /** Nucleus flushes notifications around statement traffic; an idle poll
   * (a SELECT) drains pending NotificationResponses. 0 disables. Pure
   * PostgreSQL does not need this. */
  pollIntervalMs?: number;
}

/** A live listener handle. */
export interface ListenerHandle {
  /** The channels this handle is subscribed to (snapshot). */
  readonly channels: readonly string[];
  /** Send a NOTIFY on the listener's own connection (or the borrowed
   * postgres.js handle). */
  notify(channel: string, payload?: string): Promise<void>;
  /** Stop listening on the given channels (default: all of them). */
  unlisten(channels?: string | string[]): Promise<void>;
  /** Fully stop the listener: unlisten, detach, and (owned mode) end the
   * dedicated connection. Borrowed clients are left open for their owner. */
  close(): Promise<void>;
}

function normalizeChannels(channels: string | string[]): string[] {
  const list = Array.isArray(channels) ? channels : [channels];
  if (list.length === 0) throw new Error("listen requires at least one channel");
  const seen = new Set<string>();
  for (const c of list) {
    quotedChannel(c);
    if (seen.has(c)) throw new Error(`duplicate channel ${JSON.stringify(c)}`);
    seen.add(c);
  }
  return list;
}

/**
 * LISTEN/NOTIFY over the `pg` driver, on ONE dedicated connection.
 */
export function pgListener(options: PgListenerOptions): Promise<PgListenerHandle> {
  if ((options.url !== undefined) === (options.client !== undefined)) {
    return Promise.reject(
      new Error("pgListener takes exactly one of url (owned connection) or client (borrowed, dedicated)"),
    );
  }
  if (options.url !== undefined) {
    return PgOwnedListener.create(options as PgListenerOptions & { url: string });
  }
  return Promise.resolve(new PgBorrowedListener(options as { client: PgNotifyClient; logger?: ListenLogger; signal?: AbortSignal; pollIntervalMs?: number }));
}

export interface PgListenerHandle extends ListenerHandle {
  /** Subscribe to channels on the dedicated connection. The notification
   * handler is attached BEFORE the first LISTEN is sent, so nothing that
   * arrives between LISTEN and attach can be lost.
   * `onDisconnect` (owned mode) fires when the dedicated connection dies —
   * the listener does not reconnect; notifications emitted after the
   * disconnect are missed by design. */
  listen(
    channels: string | string[],
    onNotification: (n: PgNotification) => void,
    onDisconnect?: () => void,
  ): Promise<void>;
}

abstract class PgListenerBase implements PgListenerHandle {
  protected readonly log: ListenLogger | undefined;
  protected readonly subscribed = new Set<string>();
  protected closed = false;
  private readonly onAbort = () => {
    void this.close();
  };

  constructor(protected readonly opts: { logger?: ListenLogger; signal?: AbortSignal }) {
    this.log = opts.logger;
    opts.signal?.addEventListener("abort", this.onAbort, { once: true });
  }

  get channels(): readonly string[] {
    return [...this.subscribed];
  }

  async listen(
    channels: string | string[],
    onNotification: (n: PgNotification) => void,
    _onDisconnect?: () => void,
  ): Promise<void> {
    this.assertOpen();
    const list = normalizeChannels(channels);
    this.attach(onNotification);
    for (const c of list) {
      if (this.subscribed.has(c)) throw new Error(`already listening on channel ${JSON.stringify(c)}`);
      await this.send(`LISTEN ${quotedChannel(c)}`);
      this.subscribed.add(c);
      this.log?.({ kind: "listen-open", channel: c });
    }
  }

  async notify(channel: string, payload?: string): Promise<void> {
    this.assertOpen();
    await this.send(notifyStatement(channel, payload));
    this.log?.({ kind: "notify-sent", channel });
  }

  async unlisten(channels?: string | string[]): Promise<void> {
    this.assertOpen();
    const list = channels === undefined ? [...this.subscribed] : Array.isArray(channels) ? channels : [channels];
    if (channels !== undefined) {
      for (const c of list) {
        if (!this.subscribed.has(c)) {
          throw new Error(`not listening on channel ${JSON.stringify(c)}`);
        }
      }
    }
    for (const c of list) {
      await this.unlistenOne(c);
      this.subscribed.delete(c);
      this.log?.({ kind: "listen-close", channel: c });
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.opts.signal?.removeEventListener("abort", this.onAbort);
    await this.teardown();
    this.log?.({ kind: "listener-closed" });
  }

  protected assertOpen(): void {
    if (this.closed) throw new Error("listener is closed");
  }

  /** Nucleus keys channels by the raw statement text, so a channel this
   * listener subscribed as `"chan"` is REPORTED back with its quote
   * characters. PostgreSQL reports the bare name. Normalize a reported
   * channel back to the name we subscribed under so both engines surface
   * identical strings — owned AND borrowed listeners alike; anything else
   * passes through untouched. */
  protected normalize(n: { channel: string; payload?: string; processId: number }): PgNotification {
    let channel = n.channel;
    if (
      channel.startsWith('"') &&
      channel.endsWith('"') &&
      channel.length >= 2
    ) {
      const inner = channel.slice(1, -1).replace(/""/g, '"');
      if (this.subscribed.has(inner)) channel = inner;
    }
    return { channel, payload: n.payload ?? "", processId: n.processId };
  }

  protected abstract send(sqlText: string): Promise<unknown>;
  protected abstract unlistenOne(channel: string): Promise<unknown>;
  protected abstract attach(onNotification: (n: PgNotification) => void): void;
  protected abstract detach(): void;
  protected abstract teardown(): Promise<void>;
}

class PgOwnedListener extends PgListenerBase {
  private client: PgNotifyClient | null = null;
  private handler: ((n: { channel: string; payload?: string; processId: number }) => void) | null = null;
  private onErr: ((err: Error) => void) | null = null;
  private onEnd: (() => void) | null = null;
  private disconnectCb: (() => void) | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;

  private constructor(opts: PgListenerOptions & { url: string }) {
    super(opts);
    this.ownedOpts = opts;
  }
  private readonly ownedOpts: PgListenerOptions & { url: string };

  static async create(opts: PgListenerOptions & { url: string }): Promise<PgListenerHandle> {
    const self = new PgOwnedListener(opts);
    const specifier = "pg";
    const mod = (await import(specifier)) as unknown as {
      Client: new (o: { connectionString: string }) => PgNotifyClient & { connect(): Promise<void> };
    };
    const client = new mod.Client({ connectionString: opts.url });
    self.client = client;
    // A bare pg Client does NOT auto-connect — a query submitted before
    // connect() never settles. Connect explicitly so auth/reachability
    // failures reject the create() call.
    self.wireBasics();
    try {
      await client.connect();
      await client.query("SELECT 1");
    } catch (err) {
      self.log?.({ kind: "listener-error", error: errorSummary(err) });
      await self.close();
      throw err;
    }
    const ms = self.ownedOpts.pollIntervalMs ?? 0;
    if (ms > 0) {
      self.pollTimer = setInterval(() => {
        void self.client?.query("SELECT 1").catch(() => {});
      }, ms);
      self.pollTimer.unref?.();
    }
    return self;
  }

  private wireBasics(): void {
    const client = this.client;
    if (!client) return;
    this.onErr = (err) => this.log?.({ kind: "listener-error", error: errorSummary(err) });
    this.onEnd = () => {
      this.log?.({ kind: "listener-disconnected" });
      this.disconnectCb?.();
    };
    client.on("error", this.onErr);
    client.on("end", this.onEnd);
  }

  protected attach(onNotification: (n: PgNotification) => void): void {
    if (this.handler || !this.client) return;
    this.handler = (n) => onNotification(this.normalize(n));
    this.client.on("notification", this.handler);
  }

  protected detach(): void {
    const client = this.client;
    if (!client) return;
    if (this.handler) client.removeListener("notification", this.handler as (...args: never[]) => void);
    if (this.onErr) client.removeListener("error", this.onErr as (...args: never[]) => void);
    if (this.onEnd) client.removeListener("end", this.onEnd as (...args: never[]) => void);
    this.handler = null;
    this.onErr = null;
    this.onEnd = null;
  }

  async listen(
    channels: string | string[],
    onNotification: (n: PgNotification) => void,
    onDisconnect?: () => void,
  ): Promise<void> {
    this.disconnectCb = onDisconnect ?? null;
    await super.listen(channels, onNotification, onDisconnect);
  }

  protected async send(sqlText: string): Promise<unknown> {
    if (!this.client) throw new Error("listener connection is not up");
    return this.client.query(sqlText);
  }

  protected async unlistenOne(channel: string): Promise<unknown> {
    return this.send(`UNLISTEN ${quotedChannel(channel)}`);
  }

  protected async teardown(): Promise<void> {
    if (this.pollTimer) clearInterval(this.pollTimer);
    this.pollTimer = null;
    // Detach BEFORE nulling the client: teardown used to null first, so
    // detach() saw no client and removed nothing — the subsequent
    // client.end() then fired the still-attached end-handler, logging
    // listener-disconnected and invoking onDisconnect on a DELIBERATE close.
    this.detach();
    const client = this.client;
    this.client = null;
    if (client) {
      try {
        await client.query("UNLISTEN *");
      } catch {
        // The connection may already be gone; end() below is the cleanup.
      }
      await client.end();
    }
  }
}

class PgBorrowedListener extends PgListenerBase {
  private handler: ((n: { channel: string; payload?: string; processId: number }) => void) | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private readonly borrowedClient: PgNotifyClient;

  constructor(opts: { client: PgNotifyClient; logger?: ListenLogger; signal?: AbortSignal; pollIntervalMs?: number }) {
    super(opts);
    this.borrowedClient = opts.client;
    const ms = opts.pollIntervalMs ?? 0;
    if (ms > 0) {
      this.pollTimer = setInterval(() => {
        void this.borrowedClient.query("SELECT 1").catch(() => {});
      }, ms);
      this.pollTimer.unref?.();
    }
  }

  protected attach(onNotification: (n: PgNotification) => void): void {
    if (this.handler) return;
    this.handler = (n) => onNotification(this.normalize(n));
    this.borrowedClient.on("notification", this.handler);
  }

  protected detach(): void {
    if (this.handler) {
      this.borrowedClient.removeListener("notification", this.handler as (...args: never[]) => void);
      this.handler = null;
    }
  }

  protected async send(sqlText: string): Promise<unknown> {
    return this.borrowedClient.query(sqlText);
  }

  protected async unlistenOne(channel: string): Promise<unknown> {
    return this.send(`UNLISTEN ${quotedChannel(channel)}`);
  }

  protected async teardown(): Promise<void> {
    if (this.pollTimer) clearInterval(this.pollTimer);
    this.pollTimer = null;
    this.detach();
    // Borrowed: unlisten ONLY the channels we subscribed — the client stays
    // open for its owner and may carry other listeners' subscriptions.
    for (const c of this.subscribed) {
      try {
        await this.borrowedClient.query(`UNLISTEN ${quotedChannel(c)}`);
      } catch {
        // Owner's connection may be gone — nothing to unlisten.
      }
    }
  }
}

export interface PostgresJsListenerOptions extends ListenerOptions {
  /** A postgres.js handle (`postgres(url)`). Its native `listen()` manages a
   * dedicated subscription connection internally. */
  client: PostgresJsListenClient;
}

export interface PostgresJsListenerHandle extends ListenerHandle {
  /** Subscribe via postgres.js's native LISTEN. The payload callback also
   * receives the channel. */
  listen(channels: string | string[], onNotification: (payload: string, channel: string) => void): Promise<void>;
}

/** Listener over a borrowed postgres.js handle using its native LISTEN
 * support. The handle is never ended by this listener. */
export function postgresJsListener(options: PostgresJsListenerOptions): Promise<PostgresJsListenerHandle> {
  return Promise.resolve(new PostgresJsListener(options));
}

class PostgresJsListener implements PostgresJsListenerHandle {
  private readonly log: ListenLogger | undefined;
  private readonly unlistens = new Map<string, { unlisten: () => Promise<void> }>();
  private closed = false;
  private readonly onAbort = () => {
    void this.close();
  };

  constructor(private readonly opts: PostgresJsListenerOptions) {
    this.log = opts.logger;
    opts.signal?.addEventListener("abort", this.onAbort, { once: true });
  }

  get channels(): readonly string[] {
    return [...this.unlistens.keys()];
  }

  async listen(channels: string | string[], onNotification: (payload: string, channel: string) => void): Promise<void> {
    this.assertOpen();
    for (const channel of normalizeChannels(channels)) {
      if (this.unlistens.has(channel)) continue;
      // postgres.js owns the quoting convention (double-quoted identifier),
      // matching ours, so its channel key lines up with notifyStatement().
      const handle = await this.opts.client.listen(channel, (payload) => onNotification(payload, channel));
      this.unlistens.set(channel, handle);
      this.log?.({ kind: "listen-open", channel });
    }
  }

  async notify(channel: string, payload?: string): Promise<void> {
    this.assertOpen();
    await this.opts.client.unsafe(notifyStatement(channel, payload));
    this.log?.({ kind: "notify-sent", channel });
  }

  async unlisten(channels?: string | string[]): Promise<void> {
    this.assertOpen();
    const list = channels === undefined ? [...this.unlistens.keys()] : Array.isArray(channels) ? channels : [channels];
    for (const c of list) {
      if (!this.unlistens.has(c)) throw new Error(`not listening on channel ${JSON.stringify(c)}`);
    }
    for (const c of list) {
      const handle = this.unlistens.get(c);
      this.unlistens.delete(c);
      await handle?.unlisten();
      this.log?.({ kind: "listen-close", channel: c });
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.opts.signal?.removeEventListener("abort", this.onAbort);
    for (const [c, handle] of [...this.unlistens]) {
      this.unlistens.delete(c);
      await handle.unlisten().catch(() => {
        // Subscription connection may already be gone.
      });
    }
    this.log?.({ kind: "listener-closed" });
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("listener is closed");
  }
}
