// ---------------------------------------------------------------------------
// @neutron-build/sql — bounded streaming over server-side cursors (Q08)
// ---------------------------------------------------------------------------
// `builder.stream()` / `builder.streamBatches()` consume a select through a
// PostgreSQL server-side cursor: `DECLARE "c" NO SCROLL CURSOR FOR <select>`,
// then `FETCH FORWARD <batchSize>` only when the consumer pulls, then
// `CLOSE`. At most ONE batch is buffered client-side, whatever the result
// size. Every statement is sent on its own — no multi-statement strings —
// so both drivers run it through their ordinary (extended-protocol when
// parameterized) query path. No driver-specific cursor module is needed.
//
// Transaction ownership (I02 preserved):
//   - Outside a transaction the stream OWNS one: it pins a connection and
//     drives the shared runner (runTransaction) — BEGIN with the validated
//     modes, DECLARE/FETCH…, CLOSE, COMMIT when the cursor is exhausted.
//     Early exit (break / return() / throw()) ROLLS BACK and releases the
//     connection immediately; nothing is left open for the pool.
//   - Inside `db.transaction` the stream runs on the transaction's pinned
//     connection and never issues BEGIN/COMMIT/ROLLBACK: the enclosing
//     callback owns the transaction. Early exit CLOSEs the cursor. Using the
//     stream after that transaction settled is rejected IMMEDIATELY — rows
//     still sitting in the client-side buffer are dropped, and the released
//     connection is never touched.
//
// Cancellation: `signal` / `deadlineMs` apply to every DECLARE/FETCH round
// trip exactly like QueryExecutionOptions (server-side cancel, SQLSTATE
// 57014). Aborting the signal while the consumer is idle also rolls back an
// owned transaction and releases its connection without waiting for the
// next pull.
//
// Pool usage: an owned stream holds one pooled connection (idle in
// transaction) from its first pull until it is exhausted or closed. Always
// consume with for-await or call return(); N concurrent owned streams need
// N free pooled connections.

import { NeutronSqlError, QueryCanceledError } from "./errors.js";
import { applyProjectionDecoders, type StatementCapability } from "./codecs.js";
import type { Driver } from "./drivers.js";
import type { Logger } from "./logger.js";
import {
  hasModes,
  renderBeginSql,
  runTransaction,
  transactionScopeState,
  type QueryExecutionOptions,
  type TransactionModes,
} from "./transactions.js";
import type { CompiledStatement, ExecContext } from "./builder.js";

export const DEFAULT_STREAM_BATCH_SIZE = 100;
export const MAX_STREAM_BATCH_SIZE = 10_000;

/** Stream options: batch size, per-round-trip cancellation, and (for streams
 *  that own their transaction) the transaction modes of its BEGIN. */
export interface StreamOptions extends QueryExecutionOptions, TransactionModes {
  /** Rows per FETCH (1..10000, default 100). Bounds client-side buffering. */
  batchSize?: number;
}

/** Structured, non-executing description of a stream's statement plan. */
export interface StreamPlan {
  readonly strategy: "server-cursor";
  /** "own": the stream runs its own transaction on a pinned connection.
   *  "enclosing": it runs inside the caller's db.transaction scope. */
  readonly transaction: "own" | "enclosing";
  readonly batchSize: number;
  readonly cursorName: string;
  readonly statements: ReadonlyArray<{
    readonly role: "begin" | "declare" | "fetch" | "close" | "commit";
    readonly sql: string;
    readonly params: readonly unknown[];
    /** fetch repeats until a batch returns fewer than batchSize rows. */
    readonly repeats?: true;
  }>;
  /** What early exit (break/return/throw/abort) does. */
  readonly earlyExit: "rollback" | "close";
  readonly capabilities: readonly StatementCapability[];
  readonly decoders: ReadonlyArray<{ readonly key: string; readonly wire: "text" | "native" }>;
}

/** Statement executor used for every cursor statement (the builders' `run`,
 *  so events/logging/cancel classification match ordinary execution). */
export type StreamStatementRunner = (
  ctx: ExecContext,
  sqlText: string,
  params: unknown[],
  kind: "query" | "execute",
  required?: readonly StatementCapability[],
  options?: QueryExecutionOptions,
) => Promise<unknown>;

type Row = Record<string, unknown>;

/** Thrown inside an owned stream's transaction to roll it back on early
 *  exit; never surfaces to the consumer. */
export class StreamClosedEarly extends NeutronSqlError {
  constructor() {
    super("stream closed before its cursor was exhausted (early iterator exit or abort) — transaction rolled back");
  }
}

interface CursorSession {
  /** Next non-empty batch, or null once exhausted (and, for owned streams,
   *  committed). */
  fetch(): Promise<Row[] | null>;
  /** Early exit: roll back (owned) or close the cursor (enclosing).
   *  Idempotent. */
  stop(): Promise<void>;
}

interface SessionParams {
  readonly run: StreamStatementRunner;
  readonly logger: Logger | null;
  readonly declareSql: string;
  readonly fetchSql: string;
  readonly closeSql: string;
  readonly params: unknown[];
  readonly batchSize: number;
  readonly exec: QueryExecutionOptions | undefined;
}

function ownSession(driver: Driver, modes: TransactionModes, p: SessionParams): CursorSession {
  let started = false;
  let outcome: Promise<{ ok: true } | { ok: false; err: unknown }> | null = null;
  let commandWaiter: ((cmd: "fetch" | "stop") => void) | null = null;
  let queued: "fetch" | "stop" | null = null;
  let delivery: ((rows: Row[]) => void) | null = null;
  let finalRows: Row[] = [];
  let finished = false;

  const nextCommand = (): Promise<"fetch" | "stop"> =>
    new Promise((resolve) => {
      if (queued !== null) {
        const cmd = queued;
        queued = null;
        resolve(cmd);
      } else {
        commandWaiter = resolve;
      }
    });
  const send = (cmd: "fetch" | "stop"): void => {
    if (commandWaiter !== null) {
      const waiter = commandWaiter;
      commandWaiter = null;
      waiter(cmd);
    } else {
      queued = cmd;
    }
  };

  const start = async (): Promise<void> => {
    started = true;
    const pin = await driver.pin!();
    // The shared I02 runner owns BEGIN/COMMIT/ROLLBACK, commit-ambiguity
    // classification and the pin's single release. The callback stays
    // pending between pulls; a "stop" command throws StreamClosedEarly so
    // the runner rolls back and releases the connection.
    const tx = runTransaction(
      pin,
      async (scope) => {
        const sctx: ExecContext = { driver: scope, logger: p.logger };
        await p.run(sctx, p.declareSql, p.params, "execute", [], p.exec);
        for (;;) {
          const cmd = await nextCommand();
          if (cmd === "stop") throw new StreamClosedEarly();
          const rows = (await p.run(sctx, p.fetchSql, [], "query", [], p.exec)) as Row[];
          if (rows.length < p.batchSize) {
            await p.run(sctx, p.closeSql, [], "execute");
            finalRows = rows;
            return;
          }
          delivery!(rows);
        }
      },
      modes,
      { onEvent: (event) => p.logger?.(event) },
    );
    outcome = tx.then(
      () => ({ ok: true as const }),
      (err: unknown) => ({ ok: false as const, err }),
    );
  };

  return {
    async fetch(): Promise<Row[] | null> {
      if (finished) return null;
      if (!started) {
        try {
          await start();
        } catch (err) {
          finished = true;
          throw err;
        }
      }
      const delivered = new Promise<Row[]>((resolve) => {
        delivery = resolve;
      });
      send("fetch");
      const settled = await Promise.race([
        delivered.then((rows) => ({ kind: "batch" as const, rows })),
        outcome!.then((o) => ({ kind: "settled" as const, o })),
      ]);
      if (settled.kind === "batch") return settled.rows;
      // The transaction settled: the final (short) batch is released only
      // after COMMIT succeeded, so a commit failure is never hidden behind
      // rows the consumer already saw as final.
      finished = true;
      if (!settled.o.ok) throw settled.o.err;
      const rows = finalRows;
      finalRows = [];
      return rows.length === 0 ? null : rows;
    },
    async stop(): Promise<void> {
      if (finished || !started) {
        finished = true;
        return;
      }
      finished = true;
      send("stop");
      const o = await outcome!;
      if (!o.ok && !(o.err instanceof StreamClosedEarly)) throw o.err;
    },
  };
}

function enclosingSettledError(): NeutronSqlError {
  return new NeutronSqlError(
    "stream: its enclosing transaction has settled — a stream opened inside db.transaction must be consumed or closed before the callback returns (the transaction's connection is no longer this stream's)",
  );
}

function enclosingSession(scope: Driver, p: SessionParams): CursorSession {
  let opened = false;
  let finished = false;
  const sctx: ExecContext = { driver: scope, logger: p.logger };
  const guard = (): void => {
    if (transactionScopeState(scope) !== "active") throw enclosingSettledError();
  };
  return {
    async fetch(): Promise<Row[] | null> {
      if (finished) return null;
      guard();
      try {
        if (!opened) {
          await p.run(sctx, p.declareSql, p.params, "execute", [], p.exec);
          opened = true;
        }
        const rows = (await p.run(sctx, p.fetchSql, [], "query", [], p.exec)) as Row[];
        if (rows.length < p.batchSize) {
          finished = true;
          await p.run(sctx, p.closeSql, [], "execute");
          return rows.length === 0 ? null : rows;
        }
        return rows;
      } catch (err) {
        finished = true;
        throw err;
      }
    },
    async stop(): Promise<void> {
      if (finished || !opened) {
        finished = true;
        return;
      }
      finished = true;
      // A settled transaction already closed the cursor with itself.
      if (transactionScopeState(scope) !== "active") return;
      await p.run(sctx, p.closeSql, [], "execute");
    },
  };
}

const STREAM_OPTION_KEYS: ReadonlySet<string> = new Set(["batchSize", "deadlineMs", "signal", "isolation", "readOnly", "deferrable"]);

let cursorSequence = 0;

/** A bounded, pull-driven row (or batch) stream over a server-side cursor.
 *  Single-consumer: concurrent next() calls on ONE stream are serialized;
 *  independent consumers each call `stream()` and get their own cursor. */
export class CursorStream<T> implements AsyncIterableIterator<T> {
  readonly #ctx: ExecContext;
  readonly #compiled: CompiledStatement;
  readonly #mode: "rows" | "batches";
  readonly #run: StreamStatementRunner;
  readonly #batchSize: number;
  readonly #modes: TransactionModes;
  readonly #exec: QueryExecutionOptions | undefined;
  readonly #signal: AbortSignal | undefined;
  readonly #ownership: "own" | "enclosing";
  readonly #cursorName: string;
  #session: CursorSession | null = null;
  #buffer: Row[] = [];
  #index = 0;
  #done = false;
  #aborted = false;
  #chain: Promise<unknown> = Promise.resolve();
  #onAbort: (() => void) | null = null;

  /** Internal — obtain streams through a builder's `stream()` /
   *  `streamBatches()`. Validates everything before any connection is
   *  touched: options, transaction ownership and adapter support. */
  constructor(ctx: ExecContext, compiled: CompiledStatement, options: StreamOptions | undefined, mode: "rows" | "batches", run: StreamStatementRunner) {
    const opts = options ?? {};
    if (typeof opts !== "object" || opts === null) throw new NeutronSqlError("stream: options must be an object");
    for (const key of Object.keys(opts)) {
      if (!STREAM_OPTION_KEYS.has(key)) {
        throw new NeutronSqlError(`stream: unknown option "${key}" (known: ${[...STREAM_OPTION_KEYS].join(", ")})`);
      }
    }
    const batchSize = opts.batchSize ?? DEFAULT_STREAM_BATCH_SIZE;
    if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > MAX_STREAM_BATCH_SIZE) {
      throw new NeutronSqlError(`stream: batchSize must be an integer between 1 and ${MAX_STREAM_BATCH_SIZE} (got ${String(opts.batchSize)})`);
    }
    if (opts.deadlineMs !== undefined && (!Number.isFinite(opts.deadlineMs) || opts.deadlineMs <= 0)) {
      throw new NeutronSqlError(`deadlineMs must be a positive finite number of milliseconds (got ${String(opts.deadlineMs)})`);
    }
    const modes: TransactionModes = { isolation: opts.isolation, readOnly: opts.readOnly, deferrable: opts.deferrable };
    for (const k of Object.keys(modes) as Array<keyof TransactionModes>) if (modes[k] === undefined) delete modes[k];

    const state = transactionScopeState(ctx.driver);
    if (state === "settled") {
      throw new NeutronSqlError("stream: this query belongs to a transaction scope that has already settled — open the stream inside the transaction callback or on the database itself");
    }
    if (state === "active") {
      if (hasModes(modes)) {
        throw new NeutronSqlError("stream: isolation/read-only/deferrable belong to the enclosing transaction's BEGIN — a stream inside db.transaction takes no transaction modes");
      }
      this.#ownership = "enclosing";
    } else {
      if (typeof ctx.driver.pin !== "function") {
        throw new NeutronSqlError(
          "stream: requires a pinnable adapter (Driver.pin) — a cursor lives in one transaction on one connection; both bundled drivers provide it, this custom adapter does not",
        );
      }
      renderBeginSql(modes);
      this.#ownership = "own";
    }

    this.#ctx = ctx;
    this.#compiled = compiled;
    this.#mode = mode;
    this.#run = run;
    this.#batchSize = batchSize;
    this.#modes = modes;
    this.#signal = opts.signal;
    this.#exec = opts.deadlineMs === undefined && opts.signal === undefined ? undefined : { deadlineMs: opts.deadlineMs, signal: opts.signal };
    this.#cursorName = `neutron_cursor_${++cursorSequence}`;
    if (opts.signal !== undefined && !opts.signal.aborted) {
      this.#onAbort = () => {
        void this.#serialize(() => this.#abortCleanup()).catch(() => {
          // the next pull reports the cancellation
        });
      };
      opts.signal.addEventListener("abort", this.#onAbort, { once: true });
    }
  }

  #required(): StatementCapability[] {
    return [...new Set<StatementCapability>([...this.#compiled.capabilities, "server-cursors"])];
  }

  #sql(): { declareSql: string; fetchSql: string; closeSql: string } {
    const name = `"${this.#cursorName}"`;
    return {
      declareSql: `declare ${name} no scroll cursor for ${this.#compiled.sql}`,
      fetchSql: `fetch forward ${this.#batchSize} from ${name}`,
      closeSql: `close ${name}`,
    };
  }

  /** Pure description of the statement plan — nothing executes. */
  explain(): StreamPlan {
    const { declareSql, fetchSql, closeSql } = this.#sql();
    const own = this.#ownership === "own";
    const statements: Array<StreamPlan["statements"][number]> = [];
    if (own) statements.push({ role: "begin", sql: renderBeginSql(this.#modes), params: [] });
    statements.push({ role: "declare", sql: declareSql, params: [...this.#compiled.params] });
    statements.push({ role: "fetch", sql: fetchSql, params: [], repeats: true });
    statements.push({ role: "close", sql: closeSql, params: [] });
    if (own) statements.push({ role: "commit", sql: "commit", params: [] });
    return {
      strategy: "server-cursor",
      transaction: this.#ownership,
      batchSize: this.#batchSize,
      cursorName: this.#cursorName,
      statements,
      earlyExit: own ? "rollback" : "close",
      capabilities: this.#required(),
      decoders: this.#compiled.decoders.map((d) => ({ key: d.key, wire: d.wire })),
    };
  }

  [Symbol.asyncIterator](): this {
    return this;
  }

  next(): Promise<IteratorResult<T>> {
    return this.#serialize(() => this.#next());
  }

  return(value?: unknown): Promise<IteratorResult<T>> {
    return this.#serialize(async () => {
      await this.#stop();
      return { done: true, value } as IteratorResult<T>;
    });
  }

  throw(err?: unknown): Promise<IteratorResult<T>> {
    return this.#serialize(async () => {
      await this.#stop();
      throw err;
    });
  }

  #serialize<R>(fn: () => Promise<R>): Promise<R> {
    const run = this.#chain.catch(() => {}).then(fn);
    this.#chain = run;
    return run;
  }

  #cancelError(): QueryCanceledError {
    return new QueryCanceledError("stream: canceled (signal) — the cursor was closed and no further rows are read", { reason: "signal", dispatched: false });
  }

  async #next(): Promise<IteratorResult<T>> {
    if (this.#signal?.aborted === true && !this.#done) {
      await this.#stop().catch(() => {});
      this.#aborted = true;
    }
    if (this.#aborted) throw this.#cancelError();
    if (this.#ownership === "enclosing" && !this.#done && transactionScopeState(this.#ctx.driver) !== "active") {
      // Post-settle use is rejected BEFORE serving buffered rows (Q08 review
      // LOW-3): a partially consumed batch is dropped, not drained, and the
      // released connection is never touched.
      this.#finish();
      throw enclosingSettledError();
    }
    if (this.#mode === "rows" && this.#index < this.#buffer.length) {
      const row = this.#buffer[this.#index];
      this.#index += 1;
      return { done: false, value: row as T };
    }
    if (this.#done) return { done: true, value: undefined } as IteratorResult<T>;
    const batch = await this.#fetchBatch();
    if (batch === null) {
      this.#finish();
      return { done: true, value: undefined } as IteratorResult<T>;
    }
    if (this.#mode === "batches") return { done: false, value: batch as T };
    this.#buffer = batch;
    this.#index = 1;
    return { done: false, value: batch[0] as T };
  }

  async #fetchBatch(): Promise<Row[] | null> {
    try {
      if (this.#session === null) {
        // Capabilities resolve BEFORE a connection is pinned: an engine that
        // cannot prove cursors (or the query's own requirements) fails here.
        if (this.#ctx.capabilities) await this.#ctx.capabilities.assert(this.#required());
        const { declareSql, fetchSql, closeSql } = this.#sql();
        const params: SessionParams = {
          run: this.#run,
          logger: this.#ctx.logger,
          declareSql,
          fetchSql,
          closeSql,
          params: [...this.#compiled.params],
          batchSize: this.#batchSize,
          exec: this.#exec,
        };
        this.#session = this.#ownership === "own" ? ownSession(this.#ctx.driver, this.#modes, params) : enclosingSession(this.#ctx.driver, params);
      }
      const rows = await this.#session.fetch();
      if (rows !== null) applyProjectionDecoders(rows, this.#compiled.decoders);
      return rows;
    } catch (err) {
      this.#finish();
      throw err;
    }
  }

  #finish(): void {
    this.#done = true;
    this.#buffer = [];
    this.#index = 0;
    if (this.#onAbort !== null) {
      this.#signal?.removeEventListener("abort", this.#onAbort);
      this.#onAbort = null;
    }
  }

  async #stop(): Promise<void> {
    const session = this.#session;
    const wasDone = this.#done;
    this.#finish();
    if (!wasDone && session !== null) await session.stop();
  }

  async #abortCleanup(): Promise<void> {
    if (this.#done) return;
    this.#aborted = true;
    await this.#stop();
  }
}
