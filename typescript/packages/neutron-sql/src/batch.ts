// ---------------------------------------------------------------------------
// @neutron-build/sql — explicit batch query plans (Q08)
// ---------------------------------------------------------------------------
// `db.batch([q1, q2, …], options?)` runs a FIXED, fully compiled list of
// statements sequentially on ONE connection inside ONE transaction and
// resolves to a typed tuple of their results. The plan is explicit:
// `explain()` lists every statement (SQL, parameters, result shape, decode
// plan, capabilities) plus the transaction envelope before anything runs,
// and execution issues exactly those statements — never a hidden extra
// query, never a multi-statement string (each statement is its own ordinary
// driver call).
//
// Snapshot: a batch that owns its transaction defaults to REPEATABLE READ,
// so every statement reads the same snapshot (READ COMMITTED would take a
// new snapshot per statement). Pass `isolation` to choose otherwise. Writes
// under REPEATABLE READ can fail with 40001 when they race a concurrent
// update — that surfaces; retry is opt-in via `retry` (whole batch, requires
// idempotent: true, never after an ambiguous commit — the I02 contract).
//
// Inside `db.transaction` the batch runs on the transaction's connection and
// takes no modes/retry: the enclosing callback owns BEGIN/COMMIT.

import { NeutronSqlError } from "./errors.js";
import { applyProjectionDecoders, type StatementCapability } from "./codecs.js";
import {
  hasModes,
  renderBeginSql,
  runRetriedTransaction,
  runTransaction,
  transactionScopeState,
  validateRetryOptions,
  type QueryExecutionOptions,
  type TransactionModes,
  type TransactionRetryOptions,
  type TransactionScope,
} from "./transactions.js";
import { run, type CompiledStatement, type ExecContext } from "./builder.js";

/** A builder a batch can carry: compiled purely, with a known result shape
 *  (select/set-operation rows; insert/update/delete rows with returning,
 *  affected-row count without). */
export interface Batchable<R = unknown> extends PromiseLike<R> {
  toCompiled(): CompiledStatement;
  batchResultKind(): "rows" | "count";
}

/** Result tuple of a batch: each position is what awaiting that builder
 *  alone would produce. */
export type BatchResults<Q extends readonly Batchable[]> = { -readonly [K in keyof Q]: Awaited<Q[K]> };

export interface BatchOptions extends QueryExecutionOptions, TransactionModes {
  /** Opt-in whole-batch retry on 40001/40P01 (I02 semantics). */
  retry?: TransactionRetryOptions;
}

export interface BatchStatementPlan {
  readonly index: number;
  readonly sql: string;
  readonly params: readonly unknown[];
  readonly result: "rows" | "count";
  readonly capabilities: readonly StatementCapability[];
  readonly decoders: ReadonlyArray<{ readonly key: string; readonly wire: "text" | "native" }>;
}

export interface BatchPlan {
  readonly strategy: "sequential-one-connection";
  readonly transaction: {
    /** "own": BEGIN/COMMIT issued by the batch; "enclosing": the caller's
     *  db.transaction. */
    readonly ownership: "own" | "enclosing";
    /** The BEGIN statement ("own" only). */
    readonly begin?: string;
    readonly retry: boolean;
  };
  /** Exactly the statements the batch executes, in order (BEGIN/COMMIT
   *  excluded). */
  readonly statementCount: number;
  readonly statements: readonly BatchStatementPlan[];
  readonly capabilities: readonly StatementCapability[];
}

const BATCH_OPTION_KEYS: ReadonlySet<string> = new Set(["deadlineMs", "signal", "isolation", "readOnly", "deferrable", "retry"]);

interface CompiledItem {
  readonly compiled: CompiledStatement;
  readonly result: "rows" | "count";
}

function isBatchable(v: unknown): v is Batchable {
  return (
    typeof v === "object" &&
    v !== null &&
    typeof (v as { toCompiled?: unknown }).toCompiled === "function" &&
    typeof (v as { batchResultKind?: unknown }).batchResultKind === "function"
  );
}

export class BatchQuery<T extends readonly unknown[]> implements PromiseLike<T> {
  readonly #ctx: ExecContext;
  readonly #items: readonly Batchable[];
  readonly #options: BatchOptions;

  /** Internal — obtain batches through `db.batch()` / `tx.batch()`. */
  constructor(ctx: ExecContext, items: readonly Batchable[], options: BatchOptions = {}) {
    if (!Array.isArray(items) || items.length === 0) {
      throw new NeutronSqlError("batch: pass a non-empty array of query builders");
    }
    items.forEach((item, i) => {
      if (!isBatchable(item)) {
        throw new NeutronSqlError(
          `batch: item ${i} is not a batchable query builder (select, set operation, insert, update or delete) — relational queries and raw SQL run as their own statements`,
        );
      }
    });
    if (typeof options !== "object" || options === null) throw new NeutronSqlError("batch: options must be an object");
    for (const key of Object.keys(options)) {
      if (!BATCH_OPTION_KEYS.has(key)) throw new NeutronSqlError(`batch: unknown option "${key}" (known: ${[...BATCH_OPTION_KEYS].join(", ")})`);
    }
    this.#ctx = ctx;
    this.#items = Object.freeze([...items]);
    this.#options = Object.freeze({ ...options });
    Object.freeze(this);
  }

  #ownership(): "own" | "enclosing" {
    const state = transactionScopeState(this.#ctx.driver);
    if (state === "settled") {
      throw new NeutronSqlError("batch: this batch belongs to a transaction scope that has already settled — run it inside the transaction callback or on the database itself");
    }
    return state === "active" ? "enclosing" : "own";
  }

  /** The modes of an owned batch: REPEATABLE READ unless chosen. */
  #modes(): TransactionModes {
    const o = this.#options;
    const modes: TransactionModes = { isolation: o.isolation ?? "repeatable-read" };
    if (o.readOnly !== undefined) modes.readOnly = o.readOnly;
    if (o.deferrable !== undefined) modes.deferrable = o.deferrable;
    return modes;
  }

  /** Validate the envelope and compile every statement. Pure; any failure
   *  here happens before a connection is touched. */
  #prepare(): { ownership: "own" | "enclosing"; items: CompiledItem[]; capabilities: StatementCapability[] } {
    const ownership = this.#ownership();
    const o = this.#options;
    if (ownership === "enclosing") {
      if (hasModes({ isolation: o.isolation, readOnly: o.readOnly, deferrable: o.deferrable }) || o.retry !== undefined) {
        throw new NeutronSqlError("batch: isolation/read-only/deferrable/retry belong to the enclosing db.transaction — a batch inside a transaction takes none");
      }
    } else {
      if (typeof this.#ctx.driver.pin !== "function") {
        throw new NeutronSqlError(
          "batch: requires a pinnable adapter (Driver.pin) — a batch runs its statements on one connection in one transaction; both bundled drivers provide it, this custom adapter does not",
        );
      }
      renderBeginSql(this.#modes());
      if (o.retry !== undefined) validateRetryOptions(o.retry);
    }
    if (o.deadlineMs !== undefined && (!Number.isFinite(o.deadlineMs) || o.deadlineMs <= 0)) {
      throw new NeutronSqlError(`deadlineMs must be a positive finite number of milliseconds (got ${String(o.deadlineMs)})`);
    }
    const items = this.#items.map((item) => ({ compiled: item.toCompiled(), result: item.batchResultKind() }));
    const capabilities = [...new Set(items.flatMap((i) => i.compiled.capabilities))];
    return { ownership, items, capabilities };
  }

  /** Pure description of the plan — nothing executes. */
  explain(): BatchPlan {
    const { ownership, items, capabilities } = this.#prepare();
    return {
      strategy: "sequential-one-connection",
      transaction: {
        ownership,
        ...(ownership === "own" ? { begin: renderBeginSql(this.#modes()) } : {}),
        retry: this.#options.retry !== undefined,
      },
      statementCount: items.length,
      statements: items.map((item, index) => ({
        index,
        sql: item.compiled.sql,
        params: [...item.compiled.params],
        result: item.result,
        capabilities: item.compiled.capabilities,
        decoders: item.compiled.decoders.map((d) => ({ key: d.key, wire: d.wire })),
      })),
      capabilities,
    };
  }

  async execute(): Promise<T> {
    const { ownership, items, capabilities } = this.#prepare();
    if (this.#ctx.capabilities && capabilities.length > 0) await this.#ctx.capabilities.assert(capabilities);
    const o = this.#options;
    const exec: QueryExecutionOptions | undefined =
      o.deadlineMs === undefined && o.signal === undefined ? undefined : { deadlineMs: o.deadlineMs, signal: o.signal };
    const logger = this.#ctx.logger;

    const runAll = async (driver: ExecContext["driver"]): Promise<unknown[]> => {
      const sctx: ExecContext = { driver, logger };
      const results: unknown[] = [];
      for (const item of items) {
        const { sql, params, decoders } = item.compiled;
        if (item.result === "rows") {
          const rows = (await run(sctx, sql, [...params], "query", [], exec)) as Array<Record<string, unknown>>;
          applyProjectionDecoders(rows, decoders);
          results.push(rows);
        } else {
          results.push(await run(sctx, sql, [...params], "execute", [], exec));
        }
      }
      return results;
    };

    if (ownership === "enclosing") return (await runAll(this.#ctx.driver)) as unknown as T;

    const driver = this.#ctx.driver;
    const modes = this.#modes();
    const hooks = { onEvent: (event: Parameters<NonNullable<ExecContext["logger"]>>[0]) => logger?.(event) };
    const fn = (scope: TransactionScope): Promise<unknown[]> => runAll(scope);
    if (o.retry !== undefined) {
      return (await runRetriedTransaction(() => driver.pin!(), fn, modes, o.retry, hooks)) as unknown as T;
    }
    return (await runTransaction(await driver.pin!(), fn, modes, hooks)) as unknown as T;
  }

  then<R1 = T, R2 = never>(
    onfulfilled?: ((value: T) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    return this.execute().then(onfulfilled, onrejected);
  }
}
