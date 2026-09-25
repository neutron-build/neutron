// ---------------------------------------------------------------------------
// @neutron-build/nucleus/datalog — Datalog reasoning model plugin
// ---------------------------------------------------------------------------

import type { Transport, NucleusPlugin, NucleusFeatures } from '../types.js';
import { requireNucleus } from '../helpers.js';

// ---------------------------------------------------------------------------
// DatalogModel interface
// ---------------------------------------------------------------------------

/**
 * Semantics (Nucleus 1.0.x, verified live — see conformance/live/orm x05
 * leg):
 *
 * - The engine's mutators return STATUS STRINGS (`"ASSERT parent/2"`,
 *   `"RULE ancestor/2"`, `"RETRACT parent/2"`, `"CLEAR parent"`,
 *   `"IMPORTED 3 edges into edge"`), not booleans or counts. This client
 *   types them honestly as strings.
 * - A rule is ONE string in the engine (`'ancestor(X, Z) :- parent(X, Y),
 *   ancestor(Y, Z)'`); `rule(head, body)` renders that single-argument form.
 * - Evaluation is bounded: a semi-naive bottom-up fixpoint with a hard
 *   10,000-iteration cap per evaluation. If a program needs more iterations,
 *   the evaluator STOPS WITHOUT AN ERROR and answers from the partial
 *   fixpoint — deep recursion silently truncates. The cap bounds ITERATIONS,
 *   not time: near-cap programs are quadratic in practice and a
 *   10,250-link-chain query ran for minutes on the reference build. Keep
 *   rule chains short; a linear chain of N links needs N iterations.
 * - Stratified negation is supported; unsafe negation (a negated literal
 *   with an unbound variable) and unstratifiable programs are rejected.
 * - Facts and rules are durable: every mutator appends to the datalog WAL
 *   (failing the statement if the append fails) and startup replays it,
 *   filtered to committed transactions. Verified across a restart.
 * - `DATALOG_QUERY` takes a server-side WRITE lock: queries serialise
 *   against each other and against mutations. Long queries block writers.
 * - `DATALOG_` is denied under row-level security.
 */
export interface DatalogModel {
  /** Add a fact (e.g. `parent(alice, bob)`). Returns the engine status string. */
  assert(fact: string): Promise<string>;

  /** Remove a fact. Returns the engine status string. */
  retract(fact: string): Promise<string>;

  /**
   * Define a rule from its head and body (`rule("ancestor(X, Y)", "parent(X, Y)")`).
   * Sent to the engine as the single combined rule string it accepts.
   */
  rule(head: string, body: string): Promise<string>;

  /** Evaluate a Datalog query pattern. Returns a JSON array of result tuples (e.g. `[["alice", "bob"]]`). */
  query(pattern: string): Promise<string>;

  /** Clear all facts and rules for one predicate. Returns the engine status string. */
  clear(predicate: string): Promise<string>;

  /**
   * Import every graph edge as `predicate(from_id, edge_type, to_id)` facts.
   * Returns the engine status string (`"IMPORTED <n> edges into <p>"`).
   */
  importGraph(predicate: string): Promise<string>;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class DatalogModelImpl implements DatalogModel {
  constructor(
    private readonly transport: Transport,
    private readonly features: NucleusFeatures,
  ) {}

  private require(): void {
    requireNucleus(this.features, 'Datalog');
  }

  async assert(fact: string): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT DATALOG_ASSERT($1)', [fact])) ?? '';
  }

  async retract(fact: string): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT DATALOG_RETRACT($1)', [fact])) ?? '';
  }

  async rule(head: string, body: string): Promise<string> {
    this.require();
    // The engine takes ONE argument: the full 'head :- body' rule text. The
    // old client sent two parameters, which the engine parsed as a bare fact
    // and rejected ("Variable 'X' in fact at argument position 0") — the
    // two-arg form never worked against this engine.
    return (await this.transport.fetchval<string>('SELECT DATALOG_RULE($1)', [`${head} :- ${body}`])) ?? '';
  }

  async query(pattern: string): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT DATALOG_QUERY($1)', [pattern])) ?? '';
  }

  async clear(predicate: string): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT DATALOG_CLEAR($1)', [predicate])) ?? '';
  }

  async importGraph(predicate: string): Promise<string> {
    this.require();
    return (await this.transport.fetchval<string>('SELECT DATALOG_IMPORT_GRAPH($1)', [predicate])) ?? '';
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/** Plugin: adds `.datalog` to the client. */
export const withDatalog: NucleusPlugin<{ datalog: DatalogModel }> = {
  name: 'datalog',
  init(transport: Transport, features: NucleusFeatures) {
    return { datalog: new DatalogModelImpl(transport, features) };
  },
};
