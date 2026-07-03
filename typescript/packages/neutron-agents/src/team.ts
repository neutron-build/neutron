import type { GenerateTextResult } from "@neutron-build/ai";

import type { LoadedAgent } from "./agent.js";
import { AgentError, problemFromStatus } from "./executor.js";
import { runTurn } from "./runtime.js";
import type { RunTurnOptions } from "./runtime.js";

/** Runs one member with an input; policies compose these calls. */
export type MemberRunner = (member: string, input: string) => Promise<GenerateTextResult>;

/**
 * Pure routing over member runs — the C3 extension point: verification
 * strategies (proposer/critic/verifier) land as new policies without
 * touching the runtime.
 */
export type TeamPolicy = (run: MemberRunner, input: string) => Promise<GenerateTextResult>;

/**
 * The execution unit is a team of 1..N agents; a solo agent is the
 * one-member degenerate case (`pipeline()` over a single member).
 */
export interface Team {
  name: string;
  members: Record<string, LoadedAgent>;
  policy: TeamPolicy;
}

export function defineTeam(team: Team): Team {
  if (typeof team.name !== "string" || team.name === "") {
    throw new AgentError(problemFromStatus(400, "Team `name` must be a non-empty string."));
  }
  if (Object.keys(team.members).length === 0) {
    throw new AgentError(problemFromStatus(400, `Team "${team.name}" needs at least one member.`));
  }
  if (typeof team.policy !== "function") {
    throw new AgentError(problemFromStatus(400, `Team "${team.name}" needs a policy (pipeline(), roundtrip(), or custom).`));
  }
  return team;
}

/** Members run in order, each receiving the previous member's text. */
export function pipeline(order?: string[]): TeamPolicy {
  return async (run, input) => {
    const sequence = order ?? [];
    if (sequence.length === 0) {
      throw new AgentError(problemFromStatus(400, "pipeline() needs the member order (pass member names)."));
    }
    let current = input;
    let last: GenerateTextResult | undefined;
    for (const member of sequence) {
      last = await run(member, current);
      current = last.text;
    }
    return last!;
  };
}

export interface RoundtripOptions {
  /** The proposing member. */
  from: string;
  /** The reviewing member. */
  review: string;
  maxRounds?: number;
  /** Reviewer text containing this token accepts the proposal (default "APPROVE"). */
  approveToken?: string;
}

/**
 * Propose/review loop: `from` produces, `review` critiques; feedback goes
 * back to the proposer until the reviewer approves or rounds run out
 * (the last proposal is returned either way).
 */
export function roundtrip(options: RoundtripOptions): TeamPolicy {
  const maxRounds = options.maxRounds ?? 3;
  const approveToken = options.approveToken ?? "APPROVE";
  return async (run, input) => {
    let proposal = await run(options.from, input);
    for (let round = 1; round <= maxRounds; round++) {
      const review = await run(
        options.review,
        `Review the following work. Reply with "${approveToken}" if it is acceptable; otherwise give concrete revision feedback.\n\nTask:\n${input}\n\nWork:\n${proposal.text}`,
      );
      if (review.text.includes(approveToken)) return proposal;
      // Out of rounds: return what we have rather than producing a
      // revision no reviewer will ever see.
      if (round === maxRounds) break;
      proposal = await run(
        options.from,
        `${input}\n\nYour previous attempt:\n${proposal.text}\n\nReviewer feedback (address all of it):\n${review.text}`,
      );
    }
    return proposal;
  };
}

export interface RunTeamTurnOptions extends Pick<RunTurnOptions, "executor" | "onApprovalRequest" | "abortSignal"> {
  input: string;
}

export async function runTeamTurn(team: Team, options: RunTeamTurnOptions): Promise<GenerateTextResult> {
  const run: MemberRunner = (member, input) => {
    const agent = team.members[member];
    if (agent === undefined) {
      throw new AgentError(problemFromStatus(400, `Team "${team.name}" has no member "${member}".`));
    }
    const turn: RunTurnOptions = { input };
    if (options.executor !== undefined) turn.executor = options.executor;
    if (options.onApprovalRequest !== undefined) turn.onApprovalRequest = options.onApprovalRequest;
    if (options.abortSignal !== undefined) turn.abortSignal = options.abortSignal;
    return runTurn(agent, turn);
  };
  return team.policy(run, options.input);
}
