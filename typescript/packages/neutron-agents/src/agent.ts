import type { ModelAdapter, Tool } from "@neutron-build/ai";

import { AgentError, problemFromStatus } from "./executor.js";
import type { Skill } from "./skills.js";

/** What agent.ts default-exports via defineAgent(). */
export interface AgentDefinition {
  name: string;
  model: ModelAdapter;
  /** Model-call budget per turn (default 8). */
  maxSteps?: number;
  maxOutputTokens?: number;
  temperature?: number;
  /** Tools defined inline; the loader merges tools/ files on top. */
  tools?: Tool[];
}

/** Identity helper with load-time validation — the agent.ts convention. */
export function defineAgent(definition: AgentDefinition): AgentDefinition {
  if (typeof definition.name !== "string" || definition.name === "") {
    throw new AgentError(problemFromStatus(400, "Agent `name` must be a non-empty string."));
  }
  const model = definition.model as { doGenerate?: unknown; doStream?: unknown } | undefined;
  if (typeof model?.doGenerate !== "function" || typeof model?.doStream !== "function") {
    throw new AgentError(problemFromStatus(400, `Agent "${definition.name}" needs a ModelAdapter as \`model\`.`));
  }
  if (definition.maxSteps !== undefined && (!Number.isInteger(definition.maxSteps) || definition.maxSteps < 1)) {
    throw new AgentError(problemFromStatus(400, `Agent "${definition.name}": \`maxSteps\` must be a positive integer.`));
  }
  return definition;
}

/** A fully assembled agent: definition + conventions gathered from its directory. */
export interface LoadedAgent {
  definition: AgentDefinition;
  /** instructions.md content, "" when absent. */
  instructions: string;
  /** Inline tools plus one per tools/ file, name-deduped (files win). */
  tools: Tool[];
  /** skills/ content; loaded on demand at runtime via the skill tool. */
  skills?: Skill[];
  /** The directory it was loaded from (undefined for inline agents). */
  dir?: string;
}
