import { readFile, readdir, stat } from "node:fs/promises";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import type { Tool } from "@neutron-build/ai";

import type { AgentDefinition, LoadedAgent } from "./agent.js";
import { AgentError, problemFromStatus } from "./executor.js";
import { loadSkills } from "./skills.js";

/**
 * The convention loader (mirrors core Neutron's file-convention style):
 *
 *   agent/
 *   ├── agent.js          # default-exports defineAgent({...}) — required
 *   ├── instructions.md   # always-on system prompt — optional
 *   └── tools/            # one file per tool, default-exporting a Tool
 *
 * Runs against built output (.js/.mjs) — the loader executes modules, it
 * does not compile them.
 */
export async function loadAgent(dir: string): Promise<LoadedAgent> {
  const root = resolve(dir);

  const definitionModule = await importFirst(root, ["agent.js", "agent.mjs"]);
  if (definitionModule === undefined) {
    throw new AgentError(problemFromStatus(404, `No agent.js in ${root} — export default defineAgent({...}).`));
  }
  const definition = definitionModule.default as AgentDefinition | undefined;
  if (definition === undefined || typeof definition.name !== "string") {
    throw new AgentError(problemFromStatus(400, `${root}/agent.js must default-export defineAgent({...}).`));
  }

  let instructions = "";
  try {
    instructions = await readFile(join(root, "instructions.md"), "utf8");
  } catch {
    // optional file
  }

  const byName = new Map<string, Tool>();
  for (const tool of definition.tools ?? []) byName.set(tool.name, tool);
  const toolsDir = join(root, "tools");
  if (await isDirectory(toolsDir)) {
    for (const entry of (await readdir(toolsDir)).sort()) {
      if (!entry.endsWith(".js") && !entry.endsWith(".mjs")) continue;
      const mod = (await import(pathToFileURL(join(toolsDir, entry)).href)) as { default?: Tool };
      const tool = mod.default;
      if (tool === undefined || typeof tool.name !== "string" || tool.inputSchema === undefined) {
        throw new AgentError(problemFromStatus(400, `tools/${entry} must default-export an AI SDK tool().`));
      }
      byName.set(tool.name, tool);
    }
  }

  const skills = await loadSkills(join(root, "skills"));

  const loaded: LoadedAgent = {
    definition,
    instructions: instructions.trim(),
    tools: [...byName.values()],
    ...(skills.length > 0 ? { skills } : {}),
    dir: root,
  };
  return loaded;
}

async function importFirst(root: string, names: string[]): Promise<{ default?: unknown } | undefined> {
  for (const name of names) {
    const path = join(root, name);
    try {
      await stat(path);
    } catch {
      continue;
    }
    return (await import(pathToFileURL(path).href)) as { default?: unknown };
  }
  return undefined;
}

async function isDirectory(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isDirectory();
  } catch {
    return false;
  }
}
