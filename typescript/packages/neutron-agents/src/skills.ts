import { readFile, readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import { jsonSchema } from "@neutron-build/ai";
import type { Tool } from "@neutron-build/ai";

import { AgentError, problemFromStatus } from "./executor.js";

/**
 * A skill: a procedure loaded on demand rather than always-on context.
 *
 *   skills/<name>/
 *   ├── SKILL.md     # frontmatter `description:` (when to use) + instructions
 *   └── *.js         # optional tool files, merged into the agent's toolset
 *
 * Skill tools are always registered (cheap); the instructions load only
 * when the model asks via the built-in `skill` tool. Sources beyond the
 * local directory (Teploy catalog, MCP servers) are the C1 integration
 * and plug in behind this same Skill shape.
 */
export interface Skill {
  name: string;
  /** When-to-use summary shown in the skill tool's listing. */
  description: string;
  /** Full procedure, returned when the skill is loaded. */
  instructions: string;
  tools: Tool[];
}

export async function loadSkills(dir: string): Promise<Skill[]> {
  if (!(await isDirectory(dir))) return [];
  const skills: Skill[] = [];
  for (const entry of (await readdir(dir)).sort()) {
    const skillDir = join(dir, entry);
    if (!(await isDirectory(skillDir))) continue;

    let raw: string;
    try {
      raw = await readFile(join(skillDir, "SKILL.md"), "utf8");
    } catch {
      throw new AgentError(problemFromStatus(400, `Skill "${entry}" has no SKILL.md.`));
    }
    const { frontmatter, body } = parseFrontmatter(raw);
    const description = frontmatter.description ?? "";
    if (description === "") {
      throw new AgentError(
        problemFromStatus(400, `Skill "${entry}": SKILL.md needs a \`description:\` frontmatter line (when to use it).`),
      );
    }

    const tools: Tool[] = [];
    for (const file of (await readdir(skillDir)).sort()) {
      if (!file.endsWith(".js") && !file.endsWith(".mjs")) continue;
      const mod = (await import(pathToFileURL(join(skillDir, file)).href)) as { default?: Tool };
      if (mod.default?.name === undefined) {
        throw new AgentError(problemFromStatus(400, `Skill "${entry}": ${file} must default-export an AI SDK tool().`));
      }
      tools.push(mod.default);
    }

    skills.push({ name: entry, description, instructions: body.trim(), tools });
  }
  return skills;
}

/** The on-demand loader the runtime registers when any skills exist. */
export function skillTool(skills: Skill[]): Tool {
  const listing = skills.map((skill) => `- ${skill.name}: ${skill.description}`).join("\n");
  const byName = new Map(skills.map((skill) => [skill.name, skill]));
  return {
    name: "skill",
    description: `Load a skill's instructions when its situation applies. Available skills:\n${listing}`,
    inputSchema: jsonSchema({
      type: "object",
      properties: { name: { type: "string", description: "The skill to load." } },
      required: ["name"],
      additionalProperties: false,
    }),
    execute: async (input) => {
      const { name } = input as { name: string };
      const skill = byName.get(name);
      if (skill === undefined) return `Unknown skill: ${name}. Available: ${[...byName.keys()].join(", ")}`;
      return skill.instructions;
    },
  };
}

function parseFrontmatter(raw: string): { frontmatter: Record<string, string>; body: string } {
  const frontmatter: Record<string, string> = {};
  if (!raw.startsWith("---")) return { frontmatter, body: raw };
  const end = raw.indexOf("\n---", 3);
  if (end === -1) return { frontmatter, body: raw };
  for (const line of raw.slice(3, end).split("\n")) {
    const colon = line.indexOf(":");
    if (colon === -1) continue;
    frontmatter[line.slice(0, colon).trim()] = line.slice(colon + 1).trim();
  }
  return { frontmatter, body: raw.slice(end + 4) };
}

async function isDirectory(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isDirectory();
  } catch {
    return false;
  }
}
