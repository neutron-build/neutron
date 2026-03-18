import * as fs from "node:fs";
import * as path from "node:path";

export type RouteHeadersMap = Record<string, Record<string, string>>;

export function readStaticHeadersMap(outDir: string): RouteHeadersMap {
  const headersPath = path.join(outDir, ".neutron-static-headers.json");
  if (!fs.existsSync(headersPath)) {
    return {};
  }

  try {
    const raw = fs.readFileSync(headersPath, "utf-8");
    const parsed = JSON.parse(raw) as RouteHeadersMap;
    if (!parsed || typeof parsed !== "object") {
      return {};
    }
    return parsed;
  } catch {
    return {};
  }
}

export function toCloudflareHeadersFile(headersByRoute: RouteHeadersMap): string {
  const sections: string[] = [];
  const routes = Object.keys(headersByRoute).sort();

  for (const route of routes) {
    const headers = headersByRoute[route];
    const entries = Object.entries(headers);
    if (entries.length === 0) {
      continue;
    }

    sections.push(route);
    for (const [name, value] of entries) {
      sections.push(`  ${name}: ${value}`);
    }
    sections.push("");
  }

  return sections.join("\n").trimEnd() + (sections.length > 0 ? "\n" : "");
}

export interface VercelHeaderConfig {
  source: string;
  headers: Array<{ key: string; value: string }>;
}

export function toVercelHeaders(headersByRoute: RouteHeadersMap): VercelHeaderConfig[] {
  const routes = Object.keys(headersByRoute).sort();
  const output: VercelHeaderConfig[] = [];

  for (const route of routes) {
    const headers = headersByRoute[route];
    const entries = Object.entries(headers);
    if (entries.length === 0) {
      continue;
    }

    output.push({
      source: route,
      headers: entries.map(([key, value]) => ({ key, value })),
    });
  }

  return output;
}
