/**
 * Best-effort parse of a JSON prefix as it streams in: close any open
 * strings/arrays/objects and try JSON.parse. Prefixes ending mid-token
 * degrade gracefully — a partial literal (`tru`), a key with no value
 * yet, or a trailing comma is dropped so the last complete snapshot
 * still parses. Returns undefined only when no prefix parses at all.
 */
export function parsePartialJson(text: string): unknown {
  const trimmed = text.trim();
  if (trimmed === "") return undefined;
  try {
    return JSON.parse(trimmed);
  } catch {
    // fall through to completion
  }

  let inString = false;
  let escaped = false;
  const stack: string[] = [];
  for (const char of trimmed) {
    if (inString) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') inString = false;
      continue;
    }
    if (char === '"') inString = true;
    else if (char === "{") stack.push("}");
    else if (char === "[") stack.push("]");
    else if (char === "}" || char === "]") stack.pop();
  }

  let candidate = trimmed;
  if (escaped) candidate = candidate.slice(0, -1);
  if (inString) candidate += '"';

  let result = tryParse(candidate, stack);
  if (result !== undefined) return result;

  // Dangling fragments between tokens, dropped in order: a partial
  // literal, then a key with no value, then a trailing comma.
  candidate = candidate.replace(/(?<=[:,[{\s])[-+0-9.eEa-z]+[ \t\n\r]*$/i, "");
  result = tryParse(candidate, stack);
  if (result !== undefined) return result;

  candidate = candidate.replace(/,?[ \t\n\r]*"(?:[^"\\]|\\.)*"[ \t\n\r]*:?[ \t\n\r]*$/, "");
  result = tryParse(candidate, stack);
  if (result !== undefined) return result;

  candidate = candidate.replace(/,[ \t\n\r]*$/, "");
  return tryParse(candidate, stack);
}

function tryParse(text: string, stack: string[]): unknown {
  let completed = text;
  for (let i = stack.length - 1; i >= 0; i--) completed += stack[i]!;
  try {
    return JSON.parse(completed);
  } catch {
    return undefined;
  }
}
