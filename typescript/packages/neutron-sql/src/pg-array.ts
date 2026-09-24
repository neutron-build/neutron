// ---------------------------------------------------------------------------
// @neutron-build/sql — PostgreSQL array literal text (Q07)
// ---------------------------------------------------------------------------
// Array columns are read as their array literal (`col::text`) and written as
// an array literal bound as text (`$n::text::<elem>[]`). Both directions are
// driver-independent: node-postgres and postgres.js pass text through
// untouched, whereas their native array parsers differ per element type (and
// neither knows user enum array OIDs without extra catalog lookups).
//
// Only one-dimensional arrays with the default lower bound are accepted:
// PostgreSQL does not enforce a column's declared dimensionality, so a value
// that does not have the declared shape fails loudly instead of being
// reshaped into the declared type.

/** Parse the text output of a one-dimensional PostgreSQL array (delimiter
 *  ","). Unquoted `NULL` (any case) is SQL NULL; quoted elements unescape
 *  backslash escapes. Throws with `describe` context on anything else. */
export function parseArrayLiteral(text: string, describe: string): Array<string | null> {
  if (text.startsWith("[")) {
    throw new Error(`${describe}: array value has non-default lower bounds (${text.slice(0, text.indexOf("=") + 1 || 16)}…) — only 1-based one-dimensional arrays are supported`);
  }
  if (!text.startsWith("{") || !text.endsWith("}")) {
    throw new Error(`${describe}: expected a PostgreSQL array literal, got ${JSON.stringify(text.length > 40 ? `${text.slice(0, 40)}…` : text)}`);
  }
  const out: Array<string | null> = [];
  if (text === "{}") return out;
  let i = 1;
  const end = text.length - 1;
  for (;;) {
    if (i >= end) throw new Error(`${describe}: malformed array literal (unexpected end)`);
    const ch = text[i];
    if (ch === "{") {
      throw new Error(`${describe}: array value is multi-dimensional — the column is declared as a one-dimensional array`);
    }
    if (ch === '"') {
      let value = "";
      i++;
      for (;;) {
        if (i >= end) throw new Error(`${describe}: malformed array literal (unterminated quoted element)`);
        const c = text[i];
        if (c === "\\") {
          if (i + 1 >= end) throw new Error(`${describe}: malformed array literal (dangling escape)`);
          value += text[i + 1];
          i += 2;
          continue;
        }
        if (c === '"') {
          i++;
          break;
        }
        value += c;
        i++;
      }
      out.push(value);
    } else {
      let j = i;
      while (j < end && text[j] !== "," ) {
        if (text[j] === '"' || text[j] === "{" || text[j] === "}") {
          throw new Error(`${describe}: malformed array literal (unexpected ${JSON.stringify(text[j])} in an unquoted element)`);
        }
        j++;
      }
      const raw = text.slice(i, j);
      if (raw.length === 0) throw new Error(`${describe}: malformed array literal (empty unquoted element)`);
      out.push(raw.toUpperCase() === "NULL" ? null : raw);
      i = j;
    }
    if (i === end) break;
    if (text[i] !== ",") throw new Error(`${describe}: malformed array literal (expected "," at offset ${i})`);
    i++;
  }
  return out;
}

/** Serialize element texts (already canonical per element type) as a
 *  one-dimensional array literal, mirroring PostgreSQL's own deparse rules:
 *  an element is double-quoted (with `\` and `"` escaped) only when its text
 *  would not round-trip unquoted — empty, containing structural characters
 *  (`{ } , " \` or whitespace) or spelling NULL. Plain numbers, booleans and
 *  simple words stay unquoted, matching pg_get_expr so desired defaults
 *  compare equal to introspected text even without the twin normalizer. */
const NEEDS_QUOTES = /^$|[\u0000-\u0020{},"\\]|^null$/i;

export function formatArrayLiteral(elements: ReadonlyArray<string | null>): string {
  const parts = elements.map((e) => {
    if (e === null) return "NULL";
    if (NEEDS_QUOTES.test(e)) return `"${e.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
    return e;
  });
  return `{${parts.join(",")}}`;
}
