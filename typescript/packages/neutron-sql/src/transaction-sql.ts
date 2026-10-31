import { NeutronSqlError } from './errors.js';

/** Conservative raw statement admission for an owned connection. This is a
 * lifecycle fence, not a SQL security sandbox: functions/triggers still run
 * with database privileges. Control belongs to the transaction runner. */
export function validateTransactionSql(sql: string): void {
  let index = 0;
  let terminated = false;
  const words: string[] = [];
  const invalid = (reason: string): never => { throw new NeutronSqlError(`transaction SQL: ${reason}`); };
  while (index < sql.length) {
    const char = sql[index]!;
    if (/\s/.test(char)) { index++; continue; }
    if (sql.startsWith('--', index)) {
      const end = sql.indexOf('\n', index + 2);
      index = end < 0 ? sql.length : end + 1;
      continue;
    }
    if (sql.startsWith('/*', index)) {
      let depth = 1;
      index += 2;
      while (index < sql.length && depth) {
        if (sql.startsWith('/*', index)) { depth++; index += 2; }
        else if (sql.startsWith('*/', index)) { depth--; index += 2; }
        else index++;
      }
      if (depth) invalid('unterminated comment');
      continue;
    }
    if (terminated) invalid('multiple statements are forbidden in an owned scope');
    if (char === ';') { terminated = true; index++; continue; }
    if (char === '(' && !words.length) { index++; continue; }
    if (char === "'" || char === '"') {
      const escape = char === "'" && /[eE]/.test(sql[index - 1] ?? '') && !/[A-Za-z0-9_]/.test(sql[index - 2] ?? '');
      let closed = false;
      index++;
      while (index < sql.length) {
        if (char === "'" && sql[index] === '\\') {
          if (!escape) invalid('ordinary string backslashes depend on session settings; bind the value instead');
          index += 2;
        } else if (sql[index] === char) {
          if (sql[index + 1] === char) index += 2;
          else { index++; closed = true; break; }
        } else index++;
      }
      if (!closed) invalid('unterminated quote');
      continue;
    }
    if (char === '$') {
      const delimiter = /^(?:\$\$|\$[A-Za-z_][A-Za-z0-9_]*\$)/.exec(sql.slice(index))?.[0];
      if (delimiter) {
        const end = sql.indexOf(delimiter, index + delimiter.length);
        if (end < 0) invalid('unterminated dollar quote');
        index = end + delimiter.length;
        continue;
      }
    }
    const word = /^[A-Za-z_][A-Za-z0-9_]*/.exec(sql.slice(index))?.[0];
    if (word) { if (words.length < 2) words.push(word.toUpperCase()); index += word.length; }
    else {
      if (!words.length) invalid('statement keyword required');
      index++;
    }
  }
  const first = words[0];
  if (!first) invalid('empty statement');
  // Top-level transaction control (BEGIN/COMMIT/ROLLBACK without a
  // savepoint target) belongs to the runner. SAVEPOINT, ROLLBACK TO, and
  // RELEASE [SAVEPOINT] are savepoint operations inside a transaction;
  // the runner itself uses them for nesting.
  if (['SAVEPOINT', 'ROLLBACK', 'RELEASE'].includes(first!) ||
      (first === 'BEGIN' && words[1] === '')) {
    // savepoint-family or bare BEGIN — check if it's savepoint-shaped
    const isSavepointOp =
      first === 'SAVEPOINT' ||
      (first === 'ROLLBACK' && words[1] === 'TO') ||
      (first === 'RELEASE');
    if (!isSavepointOp) {
      invalid('transaction and session control belongs to the runner');
    }
  } else if (['BEGIN', 'START', 'COMMIT', 'END', 'ABORT', 'RESET', 'DISCARD', 'PREPARE', 'DEALLOCATE', 'LISTEN', 'UNLISTEN', 'LOAD'].includes(first!)) {
    invalid('transaction and session control belongs to the runner');
  }
  // SET and SET ROLE are admitted: PostgreSQL enforces its own session
  // security. The runner still owns transaction control.
  if (first === 'PREPARE' && words[1] === 'TRANSACTION') {
    invalid('persistent session changes and distributed transaction control are forbidden');
  }
  if (first === 'CREATE' && ['TEMP', 'TEMPORARY'].includes(words[1] ?? '')) invalid('temporary session objects require a separate owned connection');
}
